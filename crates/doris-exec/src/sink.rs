//! The OLAP table sink (`VTabletWriterV2` on one BE): rows are checked and routed; rows that
//! fail are filtered, counted and logged; then every tablet of every index of each partition
//! that got a row is written (an empty rowset where no row came) and committed to the load's
//! transaction. The FE learns the outcome from one final `reportExecStatus`.
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Mutex;

use doris_proto::doris::PUniqueId;
use doris_segment::writer::Datum;
use doris_storage::rowset::write_rowset;
use doris_storage::TabletManager;
use doris_thrift::gen::frontend_service::{FrontendServiceVersion, TFragmentInstanceReport, TReportExecStatusParams};
use doris_thrift::gen::palo_internal_service::TQueryType;
use doris_thrift::gen::status::{TStatus, TStatusCode};
use doris_thrift::gen::types::{TTabletCommitInfo, TUniqueId};

use crate::error_log::write_error_log;
use crate::fragment::WriteFragment;
use crate::route::Router;
use crate::value::{ColType, Value};
use crate::RowError;

#[derive(Debug, Clone)]
pub struct SinkOutcome {
    pub status: TStatus,
    pub commit_infos: Vec<TTabletCommitInfo>,
    /// Rows written.
    pub loaded: i64,
    /// Rows filtered (`dpp.abnorm.ALL`).
    pub filtered: i64,
    pub unselected: i64,
    /// Bytes of the rows that reached the sink (`loaded_bytes`).
    pub bytes: i64,
    /// `<message>. Src line: <line>` of the first filtered row.
    pub first_error: Option<String>,
    /// The error log's name under the error log directory.
    pub error_log: Option<String>,
}

impl SinkOutcome {
    /// A write that failed before it committed anything: INTERNAL_ERROR with the message.
    pub fn failed(msg: String) -> SinkOutcome {
        SinkOutcome {
            status: status(TStatusCode::INTERNAL_ERROR, Some(msg)),
            commit_infos: Vec::new(),
            loaded: 0,
            filtered: 0,
            unselected: 0,
            bytes: 0,
            first_error: None,
            error_log: None,
        }
    }
}

fn status(code: TStatusCode, msg: Option<String>) -> TStatus {
    TStatus { status_code: code, error_msgs: msg.map(|m| vec![m]) }
}

/// Bytes a row adds to the sink's input block (`Block::bytes`): each column's data, a 4-byte
/// offset per string, and a byte for each column that has a null map (`nullable`).
pub fn row_bytes(cols: &[ColType], nullable: &[bool], row: &[Value]) -> i64 {
    use doris_thrift::gen::types::TPrimitiveType as T;
    cols.iter()
        .zip(row)
        .zip(nullable.iter().copied().chain(std::iter::repeat(true)))
        .map(|((c, v), nullable)| {
            let data = match c.ptype {
                T::CHAR | T::VARCHAR | T::STRING => {
                    let len = match v {
                        Value::Str(b) => b.len(),
                        Value::Null => 0,
                        Value::Int(i) => i.to_string().len(),
                        Value::Decimal(s) | Value::Date(s) => s.len(),
                        Value::Float(f) => f.to_string().len(),
                        Value::Bool(_) => 1,
                    };
                    len as i64 + 4
                }
                T::BOOLEAN | T::TINYINT => 1,
                T::SMALLINT => 2,
                T::INT | T::FLOAT | T::DATEV2 | T::DECIMAL32 => 4,
                T::BIGINT | T::DOUBLE | T::DATETIMEV2 | T::DECIMAL64 => 8,
                _ => 16,
            };
            data + i64::from(nullable)
        })
        .sum()
}

fn uid(id: &TUniqueId) -> PUniqueId {
    PUniqueId { hi: id.hi, lo: id.lo }
}

pub fn run_sink(
    f: &WriteFragment,
    rows: impl IntoIterator<Item = Result<Vec<Value>, RowError>>,
    tablets: &Mutex<TabletManager>,
    error_dir: &Path,
) -> SinkOutcome {
    let mut out = SinkOutcome {
        status: status(TStatusCode::OK, None),
        commit_infos: Vec::new(),
        loaded: 0,
        filtered: 0,
        unselected: 0,
        bytes: 0,
        first_error: None,
        error_log: None,
    };
    let router = match Router::new(f) {
        Ok(r) => r,
        Err(e) => {
            out.status = status(TStatusCode::INTERNAL_ERROR, Some(e.to_string()));
            return out;
        }
    };
    let mut errors: Vec<RowError> = Vec::new();
    let mut groups: BTreeMap<i64, Vec<Vec<Datum>>> = BTreeMap::new();
    let mut touched: BTreeSet<i64> = BTreeSet::new();
    let nullable = f.input_nullable();
    for row in rows {
        let routed = row.and_then(|values| {
            out.bytes += row_bytes(f.dest_columns(), &nullable, &values);
            router.route(&values)
        });
        match routed {
            Ok(r) => {
                out.loaded += 1;
                touched.insert(r.partition_id);
                for (_, tablet) in &r.tablets {
                    groups.entry(*tablet).or_default().push(r.row.clone());
                }
            }
            Err(e) => {
                out.filtered += 1;
                errors.push(e);
            }
        }
    }
    if let Some(e) = errors.first() {
        out.first_error = Some(format!("{}. Src line: {}", e.msg, e.line));
        // print_id(query_id): the halves as unsigned hex, joined by a dash
        let label = format!("stmt_{:x}-{:x}", f.query_id.hi as u64, f.query_id.lo as u64);
        match write_error_log(error_dir, "insert", &label, (f.instance_id.hi, f.instance_id.lo), &errors) {
            Ok(name) => out.error_log = Some(name),
            Err(e) => eprintln!("doris-rs be: cannot write the load error log: {e}"),
        }
    }

    // every tablet of every index of each partition that got a row
    let load_id = uid(&f.sink.load_id);
    let txn = f.sink.txn_id;
    let names: Vec<&str> = f.dest_columns().iter().map(|c| c.name.as_str()).collect();
    // The tablet lock is held only to read each tablet's meta and to commit: segments are
    // written and synced without it, so publishes, reports and other loads are not held up.
    struct Planned {
        partition: i64,
        tablet: i64,
        meta: doris_proto::doris::TabletMetaPb,
        dir: std::path::PathBuf,
        id: String,
        rows: Vec<Vec<Datum>>,
    }
    let fail = |out: &mut SinkOutcome, msg: String| {
        out.status = status(TStatusCode::INTERNAL_ERROR, Some(format!("tablet error: {msg}")));
        out.commit_infos.clear();
    };
    let mut planned = Vec::new();
    {
        let mut mgr = tablets.lock().unwrap_or_else(|e| e.into_inner());
        for part in f.sink.partition.partitions.iter().filter(|p| touched.contains(&p.id)) {
            for index in &part.indexes {
                for &tablet in &index.tablets {
                    let rows = groups.remove(&tablet).unwrap_or_default();
                    let res = (|| {
                        let meta = mgr.tablet(tablet).ok_or_else(|| format!("tablet {tablet} not found"))?.clone();
                        let schema = meta.schema.as_ref().ok_or("a tablet without a schema")?;
                        // the tablet's columns, from the destination row by name
                        let pick: Vec<usize> = schema
                            .column
                            .iter()
                            .map(|c| {
                                let name = c.name.as_deref().unwrap_or_default();
                                names.iter().position(|n| *n == name).ok_or_else(|| format!("column {name} is not written"))
                            })
                            .collect::<Result<_, _>>()?;
                        let rows: Vec<Vec<Datum>> = rows.into_iter().map(|r| pick.iter().map(|i| r[*i].clone()).collect()).collect();
                        let id = mgr.next_rowset_id();
                        let dir = mgr.tablet_path(tablet).ok_or("no tablet path")?;
                        Ok::<_, String>(Planned { partition: part.id, tablet, meta, dir, id, rows })
                    })();
                    match res {
                        Ok(p) => planned.push(p),
                        Err(msg) => {
                            fail(&mut out, msg);
                            return out;
                        }
                    }
                }
            }
        }
    }
    let mut written = Vec::with_capacity(planned.len());
    for p in planned {
        match write_rowset(&p.dir, &p.meta, &p.id, txn, &load_id, p.rows) {
            Ok(w) => written.push((p.partition, p.tablet, w.meta)),
            Err(e) => {
                fail(&mut out, e.to_string());
                return out;
            }
        }
    }
    let mut mgr = tablets.lock().unwrap_or_else(|e| e.into_inner());
    for (partition, tablet, rs) in written {
        if let Err(e) = mgr.commit_txn(partition, txn, tablet, load_id, rs) {
            fail(&mut out, e.to_string());
            return out;
        }
        out.commit_infos.push(TTabletCommitInfo { tablet_id: tablet, backend_id: f.backend_id, invalid_dict_cols: None });
    }
    out
}

/// The final `reportExecStatus` of a write fragment (`PipelineFragmentContext::send_report`).
pub fn exec_status(f: &WriteFragment, o: &SinkOutcome, tracking_url: Option<String>) -> TReportExecStatusParams {
    let counters: BTreeMap<String, String> = [
        ("dpp.norm.ALL", o.loaded),
        ("dpp.abnorm.ALL", o.filtered),
        ("unselected.rows", o.unselected),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect();
    let report = TFragmentInstanceReport {
        fragment_instance_id: Some(f.instance_id.clone()),
        num_finished_range: Some(0),
        loaded_rows: Some(o.loaded + o.filtered),
        loaded_bytes: Some(o.bytes),
    };
    let ok = o.status.status_code == TStatusCode::OK;
    TReportExecStatusParams::new(
        FrontendServiceVersion::V1,
        f.query_id.clone(),
        -1,
        TUniqueId { hi: 0, lo: 0 },
        o.status.clone(),
        true,
        None,
        None,
        None,
        counters,
        if o.filtered > 0 { tracking_url } else { None },
        None,
        if ok { Some(o.commit_infos.clone()) } else { None },
        0,
        f.backend_id,
        0,
        None,
        f.fragment_id,
        TQueryType::LOAD,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        vec![report],
        None,
        o.first_error.clone(),
        None,
    )
}
