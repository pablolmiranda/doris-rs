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
    /// Each instance: its id, the rows that reached it (filtered ones included), their bytes.
    pub instances: Vec<(TUniqueId, i64, i64)>,
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
            instances: Vec::new(),
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
                // a v1 date in a block is a VecDateTimeValue, 8 bytes
                T::BIGINT | T::DOUBLE | T::DATETIMEV2 | T::DECIMAL64 | T::DATE | T::DATETIME => 8,
                _ => 16,
            };
            data + i64::from(nullable)
        })
        .sum()
}

fn uid(id: &TUniqueId) -> PUniqueId {
    PUniqueId { hi: id.hi, lo: id.lo }
}

/// `MAX_ERROR_NUM`: error rows an instance logs; with `load_zero_tolerance` the next one stops
/// the load (`RuntimeState::append_error_msg_to_file`).
const MAX_LOGGED_ERRORS: usize = 51;

/// One instance's share of a table sink: its routed rows in order, its error rows, and what
/// reached it.
#[derive(Default)]
struct InstanceRows {
    routed: Vec<crate::route::Routed>,
    errors: Vec<RowError>,
    filtered: i64,
    /// Rows that reached the sink, filtered ones included (`num_rows_load_total`).
    reached: i64,
    bytes: i64,
}

/// The OLAP table sink of a fragment, fed by each of its instances (`VTabletWriterV2`): rows are
/// routed as they arrive; `finish`, once every instance has ended, writes each touched
/// partition's tablets and commits them, as the last sink to close does.
pub struct TableSink {
    f: WriteFragment,
    router: Router,
    instance_ids: Vec<TUniqueId>,
    instances: Mutex<Vec<InstanceRows>>,
}

impl TableSink {
    pub fn new(f: &WriteFragment, instance_ids: Vec<TUniqueId>) -> Result<TableSink, crate::ExecError> {
        let router = Router::new(f)?;
        let instances = Mutex::new(instance_ids.iter().map(|_| InstanceRows::default()).collect());
        Ok(TableSink { f: f.clone(), router, instance_ids, instances })
    }

    /// An instance's rows (destination column order; a row error is filtered), with whether
    /// each column of the block that carried them has a null map. Cancelled with
    /// DATA_QUALITY_ERROR when a zero-tolerance load has more error rows than it logs.
    pub fn push(&self, instance: usize, rows: Vec<Result<Vec<Value>, RowError>>, nullable: &[bool]) -> Result<(), crate::ExecError> {
        let mut all = self.instances.lock().unwrap_or_else(|e| e.into_inner());
        let me = all.get_mut(instance).ok_or_else(|| crate::ExecError::BadRequest(format!("a table sink without instance {instance}")))?;
        // the whole block reaches the sink before its rows are checked
        me.reached += rows.len() as i64;
        for values in rows.iter().flatten() {
            me.bytes += row_bytes(self.f.dest_columns(), nullable, values);
        }
        for row in rows {
            match row.and_then(|values| self.router.route(&values)) {
                Ok(r) => me.routed.push(r),
                Err(e) => {
                    if me.errors.len() >= MAX_LOGGED_ERRORS {
                        if self.f.zero_tolerance {
                            let q = &self.f.query_id;
                            return Err(crate::ExecError::Cancelled(format!(
                                "load_id={:x}-{:x}, reason: [DATA_QUALITY_ERROR]Encountered unqualified data, stop processing. Please check if the source data matches the schema, and consider disabling strict mode or increasing max_filter_ratio.",
                                q.hi as u64, q.lo as u64
                            )));
                        }
                    } else {
                        me.errors.push(e);
                    }
                    me.filtered += 1;
                }
            }
        }
        Ok(())
    }

    /// Ends the sink once every instance has: commits what the instances routed, or, when the
    /// query failed (`failure`), commits nothing and reports that status. Either way the counts,
    /// the error log and the first error are those of what reached the sink.
    pub fn finish(self, tablets: &Mutex<TabletManager>, error_dir: &Path, failure: Option<TStatus>) -> SinkOutcome {
        let f = &self.f;
        let instances = self.instances.into_inner().unwrap_or_else(|e| e.into_inner());
        let mut out = SinkOutcome {
            status: failure.clone().unwrap_or_else(|| status(TStatusCode::OK, None)),
            commit_infos: Vec::new(),
            loaded: instances.iter().map(|i| i.reached - i.filtered).sum(),
            filtered: instances.iter().map(|i| i.filtered).sum(),
            unselected: 0,
            bytes: instances.iter().map(|i| i.bytes).sum(),
            first_error: None,
            error_log: None,
            instances: self.instance_ids.iter().zip(&instances).map(|(id, i)| (id.clone(), i.reached, i.bytes)).collect(),
        };
        // the first instance (in plan order) that logged errors names the load's log
        if let Some((n, rows)) = instances.iter().enumerate().find(|(_, i)| !i.errors.is_empty()) {
            let e = &rows.errors[0];
            out.first_error = Some(format!("{}. Src line: {}", e.msg, e.line));
            let id = &self.instance_ids[n];
            let (db, label) = if f.plan == crate::fragment::WritePlan::Stream {
                ("insert".to_string(), format!("stmt_{:x}-{:x}", f.query_id.hi as u64, f.query_id.lo as u64))
            } else {
                // RuntimeState::init: db insert_stmt, the label print_id(fragment_instance_id)
                ("insert_stmt".to_string(), format!("{:x}-{:x}", id.hi as u64, id.lo as u64))
            };
            match write_error_log(error_dir, &db, &label, (id.hi, id.lo), &rows.errors) {
                Ok(name) => out.error_log = Some(name),
                Err(e) => eprintln!("doris-rs be: cannot write the load error log: {e}"),
            }
        }
        if failure.is_some() {
            // a cancelled load counts every row that reached it as loaded and none as filtered
            if failure.as_ref().is_some_and(|s| s.status_code == TStatusCode::CANCELLED) {
                out.loaded = instances.iter().map(|i| i.reached).sum();
                out.filtered = 0;
            }
            return out;
        }
        let mut groups: BTreeMap<i64, Vec<Vec<Datum>>> = BTreeMap::new();
        let mut touched: BTreeSet<i64> = BTreeSet::new();
        for r in instances.into_iter().flat_map(|i| i.routed) {
            touched.insert(r.partition_id);
            for (_, tablet) in &r.tablets {
                groups.entry(*tablet).or_default().push(r.row.clone());
            }
        }
        commit(f, &mut out, groups, &touched, tablets);
        out
    }
}

/// Runs one instance's rows through a table sink to its end.
pub fn run_sink(
    f: &WriteFragment,
    rows: impl IntoIterator<Item = Result<Vec<Value>, RowError>>,
    tablets: &Mutex<TabletManager>,
    error_dir: &Path,
) -> SinkOutcome {
    let sink = match TableSink::new(f, vec![f.instance_id.clone()]) {
        Ok(s) => s,
        Err(e) => return SinkOutcome::failed(e.to_string()),
    };
    let failure = sink
        .push(0, rows.into_iter().collect(), &f.input_nullable())
        .err()
        .map(|e| status(TStatusCode::CANCELLED, Some(e.to_string())));
    sink.finish(tablets, error_dir, failure)
}

/// Writes every tablet of every index of each touched partition (an empty rowset where no row
/// came) and commits them to the load's transaction.
fn commit(f: &WriteFragment, out: &mut SinkOutcome, mut groups: BTreeMap<i64, Vec<Vec<Datum>>>, touched: &BTreeSet<i64>, tablets: &Mutex<TabletManager>) {
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
                        Err(msg) => return fail(out, msg),
                    }
                }
            }
        }
    }
    let mut written = Vec::with_capacity(planned.len());
    for p in planned {
        match write_rowset(&p.dir, &p.meta, &p.id, txn, &load_id, p.rows) {
            Ok(w) => written.push((p.partition, p.tablet, w.meta)),
            Err(e) => return fail(out, e.to_string()),
        }
    }
    let mut mgr = tablets.lock().unwrap_or_else(|e| e.into_inner());
    for (partition, tablet, rs) in written {
        if let Err(e) = mgr.commit_txn(partition, txn, tablet, load_id, rs) {
            return fail(out, e.to_string());
        }
        out.commit_infos.push(TTabletCommitInfo { tablet_id: tablet, backend_id: f.backend_id, invalid_dict_cols: None });
    }
}

/// The final `reportExecStatus` of a write fragment (`PipelineFragmentContext::send_report`,
/// `_coordinator_callback`): the counters and one report per instance that had rows only when
/// some had; commit infos only when there are any; the tracking URL and first error when set.
pub fn exec_status(f: &WriteFragment, o: &SinkOutcome, tracking_url: Option<String>) -> TReportExecStatusParams {
    let had_rows = o.instances.iter().any(|(_, rows, _)| *rows > 0);
    let counters: BTreeMap<String, String> = [
        ("dpp.norm.ALL", o.loaded),
        ("dpp.abnorm.ALL", o.filtered),
        ("unselected.rows", o.unselected),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect();
    let reports: Vec<TFragmentInstanceReport> = o
        .instances
        .iter()
        .filter(|(_, rows, _)| *rows > 0)
        .map(|(id, rows, bytes)| TFragmentInstanceReport {
            fragment_instance_id: Some(id.clone()),
            num_finished_range: Some(0),
            loaded_rows: Some(*rows),
            loaded_bytes: Some(*bytes),
        })
        .collect();
    let ok = o.status.status_code == TStatusCode::OK;
    let mut p = TReportExecStatusParams::new(
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
        if o.error_log.is_some() { tracking_url } else { None },
        None,
        (ok && !o.commit_infos.is_empty()).then(|| o.commit_infos.clone()),
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
        reports,
        None,
        o.first_error.clone(),
        None,
    );
    if !had_rows {
        p.load_counters = None;
        p.fragment_instance_reports = None;
    }
    p
}
