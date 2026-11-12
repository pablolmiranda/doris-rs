//! The MySQL text rows the result sink writes, checked against the reference's replies.
use doris_exec::block::Block;
use doris_exec::row_text::{cell_text, result_batch};
use doris_exec::value::{parse_text, to_datum, ColType};
use doris_fixtures::replays::{self, plan_list, Step};
use doris_segment::writer::Datum;
use doris_thrift::gen::data::TResultBatch;
use doris_thrift::gen::data_sinks::TDataSinkType;
use doris_thrift::gen::palo_internal_service::TPipelineFragmentParamsList;
use doris_thrift::gen::types::TPrimitiveType as T;
use thrift::protocol::{TBinaryInputProtocol, TSerializable};

fn col(ptype: T, precision: i32, scale: i32) -> ColType {
    ColType {
        name: "c".into(),
        ptype,
        len: -1,
        precision,
        scale,
        nullable: true,
    }
}

fn text(d: Datum, t: &ColType) -> String {
    String::from_utf8(cell_text(&d, t).expect("not null")).unwrap()
}

#[test]
fn samples_render_as_the_reference() {
    // research note [wire] §3.2
    let b = col(T::BOOLEAN, 0, 0);
    assert_eq!(text(Datum::Bool(false), &b), "0");
    assert_eq!(text(Datum::Bool(true), &b), "1");
    for (v, s) in [
        (-127, "-127"),
        (-128, "-128"),
        (-9, "-9"),
        (6001215, "6001215"),
    ] {
        assert_eq!(text(Datum::Int(v), &col(T::BIGINT, 0, 0)), s);
    }
    assert_eq!(
        text(Datum::Int(-999999999), &col(T::LARGEINT, 0, 0)),
        "-999999999"
    );
    let f = col(T::FLOAT, 0, 0);
    for (v, s) in [
        (6.6f32, "6.6"),
        (-32769.79, "-32769.79"),
        (12345678.0, "12345678"),
        (1.2345678e16, "1.2345678e+16"),
        (1.23456e-5, "1.23456e-05"),
        (1e35, "1e+35"),
        (-0.0, "-0"),
    ] {
        assert_eq!(text(Datum::Float(v), &f), s, "FLOAT {v:e}");
    }
    let d = col(T::DOUBLE, 0, 0);
    for (v, s) in [
        (-6349.399999999999f64, "-6349.399999999999"),
        (1.7, "1.7"),
        (1e-308, "1e-308"),
        (1.2345678901234568e16, "1.2345678901234568e+16"),
        (-1.7976931348623157e308, "-1.7976931348623157e+308"),
        (f64::NAN, "NaN"),
        (f64::INFINITY, "Infinity"),
        (f64::NEG_INFINITY, "-Infinity"),
        (0.1, "0.1"),
        (1e16, "1e+16"),
    ] {
        assert_eq!(text(Datum::Double(v), &d), s, "DOUBLE {v:e}");
    }
    assert_eq!(text(Datum::Int(12345), &col(T::DECIMAL32, 9, 2)), "123.45");
    assert_eq!(text(Datum::Int(369), &col(T::DECIMAL128I, 38, 0)), "369");
    assert_eq!(text(Datum::Int(1100), &col(T::DECIMAL64, 20, 3)), "1.100");
    assert_eq!(text(Datum::Int(1500), &col(T::DECIMAL32, 9, 3)), "1.500");
    assert_eq!(text(Datum::Int(-5), &col(T::DECIMAL32, 9, 3)), "-0.005");
    assert_eq!(
        text(Datum::Int(0), &col(T::DECIMAL128I, 38, 38)),
        format!("0.{}", "0".repeat(38))
    );
    let date = doris_exec::value::parse_datev2("2017-10-01").unwrap();
    assert_eq!(
        text(Datum::Int(date.into()), &col(T::DATEV2, 0, 0)),
        "2017-10-01"
    );
    for (s, scale) in [
        ("2020-01-02 00:00:00", 0),
        ("2020-01-02 00:00:00.5", 1),
        ("2020-01-02 03:04:11.110", 3),
        ("2020-01-02 03:04:11.110111", 6),
    ] {
        let v = doris_exec::value::parse_datetimev2(s, scale).unwrap();
        assert_eq!(text(Datum::Int(v.into()), &col(T::DATETIMEV2, 0, scale)), s);
    }
    assert_eq!(
        text(Datum::Bytes("仓".as_bytes().to_vec()), &col(T::CHAR, 0, 0)),
        "仓"
    );
    assert_eq!(cell_text(&Datum::Null, &col(T::INT, 0, 0)), None);
}

/// Splits a MySQL text row into its length-encoded cells (None for NULL).
fn cells(mut row: &[u8]) -> Vec<Option<Vec<u8>>> {
    let mut out = Vec::new();
    while let Some((&first, rest)) = row.split_first() {
        let (len, rest) = match first {
            0xFB => {
                out.push(None);
                row = rest;
                continue;
            }
            0xFC => (u16::from_le_bytes([rest[0], rest[1]]) as usize, &rest[2..]),
            0xFD => (
                u32::from_le_bytes([rest[0], rest[1], rest[2], 0]) as usize,
                &rest[3..],
            ),
            0xFE => (
                u64::from_le_bytes(rest[..8].try_into().unwrap()) as usize,
                &rest[8..],
            ),
            n => (n as usize, rest),
        };
        out.push(Some(rest[..len].to_vec()));
        row = &rest[len..];
    }
    out
}

/// Every replayed result reply with rows, with the output column types of its query
/// (None for a type outside M4) — text-protocol queries only.
type CorpusBatch = (String, Vec<Option<ColType>>, TResultBatch, Vec<u8>);

fn corpus_batches() -> Vec<CorpusBatch> {
    let mut out = Vec::new();
    for r in replays::all() {
        let plans: Vec<TPipelineFragmentParamsList> = r
            .steps
            .iter()
            .filter_map(|s| match s {
                Step::ExecPlanFragment { request, .. } => plan_list(request),
                _ => None,
            })
            .collect();
        for s in &r.steps {
            let Step::Fetch { request, reply } = s else {
                continue;
            };
            let Some(bytes) = reply.row_batch.clone() else {
                continue;
            };
            let id = (request.finst_id.hi, request.finst_id.lo);
            let plan = plans.iter().find(|p| {
                p.params_list.iter().flatten().any(|f| {
                    (f.query_id.hi, f.query_id.lo) == id
                        || f.local_params
                            .iter()
                            .flatten()
                            .any(|l| (l.fragment_instance_id.hi, l.fragment_instance_id.lo) == id)
                })
            });
            let plan = plan.unwrap_or_else(|| panic!("{}: no plan for fetch {id:?}", r.suite));
            let result = plan
                .params_list
                .iter()
                .flatten()
                .find(|f| {
                    f.fragment
                        .as_ref()
                        .and_then(|x| x.output_sink.as_ref())
                        .is_some_and(|s| s.type_ == TDataSinkType::RESULT_SINK)
                })
                .expect("result fragment");
            if result
                .query_options
                .as_ref()
                .and_then(|o| o.mysql_row_binary_format)
                == Some(true)
            {
                continue;
            }
            let exprs = result
                .fragment
                .as_ref()
                .and_then(|f| f.output_exprs.clone())
                .unwrap_or_default();
            let types = exprs
                .iter()
                .map(|e| {
                    e.nodes
                        .first()
                        .and_then(|n| ColType::from_type_desc("c", &n.type_, true))
                        .filter(ColType::supported)
                })
                .collect();
            let mut b = bytes.as_slice();
            let batch =
                TResultBatch::read_from_in_protocol(&mut TBinaryInputProtocol::new(&mut b, true))
                    .expect("TResultBatch");
            out.push((r.suite.clone(), types, batch, bytes));
        }
    }
    out
}

/// A recorded cell as the datum doris-rs would hold for it.
fn datum(cell: &[u8], t: &ColType) -> Datum {
    // a reply's text is the value itself: an expression's CHAR(n) result may be longer than n
    // (the load path's cut to n does not apply)
    if matches!(t.ptype, T::CHAR | T::VARCHAR | T::STRING) {
        return Datum::Bytes(cell.to_vec());
    }
    let s = std::str::from_utf8(cell).expect("utf8 cell");
    let v = parse_text(s, t).unwrap_or_else(|| panic!("{s:?} does not parse as {t:?}"));
    to_datum(&v, t, false).unwrap_or_else(|e| panic!("{s:?} as {t:?}: {e:?}"))
}

#[test]
fn every_corpus_cell_round_trips() {
    let (mut checked, mut skipped) = (0, 0);
    for (suite, types, batch, _) in corpus_batches() {
        for row in &batch.rows {
            for (cell, t) in cells(row).iter().zip(&types) {
                let (Some(cell), Some(t)) = (cell, t) else {
                    skipped += 1;
                    continue;
                };
                let got = cell_text(&datum(cell, t), t);
                assert_eq!(
                    got.as_deref(),
                    Some(cell.as_slice()),
                    "{suite}: {t:?} {:?}",
                    String::from_utf8_lossy(cell)
                );
                checked += 1;
            }
        }
    }
    eprintln!("{checked} cells checked; {skipped} NULL or outside M4");
    assert!(checked > 1000, "only {checked} cells checked");
}

#[test]
fn result_batch_round_trips() {
    let mut checked = 0;
    for (suite, types, batch, bytes) in corpus_batches() {
        let Some(types) = types.into_iter().collect::<Option<Vec<ColType>>>() else {
            continue;
        };
        let rows = batch
            .rows
            .iter()
            .map(|row| {
                cells(row)
                    .iter()
                    .zip(&types)
                    .map(|(c, t)| c.as_deref().map_or(Datum::Null, |c| datum(c, t)))
                    .collect()
            })
            .collect();
        assert_eq!(result_batch(&Block { types, rows }), bytes, "{suite}");
        checked += 1;
    }
    assert!(checked > 50, "only {checked} batches checked");
}
