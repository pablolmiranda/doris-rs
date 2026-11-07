//! Sorting: per-key direction and NULL placement, top-n with offset, merging sorted inputs, and
//! the rows a lazy materialization fetches back by row id.
use std::sync::{Arc, Mutex};

use doris_exec::block::Block;
use doris_exec::sort::{merge_sorted, sort_block, top_n, SortKey};
use doris_exec::value::ColType;
use doris_fixtures::build::{col, create_tablet, tcolumn, template, ty};
use doris_proto::doris::{PUniqueId, RowsetStatePb};
use doris_segment::writer::Datum;
use doris_storage::reader::{fetch_by_row_ids, row_id, scan_tablet, ColTypeRef, ScanColumn};
use doris_storage::rowset::write_rowset;
use doris_storage::{DataDir, TabletManager, TabletOptions};
use doris_thrift::gen::types::{TKeysType, TPrimitiveType as T};

fn int() -> ColType {
    ColType { name: String::new(), ptype: T::INT, len: -1, precision: 0, scale: 0, nullable: true }
}

fn i(v: Option<i128>) -> Datum {
    v.map_or(Datum::Null, Datum::Int)
}

fn block(rows: &[(Option<i128>, Option<i128>)]) -> Block {
    Block { types: vec![int(), int()], rows: rows.iter().map(|(a, b)| vec![i(*a), i(*b)]).collect() }
}

#[test]
fn nulls_first_per_key() {
    let mut b = block(&[(Some(2), Some(1)), (None, Some(5)), (Some(1), None), (Some(1), Some(3)), (Some(2), None), (None, None), (Some(1), Some(9))]);
    sort_block(&mut b, &[SortKey { column: 0, asc: true, nulls_first: true }, SortKey { column: 1, asc: false, nulls_first: false }]);
    let want = block(&[(None, Some(5)), (None, None), (Some(1), Some(9)), (Some(1), Some(3)), (Some(1), None), (Some(2), Some(1)), (Some(2), None)]);
    assert_eq!(b.rows, want.rows);
    // descending with NULLs first, ascending with NULLs last
    let mut b = block(&[(Some(1), None), (None, None), (Some(3), None)]);
    sort_block(&mut b, &[SortKey { column: 0, asc: false, nulls_first: true }]);
    assert_eq!(b.rows, block(&[(None, None), (Some(3), None), (Some(1), None)]).rows);
    sort_block(&mut b, &[SortKey { column: 0, asc: true, nulls_first: false }]);
    assert_eq!(b.rows, block(&[(Some(1), None), (Some(3), None), (None, None)]).rows);
}

#[test]
fn topn_with_offset() {
    let rows: Vec<(Option<i128>, Option<i128>)> = [7, 3, 9, 1, 5, 0, 8, 2, 6, 4].iter().map(|v| (Some(*v), Some(v * 10))).collect();
    let got = top_n(block(&rows), &[SortKey { column: 0, asc: true, nulls_first: true }], 2, Some(3));
    assert_eq!(got.rows, block(&[(Some(2), Some(20)), (Some(3), Some(30)), (Some(4), Some(40))]).rows);
    // an offset past the rows gives none
    assert!(top_n(block(&rows), &[SortKey { column: 0, asc: true, nulls_first: true }], 20, Some(3)).rows.is_empty());
}

#[test]
fn merge_of_sorted_inputs() {
    let key = [SortKey { column: 0, asc: true, nulls_first: true }];
    let inputs = vec![
        block(&[(Some(1), Some(0)), (Some(4), Some(0)), (Some(7), Some(0))]),
        block(&[(None, Some(1)), (Some(2), Some(1)), (Some(9), Some(1))]),
        block(&[(Some(3), Some(2)), (Some(4), Some(2))]),
    ];
    let got = merge_sorted(inputs, &key);
    let firsts: Vec<Datum> = got.rows.iter().map(|r| r[0].clone()).collect();
    assert_eq!(firsts, [None, Some(1), Some(2), Some(3), Some(4), Some(4), Some(7), Some(9)].map(i).to_vec());
}

#[test]
fn materialization_fetches_the_rest() {
    let tmp = tempfile::tempdir().unwrap();
    let mgr = Arc::new(Mutex::new(TabletManager::open(vec![DataDir::open(tmp.path(), "127.0.0.1").unwrap()], TabletOptions::default()).unwrap()));
    let cols = [col("k", ty(T::INT), false), col("v", ty(T::VARCHAR), true)];
    let (_, t) = template();
    let col0 = t.fragment.unwrap().plan.unwrap().nodes[0].olap_scan_node.clone().unwrap().columns_desc.unwrap()[0].clone();
    let tcols = cols.iter().enumerate().map(|(n, c)| tcolumn(&col0, n, c)).collect();
    let mut m = mgr.lock().unwrap();
    m.create_tablet(&create_tablet(77, tcols, TKeysType::DUP_KEYS, 1)).unwrap();
    // two rowsets, so row ids span segments
    for (version, keys) in [(2, [5, 1, 9]), (3, [4, 8, 2])] {
        let meta = m.tablet(77).unwrap().clone();
        let dir = m.tablet_path(77).unwrap();
        let id = m.next_rowset_id();
        let rows = keys.iter().map(|k| vec![Datum::Int(*k), Datum::Bytes(format!("v{k}").into_bytes())]).collect();
        let mut rs = write_rowset(&dir, &meta, &id, version, &PUniqueId { hi: 1, lo: version }, rows).unwrap().meta;
        rs.start_version = Some(version);
        rs.end_version = Some(version);
        rs.set_rowset_state(RowsetStatePb::Visible);
        m.add_visible_rowset(77, rs).unwrap();
    }
    let k = ScanColumn { unique_id: 0, ty: ColTypeRef { ptype: T::INT, precision: 0, scale: 0 } };
    let v = ScanColumn { unique_id: 1, ty: ColTypeRef { ptype: T::VARCHAR, precision: 0, scale: 0 } };
    // the scan emits the key and a row id; the top-n keeps 3; the rest is fetched by row id
    let mut lazy = Block { types: vec![int(), ColType { ptype: T::STRING, ..int() }], rows: vec![] };
    for (s, segment) in scan_tablet(&m, 77, 3, std::slice::from_ref(&k)).unwrap().into_iter().enumerate() {
        for (ordinal, row) in segment.into_iter().enumerate() {
            lazy.rows.push(vec![row[0].clone(), Datum::Bytes(row_id(77, 3, s as u32, ordinal as u32))]);
        }
    }
    let lazy = top_n(lazy, &[SortKey { column: 0, asc: true, nulls_first: true }], 0, Some(3));
    let ids: Vec<Vec<u8>> = lazy.rows.iter().map(|r| match &r[1] { Datum::Bytes(b) => b.clone(), _ => panic!() }).collect();
    let fetched = fetch_by_row_ids(&m, &ids, std::slice::from_ref(&v)).unwrap();
    let got: Vec<Vec<Datum>> = lazy.rows.iter().zip(fetched).map(|(r, f)| vec![r[0].clone(), f[0].clone()]).collect();
    // the same as sorting the full rows
    let mut full = Block { types: vec![int(), int()], rows: scan_tablet(&m, 77, 3, &[k, v]).unwrap().into_iter().flatten().collect() };
    sort_block(&mut full, &[SortKey { column: 0, asc: true, nulls_first: true }]);
    full.rows.truncate(3);
    assert_eq!(got, full.rows);
    assert!(fetch_by_row_ids(&m, &[vec![1, 2, 3]], &[]).is_err(), "a malformed row id is an error");
}
