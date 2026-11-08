//! The stream load samples load and hold what the M4 tests replay.
use doris_fixtures::stream_loads::{stream_loads, LoadEnd};

#[test]
fn samples_decode() {
    let all = stream_loads();
    assert_eq!(all.len(), 60);
    for s in &all {
        assert!(s.path.ends_with("/_stream_load"), "{}", s.dir);
        assert!(s.reply.trim_start().starts_with('{'), "{}: {}", s.dir, s.reply);
        assert!(s.put.1.pipeline_params.is_some(), "{}", s.dir);
        let txn = s.begin.1.txn_id.unwrap();
        match &s.end {
            LoadEnd::Commit(c, _) => assert_eq!(c.txn_id, txn),
            LoadEnd::Rollback(r, _) => assert_eq!(r.txn_id, txn),
        }
    }
    println!("{}", all[0].reply);
}
