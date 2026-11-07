//! Write fragments from the corpus: they decode, plan as VALUES inserts, and their rows route
//! to the tablets the reference BE wrote.
use std::collections::BTreeMap;

use doris_exec::fragment::{decode_fragments, plan_write, WritePlan};
use doris_exec::route::Router;
use doris_exec::ExecError;
use doris_fixtures::values::values_vectors;
use doris_thrift::gen::plan_nodes::TPlanNodeType;

#[test]
fn corpus_values_fragments_plan() {
    let mut planned = 0;
    for v in values_vectors() {
        let list = decode_fragments(&v.request).unwrap_or_else(|e| panic!("{}: {e}", v.dir));
        let params = list.params_list.unwrap().remove(0);
        match plan_write(&params) {
            Ok(f) => {
                assert!(matches!(f.plan, WritePlan::Values), "{}", v.dir);
                planned += 1;
            }
            // nested types and functions outside M4 are named, not run
            Err(ExecError::Unsupported { what }) => assert!(!what.is_empty(), "{}", v.dir),
            Err(e) => panic!("{}: {e}", v.dir),
        }
    }
    assert!(planned >= 40, "{planned}");
}

#[test]
fn a_select_fragment_is_refused() {
    let v = &values_vectors()[0];
    let mut params = v.params();
    let fragment = params.fragment.as_mut().unwrap();
    fragment.plan.as_mut().unwrap().nodes[0].node_type = TPlanNodeType::OLAP_SCAN_NODE;
    match plan_write(&params) {
        Err(ExecError::Unsupported { what }) => assert_eq!(what, "OLAP_SCAN_NODE"),
        other => panic!("{other:?}"),
    }
}

#[test]
fn routing_reproduces_the_corpus_delta_rows() {
    let mut checked = 0;
    for v in values_vectors() {
        let Ok(f) = plan_write(&v.params()) else { continue };
        let Ok(router) = Router::new(&f) else { continue };
        let Ok(rows) = f.values_rows() else { continue };
        let mut per_tablet: BTreeMap<i64, i64> = BTreeMap::new();
        for row in rows {
            let Ok(row) = row else { continue };
            let Ok(routed) = router.route(&row) else { continue };
            for (_, tablet) in routed.tablets {
                *per_tablet.entry(tablet).or_default() += 1;
            }
        }
        // the reference's publish: rows per base tablet, tablets with rows only
        let base = v.publish.publish_version_req.as_ref().unwrap().base_tablet_ids.clone().unwrap_or_default();
        let mine: BTreeMap<i64, i64> = per_tablet.into_iter().filter(|(t, _)| base.is_empty() || base.contains(t)).collect();
        let theirs: BTreeMap<i64, i64> = v.finish.table_id_to_tablet_id_to_delta_num_rows.clone().unwrap_or_default().into_values().flatten().collect();
        assert_eq!(mine, theirs, "{} ({})", v.dir, v.suite);
        checked += 1;
    }
    assert!(checked >= 40, "{checked}");
}
