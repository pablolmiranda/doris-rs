//! now(), current_date() and text with a time zone, as the C++ BE evaluates them
//! (function_date_or_datetime_computation.h, cast_to_datetimev2_impl.hpp, timezone_utils.cpp).
use doris_exec::clock::{find_zone, with_clock, QueryClock, QueryZone};
use doris_exec::row_text::{datetimev2_text, datev2_text};
use doris_exec::value::{parse_text, ColType, Value};
use doris_thrift::gen::palo_internal_service::TQueryGlobals;
use doris_thrift::gen::types::TPrimitiveType as T;

fn globals(ms: i64, nanos: Option<i32>, zone: &str) -> TQueryGlobals {
    TQueryGlobals::new(String::from("2010-01-01 00:00:00"), ms, zone.to_string(), None, nanos, None)
}

fn dt(scale: i32) -> ColType {
    ColType {
        name: "k".into(),
        ptype: T::DATETIMEV2,
        len: -1,
        precision: 0,
        scale,
        nullable: true,
    }
}

#[test]
fn now_is_the_query_instant() {
    // 2010-01-01 00:15:30.123 UTC
    let c = QueryClock::from_globals(&globals(1_262_304_930_123, Some(123_456_789), "Asia/Shanghai"));
    assert_eq!(datetimev2_text(c.now(0), 0), "2010-01-01 08:15:30");
    // now(3): the nanoseconds cut to 3 digits, not rounded
    assert_eq!(datetimev2_text(c.now(3), 3), "2010-01-01 08:15:30.123");
    assert_eq!(datetimev2_text(c.now(6), 6), "2010-01-01 08:15:30.123456");
    assert_eq!(datev2_text(c.today()), "2010-01-01");
}

#[test]
fn current_date_uses_the_query_zone() {
    // 2010-01-01 23:30:00 UTC is 2010-01-02 in Shanghai
    let c = QueryClock::from_globals(&globals(1_262_388_600_000, None, "Asia/Shanghai"));
    assert_eq!(datev2_text(c.today()), "2010-01-02");
    let c = QueryClock::from_globals(&globals(1_262_388_600_000, None, "UTC"));
    assert_eq!(datev2_text(c.today()), "2010-01-01");
}

#[test]
fn without_a_time_zone_now_string_is_read_in_plus_eight() {
    let g = TQueryGlobals::new(String::from("2010-01-05 16:15:30"), None, None, None, None, None);
    let c = QueryClock::from_globals(&g);
    assert_eq!(datetimev2_text(c.now(0), 0), "2010-01-05 16:15:30");
}

fn cast_in(zone: &str, text: &str, t: &ColType) -> Option<Value> {
    let c = QueryClock::from_globals(&globals(0, None, zone));
    with_clock(&c, || parse_text(text, t))
}

#[test]
fn cast_with_named_zone() {
    let d = |s: &str| Some(Value::Date(s.into()));
    assert_eq!(cast_in("+02:00", "2010-01-05 08:15:30Europe/London", &dt(0)), d("2010-01-05 10:15:30"));
    assert_eq!(cast_in("+02:00", "2010-08-05 08:15:30Europe/London", &dt(0)), d("2010-08-05 09:15:30"));
    assert_eq!(cast_in("+02:00", "2010-01-05 08:15:30+08:00", &dt(0)), d("2010-01-05 02:15:30"));
    assert_eq!(cast_in("+02:00", "2010-01-05 08:15:30UTC", &dt(0)), d("2010-01-05 10:15:30"));
    assert_eq!(cast_in("+02:00", "2020-12-12T12:12:12asia/shanghai", &dt(0)), d("2020-12-12 06:12:12"));
    assert_eq!(cast_in("+02:00", "2022-06-01T01:02:55+04:30", &dt(0)), d("2022-05-31 22:32:55"));
    assert_eq!(cast_in("+02:00", "2022-02-01 01:02:55Z", &dt(0)), d("2022-02-01 03:02:55"));
    // the fraction is kept as it is
    assert_eq!(
        cast_in("+02:00", "2010-01-05 08:15:30.123Europe/London", &dt(3)),
        d("2010-01-05 10:15:30.123")
    );
    // no zone: no conversion
    assert_eq!(cast_in("+02:00", "2010-01-05 08:15:30", &dt(0)), d("2010-01-05 08:15:30"));
}

#[test]
fn unknown_zone_is_null() {
    assert_eq!(cast_in("+02:00", "2010-01-05 08:15:30Mars/Base", &dt(0)), None);
    assert_eq!(cast_in("+02:00", "2010-01-05 08:15:30+05:15", &dt(0)), None);
}

#[test]
fn date_target_checks_but_keeps_date() {
    let date = ColType {
        name: "k".into(),
        ptype: T::DATEV2,
        len: -1,
        precision: 0,
        scale: 0,
        nullable: true,
    };
    let v = cast_in("+02:00", "2010-01-05 23:15:30+08:00", &date).expect("a date");
    assert_eq!(v, Value::Date("2010-01-05".into()));
    assert_eq!(cast_in("+02:00", "2010-01-05 23:15:30Mars/Base", &date), None);
}

#[test]
fn zone_names() {
    assert!(matches!(find_zone("CST"), Some(QueryZone::Named(z)) if z.name() == "Asia/Shanghai"));
    assert!(matches!(find_zone("z"), Some(QueryZone::Named(z)) if z.name() == "Zulu"));
    assert_eq!(find_zone("+05:30"), Some(QueryZone::Offset(5 * 3600 + 1800)));
    assert_eq!(find_zone("-08:00"), Some(QueryZone::Offset(-8 * 3600)));
    assert_eq!(find_zone("GMT+5"), Some(QueryZone::Offset(5 * 3600)));
    assert_eq!(find_zone("UTC-3:30"), Some(QueryZone::Offset(-(3 * 3600 + 1800))));
    assert!(find_zone("Mars/Base").is_none());
    assert!(find_zone("+15:00").is_none());
}

#[test]
fn now_and_current_date_evaluate_under_a_clock() {
    use doris_exec::block::Block;
    use doris_exec::eval::{check_read, eval_column, SlotMap};
    use doris_fixtures::build::{call, int, scalar, ty};
    use doris_segment::writer::Datum;
    let c = QueryClock::from_globals(&globals(1_262_304_930_123, Some(123_456_789), "Asia/Shanghai"));
    let block = Block {
        types: vec![],
        rows: vec![vec![], vec![]],
    };
    let now3 = call("now", scalar(T::DATETIMEV2, -1, 0, 3), vec![int(3, ty(T::INT))]);
    check_read(&now3).expect("supported");
    let (_, v) = with_clock(&c, || eval_column(&now3, &block, &SlotMap::default())).unwrap();
    assert_eq!(v, vec![Datum::Int(c.now(3).into()); 2]);
    let today = call("current_date", ty(T::DATEV2), vec![]);
    check_read(&today).expect("supported");
    let (_, v) = with_clock(&c, || eval_column(&today, &block, &SlotMap::default())).unwrap();
    assert_eq!(v, vec![Datum::Int(c.today().into()); 2]);
    // no clock: an error, not a made-up instant
    assert!(eval_column(&today, &block, &SlotMap::default()).is_err());
}

#[test]
fn now_evaluates_in_a_load() {
    use doris_exec::expr::{check, eval, Slots};
    use doris_fixtures::build::{call, scalar, ty};
    let c = QueryClock::from_globals(&globals(1_262_304_930_123, Some(123_456_789), "Asia/Shanghai"));
    let now = call("now", scalar(T::DATETIMEV2, -1, 0, 0), vec![]);
    check(&now).expect("supported");
    assert_eq!(
        with_clock(&c, || eval(&now, &Slots::new())).unwrap(),
        Value::Date("2010-01-01 08:15:30".into())
    );
    let today = call("current_date", ty(T::DATEV2), vec![]);
    check(&today).expect("supported");
    assert_eq!(with_clock(&c, || eval(&today, &Slots::new())).unwrap(), Value::Date("2010-01-01".into()));
}

#[test]
fn a_query_plan_carries_its_clock() {
    use doris_exec::query::plan_query;
    use doris_fixtures::build::{col, scan_query, ty};
    use doris_thrift::gen::palo_internal_service::TPipelineFragmentParamsList;
    use doris_thrift::gen::types::TUniqueId;
    let mut p = scan_query(TUniqueId::new(1, 2), 10, 2, &[col("k", ty(T::INT), false)], 4064);
    p.query_globals = Some(globals(1_262_304_930_123, Some(123_456_789), "Asia/Shanghai"));
    let plan = plan_query(&TPipelineFragmentParamsList {
        params_list: Some(vec![p]),
        ..Default::default()
    })
    .expect("plan");
    assert_eq!(datetimev2_text(plan.clock.now(0), 0), "2010-01-01 08:15:30");
}
