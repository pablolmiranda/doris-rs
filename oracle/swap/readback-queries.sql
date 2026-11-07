# The readback check's queries, run on the C++ BE: "<name>: <query>", one per line. Every query
# orders by all columns, except those named *_unordered, whose row order comes from the storage.
all_types_all: SELECT * FROM m4.all_types ORDER BY k1, k2, k3, c_bigint, c_largeint, c_bool, c_float, c_double, c_d32, c_d64, c_d128, c_date, c_dt, c_char, c_varchar, c_string
all_types_unordered: SELECT * FROM m4.all_types
all_types_point: SELECT k1, k2, k3, c_bigint, c_string FROM m4.all_types WHERE k1 = 1 AND k2 = 10 AND k3 = 100 ORDER BY c_bigint
all_types_range: SELECT k1, k2, k3 FROM m4.all_types WHERE k1 BETWEEN 0 AND 5 ORDER BY k1, k2, k3
all_types_null: SELECT k1, c_varchar FROM m4.all_types WHERE c_varchar IS NULL ORDER BY k1, c_varchar
all_types_value: SELECT k1, c_d64, c_date FROM m4.all_types WHERE c_d64 > 0 AND c_date >= '2000-01-01' ORDER BY k1, c_d64, c_date
all_types_count: SELECT COUNT(*), COUNT(k1), COUNT(c_string) FROM m4.all_types
all_types_minmax: SELECT MIN(k3), MAX(k3), MIN(c_largeint), MAX(c_largeint), MIN(c_dt), MAX(c_dt), MIN(c_d128), MAX(c_d128), MIN(c_char), MAX(c_varchar) FROM m4.all_types
buckets_all: SELECT * FROM m4.buckets ORDER BY k, v
buckets_point: SELECT * FROM m4.buckets WHERE k = 5 ORDER BY v
buckets_range: SELECT * FROM m4.buckets WHERE k > 7 AND k <= 13 ORDER BY k, v
buckets_count: SELECT COUNT(*), MIN(k), MAX(k) FROM m4.buckets
ranged_all: SELECT * FROM m4.ranged ORDER BY dt, k, v
ranged_partition: SELECT * FROM m4.ranged WHERE dt >= '2025-01-01' ORDER BY dt, k, v
ranged_null: SELECT * FROM m4.ranged WHERE v IS NULL ORDER BY dt
ranged_count: SELECT COUNT(*), MIN(dt), MAX(dt) FROM m4.ranged
big_count: SELECT COUNT(*), COUNT(c), COUNT(k), COUNT(s), SUM(v) FROM m4.big
big_minmax: SELECT MIN(c), MAX(c), MIN(k), MAX(k), MIN(v), MAX(v), MIN(s), MAX(s) FROM m4.big
big_point: SELECT * FROM m4.big WHERE c = 'c0419' ORDER BY c, k, v, s
big_range: SELECT * FROM m4.big WHERE c >= 'c0100' AND c < 'c0105' ORDER BY c, k, v, s
big_null_key: SELECT * FROM m4.big WHERE c IS NULL ORDER BY k, v, s
big_value: SELECT c, k, v FROM m4.big WHERE v BETWEEN 0 AND 50000000 ORDER BY c, k, v
big_head_unordered: SELECT * FROM m4.big LIMIT 20
zstd_all: SELECT * FROM m5.zstd ORDER BY k, v, s, d
zstd_count: SELECT COUNT(*), COUNT(v), COUNT(s), COUNT(d) FROM m5.zstd
join: SELECT b.k, b.v, r.dt, r.v FROM m4.buckets b JOIN m4.ranged r ON b.k = r.k ORDER BY b.k, b.v, r.dt, r.v
