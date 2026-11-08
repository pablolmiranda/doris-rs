-- The M4 load dataset: duplicate-key tables written by INSERT ... VALUES and stream load.
-- Run by run_dataset.py (readback check, cpp-loaded fixture). Stream loads are
-- "-- stream_load <db>.<table> <file> <header>=<value> ..." lines; files are in load-dataset/.
CREATE DATABASE m4;

CREATE TABLE m4.all_types (
  k1 TINYINT, k2 SMALLINT, k3 INT,
  c_bigint BIGINT, c_largeint LARGEINT, c_bool BOOLEAN, c_float FLOAT, c_double DOUBLE,
  c_d32 DECIMALV3(9, 2), c_d64 DECIMALV3(18, 4), c_d128 DECIMALV3(38, 10),
  c_date DATEV2, c_dt DATETIMEV2(3), c_char CHAR(8), c_varchar VARCHAR(32), c_string STRING
) DUPLICATE KEY(k1, k2, k3) DISTRIBUTED BY HASH(k1) BUCKETS 1 PROPERTIES ('replication_num' = '1');

INSERT INTO m4.all_types VALUES
  (1, 10, 100, 1000, 170141183460469231731687303715884105727, true, 1.5, 2.25, 1234567.89, 12345678901234.5678, 1234567890123456789012345678.0123456789, '2024-02-29', '2024-02-29 23:59:59.123', 'abc', 'hello', 'a longer string value'),
  (-128, -32768, -2147483648, -9223372036854775808, -170141183460469231731687303715884105728, false, -0.0, -1e300, -9999999.99, -99999999999999.9999, -9999999999999999999999999999.9999999999, '0001-01-01', '0001-01-01 00:00:00.000', '', '', ''),
  (127, 32767, 2147483647, 9223372036854775807, 0, true, 3.4e38, 1.7e308, 0.01, 0.0001, 0.0000000001, '9999-12-31', '9999-12-31 23:59:59.999', 'zzzzzzzz', 'z', 'z'),
  (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
  (1, 10, 100, 1001, 1, false, 0.1, 0.2, 1.00, 1.0000, 1.0000000000, '2000-01-01', '2000-01-01 12:00:00.5', 'abc', 'dup key', 'second row with key (1,10,100)'),
  (0, 0, 0, 0, 0, NULL, 0, 0, 0, 0, 0, '1970-01-01', '1970-01-01 00:00:00', 'x', NULL, NULL);
-- stream_load m4.all_types all_types.csv column_separator=,
-- stream_load m4.all_types all_types.json format=json read_json_by_line=true

CREATE TABLE m4.buckets (k INT NOT NULL, v VARCHAR(16)) DUPLICATE KEY(k)
  DISTRIBUTED BY HASH(k) BUCKETS 8 PROPERTIES ('replication_num' = '1');
INSERT INTO m4.buckets VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e'), (6, 'f'), (7, 'g'),
  (8, 'h'), (9, 'i'), (10, 'j'), (11, 'k'), (12, 'l'), (13, 'm'), (14, 'n'), (15, 'o'), (16, 'p'),
  (17, 'q'), (18, 'r'), (19, 's'), (20, 't');
INSERT INTO m4.buckets VALUES (5, 'second e');

CREATE TABLE m4.ranged (dt DATEV2 NOT NULL, k INT, v STRING) DUPLICATE KEY(dt)
  PARTITION BY RANGE(dt) (
    PARTITION p1 VALUES LESS THAN ('2024-01-01'),
    PARTITION p2 VALUES LESS THAN ('2025-01-01'),
    PARTITION p3 VALUES LESS THAN ('2026-01-01'))
  DISTRIBUTED BY HASH(k) BUCKETS 2 PROPERTIES ('replication_num' = '1');
INSERT INTO m4.ranged VALUES ('2023-06-01', 1, 'p1 a'), ('2023-12-31', 2, 'p1 b'),
  ('2025-03-15', 3, 'p3 a'), ('2025-12-31', 4, NULL);

CREATE TABLE m4.big (c CHAR(8), k INT, v BIGINT, s VARCHAR(64)) DUPLICATE KEY(c, k)
  DISTRIBUTED BY HASH(k) BUCKETS 2 PROPERTIES ('replication_num' = '1');
-- stream_load m4.big big.csv column_separator=,

-- M5: one bucket and one load past the 256 KB flush threshold, so the C++ BE writes ZSTD pages.
CREATE DATABASE m5;
CREATE TABLE m5.zstd (k INT, v BIGINT, s VARCHAR(32), d DOUBLE) DUPLICATE KEY(k)
  DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num' = '1');
-- stream_load m5.zstd zstd.csv column_separator=,
