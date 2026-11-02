-- Golden corpus v1: integer scalar types, DUPLICATE model, default (LZ4F) compression.
-- Three INSERTs produce three rowsets; the generated batch has repeated values so
-- dictionary and RLE paths are exercised; the last batch is all-NULL in nullable columns.
CREATE TABLE scalar_ints (
    k          INT      NOT NULL,
    c_bool     BOOLEAN,
    c_tinyint  TINYINT,
    c_smallint SMALLINT,
    c_int      INT,
    c_bigint   BIGINT,
    c_largeint LARGEINT
) DUPLICATE KEY(k)
DISTRIBUTED BY HASH(k) BUCKETS 1
PROPERTIES ("replication_num" = "1");

INSERT INTO scalar_ints VALUES
  (1,  true,  -128, -32768, -2147483648, -9223372036854775808, -170141183460469231731687303715884105728),
  (2,  false,  127,  32767,  2147483647,  9223372036854775807,  170141183460469231731687303715884105727),
  (3,  true,     0,      0,           0,                    0,                                        0),
  (4,  false,   -1,     -1,          -1,                   -1,                                       -1),
  (5,  NULL,  NULL,   NULL,        NULL,                 NULL,                                     NULL),
  (6,  true,     1,      1,           1,                    1,                                        1),
  (7,  false,   42,   4242,      424242,         424242424242,                     42424242424242424242),
  (8,  true,   -42,  -4242,     -424242,        -424242424242,                    -42424242424242424242);

INSERT INTO scalar_ints
SELECT 1000 + number,
       number % 2 = 0,
       CAST((number % 256) - 128 AS TINYINT),
       CAST((number * 31) % 65536 - 32768 AS SMALLINT),
       IF(number % 11 = 0, NULL, CAST(number % 7 AS INT)),
       CAST(number AS BIGINT) * 1000003,
       CAST(number AS LARGEINT) * 1000000007 * 1000000007
FROM numbers("number" = "2000");

INSERT INTO scalar_ints VALUES
  (9001, NULL, NULL, NULL, NULL, NULL, NULL),
  (9002, NULL, NULL, NULL, NULL, NULL, NULL),
  (9003, NULL, NULL, NULL, NULL, NULL, NULL);
