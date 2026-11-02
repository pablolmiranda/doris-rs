-- Golden corpus v1: same integer shape as scalar_ints, ZSTD page compression, one rowset.
CREATE TABLE zstd_ints (
    k          INT      NOT NULL,
    c_bool     BOOLEAN,
    c_tinyint  TINYINT,
    c_smallint SMALLINT,
    c_int      INT,
    c_bigint   BIGINT,
    c_largeint LARGEINT
) DUPLICATE KEY(k)
DISTRIBUTED BY HASH(k) BUCKETS 1
PROPERTIES ("replication_num" = "1", "compression" = "zstd");

INSERT INTO zstd_ints
SELECT number,
       number % 3 = 0,
       CAST((number % 256) - 128 AS TINYINT),
       CAST((number * 97) % 65536 - 32768 AS SMALLINT),
       IF(number % 13 = 0, NULL, CAST(number % 5 AS INT)),
       CAST(number AS BIGINT) * -1000003,
       CAST(number AS LARGEINT) * -1000000007 * 1000000007
FROM numbers("number" = "1000");
