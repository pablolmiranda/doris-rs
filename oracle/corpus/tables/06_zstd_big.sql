-- Golden corpus v1: one large INSERT. The pipeline sink splits it into several segments
-- (one per sink task), each of which still flushes well over the 256 KB
-- segment_compression_threshold_kb with explicit ZSTD so page bodies are stored compressed,
-- and enough rows that INT/BIGINT columns span several 64 KB pages (ordinal index page).
-- c_const and c_mod are low-entropy so ZSTD saves more than the 10% minimum on top of
-- bit-shuffle; c_varchar gives a dictionary page that compresses too.
CREATE TABLE zstd_big (
    k         INT      NOT NULL,
    c_bool    BOOLEAN,
    c_mod     INT,
    c_const   INT,
    c_bigint  BIGINT,
    c_varchar VARCHAR(32)
) DUPLICATE KEY(k)
DISTRIBUTED BY HASH(k) BUCKETS 1
PROPERTIES ("replication_num" = "1", "compression" = "zstd", "disable_auto_compaction" = "true");

INSERT INTO zstd_big
SELECT number,
       number % 2 = 1,
       IF(number % 97 = 0, NULL, CAST(number % 7 AS INT)),
       7,
       CAST(number AS BIGINT) * 10007,
       CONCAT('v', number % 100)
FROM numbers("number" = "200000");
