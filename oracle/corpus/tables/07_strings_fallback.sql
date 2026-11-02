-- Golden corpus v1: string columns with a small dictionary page so the writer falls back from
-- dictionary codewords to plain pages partway through each column. The 1024 requested here is
-- aligned up by the FE to 4096 (see expected/strings_fallback.ddl.sql), a quarter of the
-- 256 KB default. Also an interior-NUL
-- row: the reference reads CHAR back truncated at the first NUL, VARCHAR and STRING keep it.
CREATE TABLE strings_fallback (
    k         INT NOT NULL,
    c_char    CHAR(20),
    c_varchar VARCHAR(64),
    c_string  STRING
) DUPLICATE KEY(k)
DISTRIBUTED BY HASH(k) BUCKETS 1
PROPERTIES ("replication_num" = "1", "storage_dict_page_size" = "1024", "disable_auto_compaction" = "true");

INSERT INTO strings_fallback VALUES
  (1, '',                     '',                                                                 ''),
  (2, NULL,                   NULL,                                                               NULL),
  (3, unhex('610062'),        unhex('610062'),                                                    unhex('610062')),
  (4, 'twenty-chars-exactly', '0123456789012345678901234567890123456789012345678901234567890123', 'a string well past sixty-four bytes, so that the plain page holds a long value'),
  (5, 'ünïcödé',              'ünïcödé ✓ 日本語',                                                   '日本語のテキスト'),
  (6, ' leading space',       'trailing space ',                                                  '  ');

INSERT INTO strings_fallback
SELECT 1000 + number,
       CONCAT('c-', number),
       CONCAT('varchar-', number, '-', number * 7919),
       IF(number % 17 = 0, NULL, CONCAT('string-', number))
FROM numbers("number" = "3000");
