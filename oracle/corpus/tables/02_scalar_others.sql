-- Golden corpus v1: floating point, decimal, date/time, and string types.
CREATE TABLE scalar_others (
    k          INT NOT NULL,
    c_float    FLOAT,
    c_double   DOUBLE,
    c_decimal  DECIMAL(18, 4),
    c_date     DATEV2,
    c_datetime DATETIMEV2(6),
    c_char     CHAR(8),
    c_varchar  VARCHAR(64),
    c_string   STRING
) DUPLICATE KEY(k)
DISTRIBUTED BY HASH(k) BUCKETS 1
PROPERTIES ("replication_num" = "1", "disable_auto_compaction" = "true");

INSERT INTO scalar_others VALUES
  (1,  0.0,        0.0,         0.0000,              '1970-01-01', '1970-01-01 00:00:00.000000', '',         '',                                                                 ''),
  (2, -0.0,       -0.0,        -0.0001,              '9999-12-31', '9999-12-31 23:59:59.999999', 'abcdefgh', '0123456789012345678901234567890123456789012345678901234567890123', 'sixty-four plus'),
  (3,  1.5,        2.25,        12345678901234.5678, '2024-02-29', '2024-02-29 12:34:56.789012', 'a',        'héllo wörld ✓',                                                    'ünïcödé ✓ 日本語'),
  (4, -1.5,       -2.25,       -12345678901234.5678, '2000-01-01', '2000-01-01 00:00:00.000001', 'AB',       'repeat',                                                           'repeat'),
  (5,  NULL,       NULL,        NULL,                NULL,         NULL,                         NULL,       NULL,                                                               NULL),
  (6,  3.4028235e38, 1.7976931348623157e308, 99999999999999.9999, '2024-12-31', '2024-12-31 23:59:59.000000', 'xyz',  'repeat',                                                 'repeat'),
  (7,  1.0e-10,    1.0e-300,    0.0001,              '1900-05-05', '1900-05-05 05:05:05.500000', 'Z',        'x',                                                                'y'),
  (8,  100.125,    100.125,     100.1250,            '2024-01-01', '2024-01-01 00:00:00.000000', 'eight',    'repeat',                                                           'repeat'),
  (9,  NULL,       1.0,         NULL,                '2024-01-02', NULL,                         NULL,       'mixed',                                                            NULL),
  (10, 7.0,        NULL,        7.7777,              NULL,         '2024-01-03 03:03:03.333333', 'ten',      NULL,                                                               'ten');

INSERT INTO scalar_others
SELECT 1000 + number,
       CAST(number AS FLOAT) / 4,
       CAST(number AS DOUBLE) / 8,
       CAST(CAST(number AS DECIMAL(18,4)) / 7 AS DECIMAL(18,4)),
       DATE_ADD('2024-01-01', INTERVAL number DAY),
       SECONDS_ADD(CAST('2024-01-01 00:00:00.123456' AS DATETIMEV2(6)), number),
       CONCAT('c', number % 3),
       CONCAT('v', number % 5),
       IF(number % 10 = 0, NULL, CONCAT('s', number))
FROM numbers("number" = "500");
