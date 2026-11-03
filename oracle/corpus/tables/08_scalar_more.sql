-- Golden corpus v1: decimal and datetime scales, IP types, and the legacy DATE, DATETIME and
-- DECIMALV2 storage formats that upgraded clusters still hold. The generated batch is large
-- enough that bit-shuffle pages with 3-byte (DATE v1) and 12-byte (DECIMALV2) elements span
-- more than one LZ4 block.
CREATE TABLE scalar_more (
    k            INT NOT NULL,
    c_dec32      DECIMAL(9, 2),
    c_dec128     DECIMAL(38, 10),
    c_dt0        DATETIME,
    c_dt3        DATETIME(3),
    c_ipv4       IPV4,
    c_ipv6       IPV6,
    c_datev1     DATEV1,
    c_datetimev1 DATETIMEV1,
    c_decv2      DECIMALV2(27, 9)
) DUPLICATE KEY(k)
DISTRIBUTED BY HASH(k) BUCKETS 1
PROPERTIES ("replication_num" = "1", "disable_auto_compaction" = "true");

INSERT INTO scalar_more VALUES
  (1,  1234567.89,  1234567890123456789012345678.0123456789,  '2024-02-29 12:34:56', '2024-02-29 12:34:56.789', '192.168.0.1',     '::ffff:10.0.0.1',      '2024-02-29', '2024-02-29 12:34:56', 123456789012345678.123456789),
  (2, -9999999.99, -0.0000000001,                             '1970-01-01 00:00:00', '1970-01-01 00:00:00.001', '0.0.0.0',         '::',                   '0001-01-01', '0001-01-01 00:00:00', -0.000000001),
  (3,  NULL,        NULL,                                     NULL,                  NULL,                      '255.255.255.255', '2001:db8::1',          NULL,         NULL,                  NULL),
  (4,  0,           0,                                        '9999-12-31 23:59:59', '9999-12-31 23:59:59.999', '10.0.0.1',        'fe80:0:0:0:1:0:0:1',   '9999-12-31', '9999-12-31 23:59:59', 999999999999999999.999999999),
  (5,  9999999.99,  9999999999999999999999999999.9999999999,  '2000-01-01 00:00:01', '2000-01-01 00:00:00.5',   '127.0.0.1',       '::1',                  '1900-01-01', '1900-01-01 01:01:01', -999999999999999999.999999999),
  (6,  NULL,        NULL,                                     NULL,                  NULL,                      NULL,              NULL,                   NULL,         NULL,                  NULL),
  (7, -0.01,        0.0000000001,                             '2024-12-31 23:59:59', '2024-12-31 23:59:59.000', '1.2.3.4',         '2001:db8:0:0:1:0:0:1', '2024-12-31', '2024-12-31 23:59:59', 1.5),
  (8,  0.01,       -1,                                        '2024-06-15 06:07:08', '2024-06-15 06:07:08.010', '172.16.254.1',    '::10.0.0.1',           '2024-06-15', '2024-06-15 06:07:08', -1);

INSERT INTO scalar_more
SELECT 1000 + number,
       CAST(number / 3 AS DECIMAL(9, 2)),
       CAST(number AS DECIMAL(38, 10)) / 7,
       SECONDS_ADD(CAST('2024-01-01 00:00:00' AS DATETIME), number * 37),
       MILLISECONDS_ADD(CAST('2024-01-01 00:00:00.000' AS DATETIME(3)), number * 7),
       to_ipv4(CONCAT('10.', number DIV 256 % 256, '.', number % 256, '.', number % 7)),
       to_ipv6(CONCAT('2001:db8::', lower(hex(number)))),
       DATE_ADD(CAST('2000-01-01' AS DATE), INTERVAL number DAY),
       SECONDS_ADD(CAST('2000-01-01 00:00:00' AS DATETIME), number * 3601),
       CAST(number AS DECIMALV2(27, 9)) / 7
FROM numbers("number" = "6000");
