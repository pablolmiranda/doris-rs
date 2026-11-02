-- Golden corpus v1: a table whose only load carries zero rows. Whether the reference writes an
-- empty segment for it is itself a fact the corpus records.
CREATE TABLE empty_table (
    k INT NOT NULL,
    v VARCHAR(10)
) DUPLICATE KEY(k)
DISTRIBUTED BY HASH(k) BUCKETS 1
PROPERTIES ("replication_num" = "1");

INSERT INTO empty_table SELECT * FROM empty_table;
