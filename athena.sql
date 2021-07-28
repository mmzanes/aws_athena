CREATE EXTERNAL TABLE IF NOT EXISTS sampledb.prices (
  `date` date,
  `symbol` string,
  `open` decimal(14,8),
  `close` decimal(14,8),
  `low` decimal(14,8),
  `high` decimal(14,8),
  `volume` decimal(14,8)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://mmzanes-athena/prices/'
TBLPROPERTIES ('has_encrypted_data'='false');
