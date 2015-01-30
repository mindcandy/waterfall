CREATE TABLE testing.waterfall_cassandra_spec_decimal (
  date timestamp,
  currency text,
  in_gbp decimal,
  in_usd decimal,
  PRIMARY KEY (date, currency)
) WITH DEFAULT_TIME_TO_LIVE = 300;