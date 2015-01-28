CREATE TABLE testing.waterfall_cassandra_spec (
  userID TEXT,
  joined TIMESTAMP,
  age INT,
  name TEXT,
  PRIMARY KEY (userID, joined)
) WITH DEFAULT_TIME_TO_LIVE = 300;