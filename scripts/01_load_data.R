library(DBI)
library(duckdb)

con <- dbConnect(
  duckdb::duckdb(),
  dbdir = "../data/db/shots.duckdb"
)

dbExecute(con, "
  CREATE TABLE shots AS
  SELECT *
  FROM read_csv_auto(
    '../data/raw/shots_by_region_02_10.csv',
    SAMPLE_SIZE = -1
  )
")

dbDisconnect(con, shutdown = TRUE)

