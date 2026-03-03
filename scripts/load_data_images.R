library(DBI)
library(duckdb)

con <- dbConnect(
  duckdb::duckdb(),
  dbdir = "../data/db/player.duckdb"
)

dbExecute(con, "
  CREATE TABLE player AS
  SELECT *
  FROM read_csv_auto(
    '../data/raw/player_images.csv',
    SAMPLE_SIZE = -1
  )
")

dbDisconnect(con, shutdown = TRUE)

