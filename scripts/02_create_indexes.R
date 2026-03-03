library(DBI)
library(duckdb)

con <- dbConnect(
  duckdb::duckdb(),
  dbdir = "../data/db/shots.duckdb"
)

dbExecute(con, "
  CREATE INDEX idx_player_season
  ON shots(athlete_id_1, season)
")

dbExecute(con, "
  CREATE INDEX idx_team_season
  ON shots(team_id, season)
")

dbExecute(con, "
  CREATE INDEX idx_location
  ON shots(loc_x, loc_y)
")

dbDisconnect(con, shutdown = TRUE)
