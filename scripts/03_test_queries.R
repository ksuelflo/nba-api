library(DBI)
library(duckdb)

con <- dbConnect(duckdb(), "../data/db/shots.duckdb")

dbGetQuery(con, "
  SELECT name_2, COUNT(*) AS assists
  FROM shots
  WHERE season = 2016 and season_type = 3 and assisted = TRUE
  GROUP BY name_2
  ORDER BY assists DESC
  LIMIT 5
")

dbDisconnect(con, shutdown = TRUE)
