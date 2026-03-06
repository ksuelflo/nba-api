
#* @apiTitle NBA Shot Chart API
#* @apiDescription Query NBA shot data



# ── Precompute KDE vectors at API startup ──────────────────────────────────────

# Null coalescing operator
`%||%` <- function(a, b) if (!is.null(a) && !is.na(a) && a != "") a else b
library(dplyr)

# ── Precompute zone frequency vectors at API startup ──────────────────────────

ZONES <- c(
  "Restricted Area",
  "In the Paint (Non-RA)",
  "Left Mid-Range",
  "Right Mid-Range",
  "Left Wing Mid-Range",
  "Right Wing Mid-Range",
  "Center Mid-Range",
  "Left Corner 3",
  "Right Corner 3",
  "Left Wing 3",
  "Right Wing 3",
  "Above the Break 3"
)

compute_zone_vector <- function(shots_df) {
  if (nrow(shots_df) < 20) return(NULL)
  
  counts <- table(factor(shots_df$region, levels = ZONES))
  vec    <- as.numeric(counts)
  
  total <- sum(vec)
  if (total == 0) return(NULL)
  
  # Convert to shot frequency distribution
  vec / total
}

cosine_similarity <- function(a, b) {
  denom <- sqrt(sum(a^2)) * sqrt(sum(b^2))
  if (denom == 0) return(0)
  sum(a * b) / denom
}

build_zone_cache <- function(con, min_attempts = 200) {

  message("Building zone frequency cache...")

  # Aggregate in SQL — avoids loading millions of raw rows into R
  season_sql <- "
    WITH qualifying AS (
      SELECT name, season
      FROM shots
      WHERE region != 'Backcourt'
      GROUP BY name, season
      HAVING COUNT(*) >= ?
    )
    SELECT s.name, s.season, s.region, COUNT(*) AS zone_count
    FROM shots s
    INNER JOIN qualifying q ON s.name = q.name AND s.season = q.season
    WHERE s.region != 'Backcourt'
    GROUP BY s.name, s.season, s.region
    ORDER BY s.name, s.season, s.region
  "
  season_agg <- dbGetQuery(con, season_sql, params = list(min_attempts))

  season_cache <- list()
  ps <- unique(season_agg[, c("name", "season")])
  for (i in seq_len(nrow(ps))) {
    player <- ps$name[i]
    szn    <- ps$season[i]
    grp    <- season_agg[season_agg$name == player & season_agg$season == szn, ]
    counts <- setNames(grp$zone_count, grp$region)
    vec    <- as.numeric(counts[ZONES])
    vec[is.na(vec)] <- 0
    total  <- sum(vec)
    if (total >= 20) {
      key <- paste0(player, "||", szn)
      season_cache[[key]] <- list(name = player, season = szn, vector = vec / total)
    }
  }

  career_sql <- "
    WITH qualifying AS (
      SELECT name
      FROM shots
      WHERE region != 'Backcourt'
      GROUP BY name
      HAVING COUNT(*) >= ?
    )
    SELECT s.name, s.region, COUNT(*) AS zone_count
    FROM shots s
    INNER JOIN qualifying q ON s.name = q.name
    WHERE s.region != 'Backcourt'
    GROUP BY s.name, s.region
    ORDER BY s.name, s.region
  "
  career_agg <- dbGetQuery(con, career_sql, params = list(min_attempts))

  career_cache <- list()
  for (player in unique(career_agg$name)) {
    grp    <- career_agg[career_agg$name == player, ]
    counts <- setNames(grp$zone_count, grp$region)
    vec    <- as.numeric(counts[ZONES])
    vec[is.na(vec)] <- 0
    total  <- sum(vec)
    if (total >= 20) {
      career_cache[[player]] <- list(name = player, vector = vec / total)
    }
  }

  message(sprintf(
    "Zone cache built: %d player-seasons, %d career profiles",
    length(season_cache), length(career_cache)
  ))

  list(season = season_cache, career = career_cache)
}

# ── Similarity endpoint ────────────────────────────────────────────────────────

#* Get the 5 most similar players based on shooting location
#* @param player_name:str
#* @param season:str
#* @param n_results:int
#* @get /player/similar
function(player_name, season = "All", n_results = 5) {
  
  n_results <- as.integer(n_results)
  
  # ── 1. Get the query player's vector ──
  if (season == "All") {
    entry <- kde_cache$career[[player_name]]
  } else {
    key   <- paste0(player_name, "||", season)
    entry <- kde_cache$season[[key]]
  }
  
  if (is.null(entry)) {
    return(list(error = paste("No zone vector found for", player_name,
                              "in season", season,
                              "(player may not meet minimum attempt threshold)")))
  }
  
  query_vec <- entry$vector
  
  # ── 2. Compute similarity against the right pool ──
  if (season == "All") {
    pool <- kde_cache$career
    pool <- pool[names(pool) != player_name]
  } else {
    pool <- kde_cache$season
    pool <- pool[sapply(pool, function(p) p$season == season && p$name != player_name)]
  }
  
  if (length(pool) == 0) {
    return(list(error = "No comparable players found for this season"))
  }
  
  scores       <- sapply(pool, function(p) cosine_similarity(query_vec, p$vector))
  names(scores) <- sapply(pool, function(p) p$name)
  
  # ── 3. Get top N ──
  top_idx    <- order(scores, decreasing = TRUE)[1:min(n_results, length(scores))]
  top_names  <- names(scores)[top_idx]
  top_scores <- scores[top_idx]
  
  # ── 4. Fetch player images ──
  all_names    <- c(player_name, top_names)
  placeholders <- paste(rep("?", length(all_names)), collapse = ", ")
  
  profile_sql <- sprintf("
    SELECT name, MAX(image) as image
    FROM player
    WHERE name IN (%s)
    GROUP BY name
  ", placeholders)
  
  profiles     <- dbGetQuery(conPlayer, profile_sql, params = as.list(all_names))
  image_lookup <- setNames(
    ifelse(is.na(profiles$image), "", profiles$image),
    profiles$name
  )
  
  # ── 5. Build response ──
  similar <- lapply(seq_along(top_names), function(i) {
    list(
      name       = top_names[i],
      similarity = round(top_scores[i], 4),
      image      = image_lookup[[top_names[i]]] %||% ""
    )
  })
  
  list(
    player = list(
      name  = player_name,
      image = image_lookup[[player_name]] %||% ""
    ),
    similar = similar
  )
}

#* Player average shot distance by season
#* @param player_name:str
#* @get /player/avg-distance
function(player_name = "All") {
  
  sql <- "
    SELECT
      season,
      AVG(SQRT(LOC_X * LOC_X + LOC_Y * LOC_Y)) AS avg_distance
    FROM shots
    WHERE (? = 'All' OR name = ?)
      AND region != 'Backcourt'
    GROUP BY season
    ORDER BY season
  "
  
  dbGetQuery(
    con,
    sql,
    params = list(player_name, player_name)
  )
}

#* Health check
#* @get /health
function() {
  list(status = "ok")
}

#* Player assist percentage on made shots by season
#* @param player_name:str
#* @get /player/assist-pct
function(player_name = "All") {
  
  sql <- "
    SELECT
      season,
      COUNT(*) AS total_makes,
      SUM(assisted::INT) AS assisted_makes,
      AVG(assisted::INT) AS assist_pct
    FROM shots
    WHERE scoring_play = TRUE
      AND (? = 'All' OR name = ?)
    GROUP BY season
    ORDER BY season
  "
  
  dbGetQuery(
    con,
    sql,
    params = list(player_name, player_name)
  )
}

#* Player points per shot with percentile
#* @param player_name:str
#* @param season:str
#* @param period:str
#* @get /player/points-per-shot
function(player_name, season = "All", period = "All") {
  
  sql <- "
    WITH player_stats AS (
      SELECT
        name,
        SUM(score_value) AS total_points,
        COUNT(*) AS attempts,
        SUM(score_value)::FLOAT / COUNT(*) AS points_per_shot
      FROM shots
      WHERE (? = 'All' OR season = ?)
        AND (? = 'All' OR period_number = ?)
      GROUP BY name
      HAVING COUNT(*) >= 50
    ),
    with_percentile AS (
      SELECT
        name,
        points_per_shot,
        attempts,
        PERCENT_RANK() OVER (ORDER BY points_per_shot) AS percentile
      FROM player_stats
    )
    SELECT
      points_per_shot,
      attempts,
      percentile
    FROM with_percentile
    WHERE name = ?
  "
  
  result <- dbGetQuery(
    con,
    sql,
    params = list(
      season, season,
      period, period,
      player_name
    )
  )
  
  if (nrow(result) == 0) {
    return(list(
      points_per_shot = NA,
      attempts        = 0,
      percentile      = NA,
      error           = "Player not found or insufficient attempts"
    ))
  }
  
  list(
    points_per_shot = round(result$points_per_shot, 3),
    attempts        = result$attempts,
    percentile      = round(result$percentile, 3)
  )
}


#* Player total attempts with percentile
#* @param player_name:str
#* @param season:str
#* @param period:str
#* @get /player/total-attempts
function(player_name, season = "All", period = "All") {
  
  sql <- "
    WITH player_stats AS (
      SELECT
        name,
        COUNT(*) AS attempts
      FROM shots
      WHERE (? = 'All' OR season = ?)
        AND (? = 'All' OR period_number = ?)
      GROUP BY name
      HAVING COUNT(*) >= 50
    ),
    with_percentile AS (
      SELECT
        name,
        attempts,
        PERCENT_RANK() OVER (ORDER BY attempts) AS percentile
      FROM player_stats
    )
    SELECT
      attempts,
      percentile
    FROM with_percentile
    WHERE name = ?
  "
  
  result <- dbGetQuery(
    con,
    sql,
    params = list(
      season, season,
      period, period,
      player_name
    )
  )
  
  if (nrow(result) == 0) {
    return(list(
      attempts   = NA,
      percentile = NA,
      error      = "Player not found or insufficient attempts"
    ))
  }
  
  list(
    attempts   = result$attempts,
    percentile = round(result$percentile, 3)
  )
}

# -------------------------
# Binned shot data endpoint
# -------------------------

#* Filter universe
#* @param season
#* @param player
#* @param team
#* @param quarter
#* @get /filters/universe
function(season = "All", player = "All", team = "All", quarter = "All") {
  
  sql <- "
    SELECT
      ARRAY_AGG(DISTINCT season ORDER BY season DESC) AS seasons,
      ARRAY_AGG(DISTINCT name ORDER BY name) AS players,
      ARRAY_AGG(DISTINCT team_name ORDER BY team_name) AS teams,
      ARRAY_AGG(DISTINCT period_number ORDER BY period_number) AS quarters
    FROM shots
    WHERE
      (season = ? OR ? = 'All')
      AND (name = ? OR ? = 'All')
      AND (team_name = ? OR ? = 'All')
      AND (period_number = ? OR ? = 'All')
  "
  
  res <- dbGetQuery(
    con,
    sql,
    params = list(
      season, season,
      player, player,
      team, team,
      quarter, quarter
    )
  )
  
  # Convert single-row arrays → plain lists
  list(
    seasons  = res$seasons[[1]],
    players  = res$players[[1]],
    teams    = res$teams[[1]],
    quarters = res$quarters[[1]]
  )
}

#* League average FG% by region
#* @param season:str
#* @get /league/regions
function(season = "All") {
  
  sql <- "
  SELECT
    region,
    COUNT(*) AS attempts,
    SUM(scoring_play::INT) AS makes,
    AVG(scoring_play::INT) AS fg_pct
  FROM shots
  WHERE (? = 'All' OR season = ?)
  GROUP BY region
  ORDER BY region
"
  
  
  dbGetQuery(con, sql, params = list(season, season))
}

#----------------------------------------------

#* Position average FG% by region
#* @param position:str
#* @param season:str
#* @get /position/regions
function(position = "All", season = "All") {

  sql <- "
    SELECT
      region,
      COUNT(*) AS attempts,
      SUM(scoring_play::INT) AS makes,
      AVG(scoring_play::INT) AS fg_pct
    FROM shots
    WHERE (? = 'All' OR position = ?)
      AND (? = 'All' OR season = ?)
    GROUP BY region
    ORDER BY region
  "

  dbGetQuery(con, sql, params = list(position, position, season, season))
}

#----------------------------------------------

#* Team average FG% by region
#* @param team_name:str
#* @param season:str
#* @param period:str
#* @get /team/regions
function(team_name = "All", season = "All", period = "All") {
  
  sql <- "
    SELECT
      region,
      COUNT(*) AS attempts,
      SUM(scoring_play::INT) AS makes,
      AVG(scoring_play::INT) AS fg_pct
    FROM shots
    WHERE (? = 'All' OR season = ?)
      AND (? = 'All' OR team_name = ?)
      AND (? = 'All' OR period_number = ?)
    GROUP BY region
  "
  
  dbGetQuery(
    con,
    sql,
    params = list(
      season, season,
      team_name, team_name,
      period, period
    )
  )
}


#----------------------------------------------

#* Player average FG% by region
#* @param player_name:str
#* @param season:str
#* @param period:str
#* @get /player/regions
function(player_name = "All", season = "All", period = "All") {
  
  sql <- "
    SELECT
      region,
      COUNT(*) AS attempts,
      SUM(scoring_play::INT) AS makes,
      AVG(scoring_play::INT) AS fg_pct
    FROM shots
    WHERE (? = 'All' OR season = ?)
      AND (? = 'All' OR name = ?)
      AND (? = 'All' OR period_number = ?)
    GROUP BY region
  "

  dbGetQuery(
    con,
    sql,
    params = list(
      season, season,
      player_name, player_name,
      period, period
    )
  )
}


#----------------------------------------------

#* Get teams (optionally filtered by player and/or season)
#* @param player
#* @param season:int
#* @get /teams
function(player = "All", season = NULL) {
  
  if (player == "All" && is.null(season)) {
    sql <- "
      SELECT DISTINCT team_name
      FROM shots
      ORDER BY team_name
    "
    return(dbGetQuery(con, sql))
  }
  
  if (player != "All" && is.null(season)) {
    sql <- "
      SELECT DISTINCT team_name
      FROM shots
      WHERE name = ?
      ORDER BY team_name
    "
    return(dbGetQuery(con, sql, params = list(player)))
  }
  
  if (player == "All" && !is.null(season)) {
    sql <- "
      SELECT DISTINCT team_name
      FROM shots
      WHERE season = ?
      ORDER BY team_name
    "
    return(dbGetQuery(con, sql, params = list(season)))
  }
  
  # player + season
  sql <- "
    SELECT DISTINCT team_name
    FROM shots
    WHERE name = ?
      AND season = ?
    ORDER BY team_name
  "
  
  dbGetQuery(con, sql, params = list(player, season))
}


#----------------------------------------------

#* Get players (optionally filtered by team and/or season)
#* @param team
#* @param season:int
#* @get /players
function(team = "All", season = NULL) {
  
  if (team == "All" && is.null(season)) {
    sql <- "
      SELECT DISTINCT name
      FROM shots
      ORDER BY name
    "
    return(dbGetQuery(con, sql))
  }
  
  if (team != "All" && is.null(season)) {
    sql <- "
      SELECT DISTINCT name
      FROM shots
      WHERE team_name = ?
      ORDER BY name
    "
    return(dbGetQuery(con, sql, params = list(team)))
  }
  
  if (team == "All" && !is.null(season)) {
    sql <- "
      SELECT DISTINCT name
      FROM shots
      WHERE season = ?
      ORDER BY name
    "
    return(dbGetQuery(con, sql, params = list(season)))
  }
  
  # team + season
  sql <- "
    SELECT DISTINCT name
    FROM shots
    WHERE team_name = ?
      AND season = ?
    ORDER BY name
  "
  
  dbGetQuery(con, sql, params = list(team, season))
}

#----------------------------------------------

#* Get all seasons by player
#* @param player_name:str
#* @get /player/regions/yearly
function(player_name = "All") {
  
  sql <- "
    SELECT
      season,
      region,
      COUNT(*) AS attempts,
      SUM(scoring_play::INT) AS makes,
      AVG(scoring_play::INT) AS fg_pct
    FROM shots
    WHERE region != 'Backcourt'
      AND (? = 'All' OR name = ?)
    GROUP BY season, region
    ORDER BY season, region
  "
  
  dbGetQuery(
    con,
    sql,
    params = list(
      player_name, player_name
    )
  )
}

#----------------------------------------------

#* Get all seasons by team
#* @param team_name:str
#* @get /team/regions/yearly
function(team_name = "All") {
  
  sql <- "
    SELECT
      season,
      region,
      COUNT(*) AS attempts,
      SUM(scoring_play::INT) AS makes,
      AVG(scoring_play::INT) AS fg_pct
    FROM shots
    WHERE region != 'Backcourt'
      AND (? = 'All' OR team_name = ?)
    GROUP BY season, region
    ORDER BY season, region
  "
  
  dbGetQuery(
    con,
    sql,
    params = list(
      team_name, team_name
    )
  )
}

#----------------------------------------------

#* @param player_name:str
#* @get /player/shot-distribution/yearly
function(player_name = "All") {
  
  sql <- "
    WITH grouped AS (
      SELECT
        season,
        CASE
          WHEN region IN (
            'Restricted Area',
            'In the Paint (Non-RA)'
          ) THEN 'paint'

          WHEN region IN (
            'Right Mid-Range',
            'Left Mid-Range',
            'Center Mid-Range',
            'Left Wing Mid-Range',
            'Right Wing Mid-Range'
          ) THEN 'mid'

          WHEN region IN (
            'Left Corner 3',
            'Right Corner 3',
            'Left Wing 3',
            'Right Wing 3',
            'Above the Break 3'
          ) THEN 'three'
        END AS shot_group,

        COUNT(*) AS attempts

      FROM shots
      WHERE region != 'Backcourt'
        AND (? = 'All' OR name = ?)

      GROUP BY season, shot_group
    )

    SELECT
      season,
      shot_group,
      attempts,
      attempts::FLOAT
        / SUM(attempts) OVER (PARTITION BY season) AS freq
    FROM grouped
    ORDER BY season, shot_group
  "
  
  dbGetQuery(
    con,
    sql,
    params = list(player_name, player_name)
  )
}

#----------------------------------------------

#* Get player profile (name + image)
#* @param player_name:str
#* @get /player/profile
function(player_name) {
  
  sql <- "
    SELECT
      name,
      image,
      position
    FROM shots
    WHERE name = ?
    LIMIT 1
  "
  
  dbGetQuery(
    con,
    sql,
    params = list(
      player_name
    )
  )
}

#----------------------------------------------

#* Player shot attempt density grid (KDE)
#* @param player_name:str
#* @param season:str
#* @param period:str
#* @param bandwidth:num
#* @param resolution:num
#* @get /player/shot-density
function(player_name = "All", season = "All", period = "All", bandwidth = 2.5, resolution = 0.5) {
  
  # Pull raw shot coordinates
  sql <- "
    SELECT LOC_X, LOC_Y
    FROM shots
    WHERE (? = 'All' OR name = ?)
      AND (? = 'All' OR season = ?)
      AND (? = 'All' OR period_number = ?)
      AND LOC_Y <= 47
  "
  shots <- dbGetQuery(
    con, sql,
    params = list(
      player_name, player_name,
      season, season,
      period, period
    )
  )
  
  if (nrow(shots) == 0) return(list())
  
  # Build the grid of cell centers in court coordinates
  # Court is 50 wide (-25 to 25) and we use half-court (0 to 47)
  res <- as.numeric(resolution)
  bw  <- as.numeric(bandwidth)
  
  x_seq <- seq(-25 + res/2, 25 - res/2, by = res)
  y_seq <- seq(0  + res/2, 47 - res/2, by = res)
  
  kde <- MASS::kde2d(
    x    = shots$LOC_X,
    y    = shots$LOC_Y + 4.75,
    h    = bw,
    n    = c(length(x_seq), length(y_seq)),
    lims = c(-25, 25, 0, 47)
  )
  
  grid <- expand.grid(x = kde$x, y = kde$y)
  grid$density <- as.vector(kde$z)
  
  # Normalize to [0, 1] so D3 color scale is easy to work with
  max_d <- max(grid$density)
  if (max_d > 0) grid$density <- grid$density / max_d
  
  # Drop zero-density cells to reduce payload size
  # grid <- grid[grid$density > 0.001, ]
  grid
}

#----------------------------------------------

library(plumber)
library(DBI)
library(duckdb)

# Enable CORS
#* @filter cors
function(req, res){
  res$setHeader("Access-Control-Allow-Origin", "*")
  res$setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
  res$setHeader("Access-Control-Allow-Headers", "Content-Type")
  if (req$REQUEST_METHOD == "OPTIONS") {
    res$status <- 200
    return(list())
  }
  plumber::forward()
}

con <- dbConnect(
  duckdb::duckdb(),
  dbdir = "/app/data/db/shots.duckdb",
  read_only = TRUE
)

conPlayer <- dbConnect(
  duckdb::duckdb(),
  dbdir = "/app/data/db/player.duckdb",
  read_only = TRUE
)

# Build cache when API loads — con must already be defined above this line
kde_cache <- build_zone_cache(con)