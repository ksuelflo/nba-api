library(plumber)

pr <- plumb("api/api_plumber.R")
pr$run(host = "0.0.0.0", port = 8000)