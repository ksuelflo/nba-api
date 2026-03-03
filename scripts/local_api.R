library(plumber)
pr <- plumb("../api/plumber.R")
pr$run(port = 8000)

