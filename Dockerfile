FROM rstudio/plumber

WORKDIR /app

COPY . /app

RUN R -e "install.packages(c('duckdb','dplyr','DBI','jsonlite'))"

RUN mkdir -p data/db

RUN curl -L https://nba-api-data-2026.s3.us-east-2.amazonaws.com/shots.duckdb -o data/db/shots.duckdb
RUN curl -L https://nba-api-data-2026.s3.us-east-2.amazonaws.com/player.duckdb -o data/db/player.duckdb

EXPOSE 8000

CMD ["api/api_plumber.R"]