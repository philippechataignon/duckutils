# duckutils

Utility functions for [DuckDB](https://duckdb.org/) in R, built on top of [DBI](https://dbi.r-dbi.org/), [dplyr](https://dplyr.tidyverse.org/), and [dbplyr](https://dbplyr.tidyverse.org/).

## Installation

```r
# Install from GitHub
# install.packages("pak")
pak::pkg_install("philippechataignon/duckutils")
```

## Features

- **Connections** – Simple helpers to open and manage DuckDB connections, including an R6 singleton class (`duckdb_conn`) that lazily creates and reconnects as needed.
- **Parquet tables** – Read local and S3 parquet files/directories (with Hive partitioning) directly as dplyr tables.
- **CSV tables** – Read CSV files as dplyr tables.
- **Write parquet** – Write dplyr tables or data frames to parquet files (local or S3).
- **Views & cubes** – Create DuckDB SQL views and CUBE aggregations from dplyr tables.
- **Attach databases** – Attach other DuckDB databases or PostgreSQL databases to an existing connection.
- **Type helpers** – `as.int`, `as.bool`, `as_int()`, `as_bool()` wrappers for `TRY_CAST` inside `dplyr::mutate()`.

## Quick Start

```r
library(duckutils)

# --- Simple in-memory connection ---
conn <- get_conn()

# --- Singleton connection (auto-reconnects) ---
db <- duckdb_conn$new()
conn <- db$conn   # opens connection lazily

# --- Read parquet files ---
tbl <- tbl_pqt(conn, "s3://my-bucket/data/")
tbl <- tbl_pqt(conn, "s3://my-bucket/data/", level = 1)  # one level of partitioning

# --- Read CSV files ---
tbl <- tbl_csv(conn, "data/file.csv", delim = ";")

# --- Write a dplyr table to parquet ---
tbl |>
  dplyr::filter(year == 2023) |>
  write_duckdb_parquet(path = "output.parquet")

# --- Write a data frame to parquet ---
write_df_parquet(mtcars, conn, path = "mtcars.parquet")

# --- Attach another DuckDB file ---
attach_db(conn, "path/to/other.duckdb", db = "other")
tables <- tbl_db(conn, "other")

# --- Attach PostgreSQL ---
pg_attach(conn,
  param = list(host = "localhost", port = 5432,
               user = "me", dbname = "mydb"),
  password_env = "MY_PG_PASSWORD"
)

# --- Type cast helpers inside mutate ---
tbl |>
  dplyr::mutate(dplyr::across(dplyr::starts_with("N"), ~ !!as.int))

db$disconnect()
```

## Main Functions

| Function | Description |
|---|---|
| `get_conn()` | Open a new DuckDB connection |
| `duckdb_conn` | R6 class: singleton connection with auto-reconnect |
| `tbl_pqt()` | Read parquet file/directory as a dplyr table |
| `tbl_csv()` | Read CSV file as a dplyr table |
| `tbl_list()` | Read a list of parquet paths as dplyr tables |
| `tbl_db()` | List tables from an attached DuckDB database |
| `tbl_duckdb()` | Attach a DuckDB file and return its tables |
| `attach_db()` | Attach a DuckDB database file |
| `write_duckdb_parquet()` | Write a dplyr table to parquet |
| `write_duckdb_parquet_sql()` | Write parquet from a SQL query string |
| `write_df_parquet()` | Write a data frame to parquet |
| `create_view()` | Create a DuckDB SQL view from a dplyr table |
| `create_cube()` | Create a CUBE aggregation query |
| `pg_attach()` | Attach a PostgreSQL database |
| `as.int`, `as.bool` | TRY_CAST helpers for use in `mutate(across(...))` |
| `as_int()`, `as_bool()` | TRY_CAST helpers for named columns |

## License

GPL-2
