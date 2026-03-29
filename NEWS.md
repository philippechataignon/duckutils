# duckutils 0.0.2 (development)

## Bug Fixes

* Added missing **R6** dependency to `DESCRIPTION` Imports.
* Added missing helper functions `tempname()` and `perso()` in `R/utils.R`.
* Fixed `dbGetQuery` → `DBI::dbGetQuery` in `tbl_db()`.
* Added missing `return(ret)` in `tbl_db()`.
* Fixed `tbl()` → `dplyr::tbl()` in `create_cube()` and `create_view()`.
* Fixed `sql_render()` → `dbplyr::sql_render()` in `write_duckdb_parquet()`.
* Fixed `duckdb_register()` / `duckdb_unregister()` → `duckdb::duckdb_register()` /
  `duckdb::duckdb_unregister()` in `write_df_parquet()`.
* Fixed parameter documentation mismatch (`liste` → `paths`) in `tbl_list()`.

## Improvements

* `pg_attach()` now accepts a `password_env` parameter (default `"PASSWORDINSEE"`)
  so users can specify their own password environment variable.
* `get_conn()`: `macro` parameter documentation updated to reflect it is not yet
  implemented; passing `macro = FALSE` now emits an informative warning.
* Added input validation with informative error messages to `tbl_pqt()`,
  `tbl_csv()`, `tbl_list()`, `write_duckdb_parquet()`, and `write_df_parquet()`.
* Enhanced `README.md` with installation instructions, quick-start examples, and a
  full function reference table.
* Added `NEWS.md` for version tracking.
* Added GitHub Actions workflow for automated `R CMD check` on multiple platforms.

# duckutils 0.0.1

* Initial release.
