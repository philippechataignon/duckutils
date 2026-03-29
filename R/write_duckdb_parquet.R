#' Writes a parquet file from a duckdb query
#' @param conn duckdb connection, e.g. db$conn
#' @param query duckdb query
#' @param path file name or directory if partition is specified
#' @param compression_level between 1 and 22, default 3
#' @param partition string of the form "var1, var2" to create a partition, no partition by default
#' @param verbose prints the generated SQL query
#' @return Path of the created parquet file
#' @export
write_duckdb_parquet_sql <- function(
  conn,
  query,
  path,
  partition = "",
  compression_level = 3,
  verbose = FALSE
) {
  cmd = glue::glue(
    "COPY ({query}) TO '{path}'
    (FORMAT 'parquet', COMPRESSION 'zstd', COMPRESSION_LEVEL {compression_level} {partition})"
  )
  if (verbose) {
    cat(cmd, "\n")
  }
  DBI::dbExecute(conn, cmd)
  path
}

#' Writes a parquet file from a duckdb table
#' @param table duckdb table
#' @param path file name or directory if partition is specified, defaults to the table name
#' @param dir output directory, optionally an S3 path
#' @param compression_level between 1 and 22, default 3
#' @param partition string of the form "var1, var2" to create a partition, no partition by default
#' @param keep If TRUE, returns the input table, otherwise returns the table linked to the created parquet file. Defaults to FALSE.
#' @param verbose prints the generated SQL query
#' @return dplyr table linked to the created parquet file (see 'keep' parameter to retain the input table)
#' @export

write_duckdb_parquet <- function(
  table,
  path = NULL,
  dir = NULL,
  compression_level = 3,
  partition = NULL,
  keep = FALSE,
  verbose = FALSE
) {
  if (!is.null(dir)) {
    if (is.null(path)) {
      if (is.null(partition)) {
        path = file.path(dir, paste0(deparse(substitute(table)), ".parquet"))
      } else {
        path = file.path(dir, deparse(substitute(table)))
      }
    } else {
      path = file.path(dir, path)
    }
  }
  if (is.null(partition)) {
    partition_str = ""
  } else {
    partition_str = glue::glue(
      ", PARTITION_BY ({partition}), OVERWRITE_OR_IGNORE"
    )
  }
  write_duckdb_parquet_sql(
    conn = table$src$con,
    query = dbplyr::sql_render(table),
    path = path,
    compression_level = compression_level,
    partition = partition_str,
    verbose = verbose
  )
  if (keep)
    table
  else
    tbl_pqt(table$src$con, path)
}

#' Writes a parquet file from a data frame
#' @param df data.frame
#' @param conn duckdb connection, e.g. db$conn
#' @param path file name, defaults to the table name with .parquet extension
#' @param dir output directory, optionally an S3 path, defaults to current directory
#' @param keep If TRUE, returns the input data.frame, otherwise returns the table linked to the created parquet file. Defaults to FALSE.
#' @return dplyr table linked to the created parquet file (see 'keep' parameter to retain the input data.frame)
#' @export
write_df_parquet <- function (df, conn, path, dir = NULL, keep = FALSE)
{
  name = tempname()
  if (!is.null(dir))
    path = file.path(dir, path)
  duckdb::duckdb_register(conn, name, df, overwrite = TRUE)
  outfile = write_duckdb_parquet_sql(conn, paste("FROM", name), path)
  duckdb::duckdb_unregister(conn, name)
  if (keep)
    df
  else
    tbl_pqt(conn, outfile)
}
