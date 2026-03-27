#' Attaches a Postgres database to duckdb
#' @param conn duckdb connection, can be obtained via the get_conn function
#' @param param list of parameters
#' @param db alias name for the database
#' @param schema Postgres schema name, defaults to 'public'
#' @return dbExecute return code
#' @export
pg_attach <- function(conn, param, db = "pg", schema = "public") {
  conn_str = glue::glue("
    host={param$host} port={param$port} user={param$user}
    password={Sys.getenv('PASSWORDINSEE')} dbname={param$dbname}
  ")
  DBI::dbExecute(conn, paste("DETACH DATABASE IF EXISTS", db))
  DBI::dbExecute(
    conn,
    glue::glue("
      INSTALL postgres;
      LOAD postgres;
      ATTACH '{conn_str}' AS {db} (TYPE postgres, SCHEMA '{schema}', READ_ONLY);
    ")
  )
}
