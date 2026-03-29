#' Attaches a Postgres database to duckdb
#' @param conn duckdb connection, can be obtained via the get_conn function
#' @param param list of parameters
#' @param db alias name for the database
#' @param schema Postgres schema name, defaults to 'public'
#' @param password_env Name of the environment variable holding the Postgres
#'   password, defaults to 'PASSWORDINSEE'
#' @return dbExecute return code
#' @export
pg_attach <- function(conn, param, db = "pg", schema = "public",
                      password_env = "PASSWORDINSEE") {
  conn_str = glue::glue("
    host={param$host} port={param$port} user={param$user}
    password={Sys.getenv(password_env)} dbname={param$dbname}
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
