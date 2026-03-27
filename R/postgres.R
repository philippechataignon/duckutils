#' Attache une base Postgres à duckdb
#' @param conn connexion duckdb, peut être obtenu par la fonction get_conn
#' @param param liste de paramètres
#' @param db nom de l'alias de la base
#' @param schema nom du schema Postgres, 'public' par défaut
#' @return Code retour dbExecute
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
