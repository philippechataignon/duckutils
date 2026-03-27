#' Écrit un fichier parquet depuis une requête duckdb
#' @param conn Connexion duckdb, par exemple db$conn
#' @param query Requête duckdb
#' @param path nom du fichier ou répertoire si partition est renseigné
#' @param compression_level, entre 1 et 22, 3 par défaut
#' @param partition chaine de la forme "var1, var2" pour créer une partition, pas de partition par défaut
#' @param verbose indique la requete SQL générée
#' @return Chemin du fichier parquet créé
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

#' Écrit un fichier parquet depuis une table duckdb
#' @param table table duckdb
#' @param path nom du fichier ou répertoire si partition est renseigné, par défaut le nom de la table
#' @param dir répertoire de sortie, éventuellement s3
#' @param compression_level, entre 1 et 22, 3 par défaut
#' @param partition chaine de la forme "var1, var2" pour créer une partition, pas de partition par défaut
#' @param keep Si TRUE, renvoit la table fournie en entrée sinon la table liée au fichier parquet créé. FALSE par défaut.
#' @param verbose indique la requete SQL générée
#' @return table dplyr liée au fichier parquet créé (voir paramètre 'keep' pour conserver la table en entrée)
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
    query = sql_render(table),
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

#' Écrit fichier parquet depuis une table duckdb
#' @param df data.frame
#' @param conn Connexion duckdb, par exemple db$conn
#' @param path Nom du fichier, par défaut le nom de la table.parquet
#' @param dir Répertoire de sortie, éventuellement s3, par défaut répertoire courant
#' @param keep Si TRUE, renvoit le data.frame fourni en entrée sinon la table liée au fichier parquet créé. FALSE par défaut.
#' @return table dplyr liée au fichier parquet créé (voir paramètre 'keep' pour conserver le data.frame en entrée)
#' @export
write_df_parquet <- function (df, conn, path, dir = NULL, keep = FALSE)
{
  name = tempname()
  if (!is.null(dir))
    path = file.path(dir, path)
  duckdb_register(conn, name, df, overwrite = TRUE)
  outfile = write_duckdb_parquet_sql(conn, paste("FROM", name), path)
  duckdb_unregister(conn, name)
  if (keep)
    df
  else
    tbl_pqt(conn, outfile)
}
