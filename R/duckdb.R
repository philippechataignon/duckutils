extlist = c("none", "core", "geo", "stat", "all")

#' Returns a duckdb connection
#' @param dbdir Path to the duckdb file, default is an in-memory database
#' @param ext Specifies loaded extensions. 'core': spatial, 'geo': h3 and spatial,
#' 'none': no extension. Defaults to 'none'
#' @param macro If TRUE (default), standard macros from the add_macros function are loaded
#' @param new Deprecated, not used, present for backward compatibility
#' with older programs
#' @export
get_conn <- function(dbdir = ":memory:", ext = "none", macro=TRUE, new = NULL, ...)
{
  if (!missing(new)) {
    warning("The 'new' parameter is provided but not used. ",
            "It can be removed: ",
            "get_conn returns a new connection on each call")
  }
  ext = match.arg(ext, extlist)
  conn = DBI::dbConnect(
    duckdb::duckdb(),
    dbdir = dbdir,
    ...
  )
  if (ext %in% c("geo")) {
    DBI::dbExecute(
      conn, "
      LOAD spatial;
    ")
  }
  invisible(conn)
}

#' Returns a duckdb table from a parquet file including S3
#' @param conn duckdb connection
#' @param path Path to the parquet table/directory
#' @param level Number of levels for partitioned parquet files, default 0 if not partitioned
#' @param lower If TRUE, variable names are converted to lowercase
#' @return List of duckdb tables
#' @export
tbl_pqt <- function(conn, path, level = 0, lower = FALSE, verbose = FALSE) {
  # if paths end with /, append *.parquet
  if (
    level > 0 ||
      (level == 0 && all(substr(path, nchar(path), nchar(path)) == "/"))
  ) {
    niv <- paste0(rep('/*', level + 1), collapse = "")
    path <- paste0(path, niv)
  }
  cmd <- paste0(
    "read_parquet([",
    paste(paste0("'", path, "'"), collapse = ","),
    "], hive_partitioning = true, hive_types_autocast = 0)"
  )
  if (verbose) {
    cat(cmd, "\n")
  }
  table <- dplyr::tbl(conn, cmd)
  if (lower) {
    table <- dplyr::rename_with(table, tolower)
  }
  table
}
#' Returns a duckdb table from a CSV file including S3
#' @param conn duckdb connection
#' @param path Path to the CSV file
#' @param ...  Options to pass to the duckdb read_csv function
#' @param lower If TRUE, variable names are converted to lowercase
#' @return duckdb table
#' @export
tbl_csv <- function(conn, path, ..., lower = FALSE, verbose = FALSE) {
  opt = list(...)
  if (length(opt) == 0)
    cmd <- paste0("read_csv('", path, "')")
  else {
    opt = lapply(opt, \(x) {if(is.character(x)) paste0("'", x, "'") else x})
    opt = lapply(names(opt), \(n) paste0(n, " = ", opt[[n]]))
    opt = paste(opt, collapse = ",")
    cmd <- paste0("read_csv('", path, "', ", opt, ")")
  }
  if (verbose) {
    cat(cmd, "\n")
  }
  table <- dplyr::tbl(conn, cmd)
  if (lower) {
    table <- dplyr::rename_with(table, tolower)
  }
  table
}

#' Returns a list of duckdb tables from a list
#' of paths to parquet files/directories
#' @param conn duckdb connection
#' @param paths List of paths to 'parquet' files
#' @return liste List of duckdb tables
#' @export
tbl_list <- function(conn, paths, level = 0, lower = FALSE, verbose = FALSE) {
  lapply(
    paths,
    function(x) {
      tbl_pqt(conn, x, level, lower, verbose)
    }
  )
}

#' Attaches a duckdb database to a connection
#' @param conn duckdb connection, can be obtained via the get_conn function
#' @param dbdir Path to the duckdb file
#' @param db Alias name for the database
#' @param crypt If TRUE, passes the value of DUCKDB_ENCRYPTION_KEY as the key for the encrypted database
#' @return Name of the duckdb database
#' @export
attach_db <- function(conn, dbdir, db, crypt=FALSE) {
  DBI::dbExecute(conn, paste("DETACH DATABASE IF EXISTS", db))
  cmd = paste0("ATTACH '", dbdir, "' as ", db)
  if (crypt) {
    cmd = paste0(
      cmd,
      "(encryption_key '",
      Sys.getenv("DUCKDB_ENCRYPTION_KEY"),
      "')"
    )
  }
  DBI::dbExecute(conn, cmd)
  invisible(db)
}

#' Returns a list of tables from a duckdb database
#' @param conn duckdb connection, can be obtained via the get_conn function
#' @param db Name of the duckdb database
#' @return List of tables
#' @export
tbl_db <- function(conn, db, verbose=FALSE) {
  tables = DBI::dbGetQuery(conn, paste("SHOW TABLES FROM", db))$name
  if (verbose) {
    print(tables)
  }
  ret = lapply(tables, function(x) dplyr::tbl(conn, paste0(db, ".", x)))
  names(ret) = tables
  ret
}

#' Returns a list of tables from a duckdb database
#' @param conn duckdb connection, can be obtained via the get_conn function
#' @param name Name of the duckdb database; ".duckdb" is appended automatically and the database is located in the
#'        (s3perso)/duckdb directory
#' @param db Alias name for the database, defaults to path
#' @param crypt If TRUE, passes the value of DUCKDB_ENCRYPTION_KEY as the key for the encrypted database
#' @return List of tables
#' @export
tbl_duckdb <- function(conn, name, db=NULL, crypt=FALSE) {
  if (is.null(db)) {
    db = name
  }
  if (grepl("crypt", name, fixed=TRUE)) {
    crypt = TRUE
  }
  db = attach_db(conn, dbdir = perso("duckdb", paste0(name, ".duckdb")), db, crypt=crypt)
  tbl_db(conn, db)
}

#' Wrapper try_cast duckdb
#' @examples
#' p90 |>
#'  mutate(
#'    across(
#'      starts_with(c("N")),
#'      ~ !!as.int
#'    )
#'  )
#' @export
as.int =  quote(try_cast(dbplyr::sql(paste0('"', cur_column(), '" as integer'))))

#' Wrapper try_cast duckdb
#' @export
#' @examples
#' p90 |>
#'  mutate(
#'    across(
#'      starts_with(c("I")),
#'      ~ !!as.bool
#'    )
#'  )
as.bool =  quote(try_cast(dbplyr::sql(paste0('"', cur_column(), '" as boolean'))))

#' Wrapper try_cast duckdb
#' @examples
#' p90 |>
#'   transmute(
#'     nb = !!as_int(nb),
#'   )
#' @export
as_int <- function(x) {
  nom = deparse(substitute(x))
  inner = paste0('"', nom, '" as integer')
  substitute(try_cast(dbplyr::sql(inner)))
}

#' Wrapper try_cast duckdb
#' @examples
#' p90 |>
#'   transmute(
#'     INP75M = !!as_bool(is_present)
#'   )
#' @export
as_bool <- function(x) {
  nom = deparse(substitute(x))
  inner = paste0('"', nom, '" as boolean')
  substitute(try_cast(dbplyr::sql(inner)))
}

#' Generates a SQL query to create a duckdb cube
#' @param table duckdb/dbplyr table
#' @param dims  Character vector of dimensions
#' @param aggr  Description of cube measure calculations as a character string
#' @examples
#' table |>
#'  compute() |>
#'  create_cube(
#'    dims = c("dim1", "dim2"),
#'    aggr =
#'    "round(sum(n))::integer as eff,
#'     round(sum(n * is_present::int))::integer as eff_present"
#'  ) |>
#'  mutate(
#'    tx_present = round(eff_present / eff * 100, 2)
#'  ) -> tbl_cube
#' @export
create_cube <- function(table, dims, aggr) {
  temp_name = tempname(prefix = "view")
  temp_view = create_view(table, temp_name)
  chr_dims = paste(dims, collapse="," )
  dplyr::tbl(
    table$src$con,
    dbplyr::sql(glue::glue("
    select
      grouping_id({chr_dims}) as grouping,
      {chr_dims},
      {aggr}
    from {temp_name}
    group by cube ({chr_dims})
    order by grouping, ({chr_dims})
    "))
  )
}

#' Generates a SQL view from a dbplyr table
#' @param table duckdb/dbplyr table
#' @param view_name Name of the view; if NULL, a random name is generated
#' @return Table corresponding to the view
#' @export
create_view <- function(table, view_name=NULL) {
  if (is.null(view_name)) {
    view_name = tempname(prefix = "view")
  }
  DBI::dbExecute(
    table$src$con,
    glue::glue("DROP VIEW IF EXISTS {`view_name`}")
  )
  DBI::dbExecute(
    table$src$con,
    glue::glue(
    "CREATE VIEW {`view_name`} AS\n",
    "{dbplyr::sql_render(table)}\n"
    )
  )
  dplyr::tbl(table$src$con, view_name)
}
