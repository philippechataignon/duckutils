#' Duckdb connection
#' @description
#' Singleton for duckdb connection
#' @examples
#' # New connection
#' db = duckdb_conn$new()
#' # Show connection
#' print(db)
#' # Never use conn, create new connection
#' DBI::dbExecute(db$conn, "select 42 as value")
#' print(db)
#' # Reuse connection
#' DBI::dbExecute(db$conn, "select 44 as value")
#' print(db)
#' # Disconnect
#' db$disconnect()
#' # Show connection
#' print(db)
#' # After disconnect, create new connection
#' DBI::dbExecute(db$conn, "select 42 as value")
#' print(db)
#' # Disconnect
#' db$disconnect()
#' @export
duckdb_conn <- R6Class(
  "duckdb_conn",
  public = list(
    #' @description
    #' Creates a 'duckdb_conn' object
    #' @param ext Loaded extensions: 'core': httpfs+spatial, 'geo': core+h3,
    #' 'all': geo+ead_stat. Defaults to 'none'
    #' @param dbdir Name of the duckdb database, default is an in-memory database
    #' @param geometry Geometry format name, 'wk' (default) or 'blob'
    #' @param bigint bigint conversion type, 'integer64' (default) or 'numeric'
    #' @return A 'duckdb_conn' object
    initialize = function(
        ext = "none",
        dbdir = ":memory:",
        geometry = c("wk", "blob"),
        bigint = c("integer64", "numeric")
      ){
      stopifnot(is.character(dbdir), length(dbdir) == 1)
      stopifnot(is.character(ext), length(ext) == 1)
      ext <- match.arg(ext, extlist)
      geometry <- match.arg(geometry)
      bigint <- match.arg(bigint)
      private$pext <- ext
      private$pdbdir <- dbdir
      private$pgeometry <- geometry
      private$pbigint <- bigint
    },
    #' @description
    #' Dedicated 'print' function
    #' @param ... Unused
    print = function(...) {
      message("Duckdb connection ", "ext:", private$pext, " dbdir:", private$pdbdir)
      if (self$is_connected())
        print(private$pconn)
      else
        message("No connection")
      invisible(self)
    },
    #' @description
    #' Is the connection valid?
    #' @return TRUE/FALSE
    is_connected = function() {
      if (is.null(private$pconn)) {
        ret = FALSE
      } else {
        ret = DBI::dbIsValid(private$pconn)
      }
      ret
    },
    #' @description
    #' Renews the connection without changing the duckdb_conn object.
    #' Warning: all in-memory tables are lost
    renew = function() {
      self$disconnect()
      private$pconn = get_conn(
        ext = private$pext,
        dbdir = private$pdbdir,
        geometry = private$pgeometry,
        bigint = private$pbigint
      )
      invisible(self)
    },
    #' @description
    #' Disconnects the current connection. A new connection will be created
    #' on the next call to '$conn'.
    #' Warning: all in-memory tables are lost
    disconnect = function() {
      if (self$is_connected()) {
        tryCatch(
          suppressWarnings(DBI::dbDisconnect(private$pconn)),
          error = function(e) warning("dbDisconnect error: ", conditionMessage(e))
        )
      }
      private$pconn <- NULL
    }
  ),
  private = list(
    pext = NULL,
    pdbdir = NULL,
    pgeometry = NULL,
    pbigint = NULL,
    pconn = NULL,
    finalize = function() {
      self$disconnect()
    }
  ),
  active = list(
    #' @field ext Loaded extensions
    ext = function(value) {
      if (!missing(value)) {
        stop("ext can't be modified. Use duckdb_conn$new(ext=...)")
      }
      private$pext
    },
    #' @field dbdir Name of the duckdb database
    dbdir = function(value) {
      if (!missing(value)) {
        stop("dbdir can't be modified. Use duckdb_conn$new(dbdir=...)")
      }
      private$pdbdir
    },
    #' @field geometry Geometry format name, 'wk' (default) or 'blob'
    geometry = function(value) {
      if (!missing(value)) {
        stop("geometry can't be modified. Use duckdb_conn$new(geometry=...)")
      }
      private$pgeometry
    },
    #' @field bigint bigint conversion type, 'integer64' (default) or 'numeric'
    bigint = function(value) {
      if (!missing(value)) {
        stop("bigint can't be modified. Use duckdb_conn$new(bigint=...)")
      }
      private$pbigint
    },
    #' @field conn duckdb connection
    conn = function() {
      if (!self$is_connected()) {
        self$renew()
      }
      private$pconn
    }
  )
)
