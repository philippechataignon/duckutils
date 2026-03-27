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
    #' Crée un objet 'duckdb_conn'
    #' @param ext Extensions chargées 'core': httpfs+spatial, 'geo' core+h3
    #' 'all' geo+ead_stat. 'none' par défaut
    #' @param dbdir Nom du la base duckdb, par défaut base stockée en mémoire
    #' @param geometry Nom de la géométrie, 'wk' (défaut) ou 'blob'
    #' @param bigint Conversion des bigint, 'integer64' (défaut) ou 'numeric'
    #' @return Un objet 'duckdb_conn'
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
    #' Fonction 'print' dédiée
    #' @param ... Inutilisé
    print = function(...) {
      message("Duckdb connection ", "ext:", private$pext, " dbdir:", private$pdbdir)
      if (self$is_connected())
        print(private$pconn)
      else
        message("No connection")
      invisible(self)
    },
    #' @description
    #' Connexion valide ?
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
    #' Renouvelle la connexion sans changer de duckdb_conn
    #' Attention: toutes les tables en mémoire sont perdues
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
    #' Rafraichit les identifiants de connexion S3
    refresh = function() {
      if (self$is_connected()) {
        refresh_secret(private$pconn)
      }
      invisible(self)
    },
    #' @description
    #' Déconnecte la connexion courante. Une nouvelle connexion sera créée
    #' au prochain appel de '$conn'
    #' Attention: toutes les tables en mémoire sont perdues
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
    #' @field ext Extensions chargées
    ext = function(value) {
      if (!missing(value)) {
        stop("ext can't be modified. Use duckdb_conn$new(ext=...)")
      }
      private$pext
    },
    #' @field dbdir Nom de la base duckdb
    dbdir = function(value) {
      if (!missing(value)) {
        stop("dbdir can't be modified. Use duckdb_conn$new(dbdir=...)")
      }
      private$pdbdir
    },
    #' @field geometry Nom de la géométrie, 'wk' (défaut) ou 'blob'
    geometry = function(value) {
      if (!missing(value)) {
        stop("geometry can't be modified. Use duckdb_conn$new(geometry=...)")
      }
      private$pgeometry
    },
    #' @field bigint Conversion des bigint, 'integer64' (défaut) ou 'numeric'
    bigint = function(value) {
      if (!missing(value)) {
        stop("bigint can't be modified. Use duckdb_conn$new(bigint=...)")
      }
      private$pbigint
    },
    #' @field conn Connexion duckdb
    conn = function() {
      if (!self$is_connected()) {
        self$renew()
      }
      private$pconn
    }
  )
)
