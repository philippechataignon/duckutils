#' Generate a temporary name
#' @param prefix Character prefix for the name
#' @return Character string with temporary name
#' @keywords internal
tempname <- function(prefix = "temp") {
  paste0(
    prefix, "_",
    gsub("[^0-9]", "", as.character(Sys.time())), "_",
    Sys.getpid(), "_",
    sample(10000:99999, 1)
  )
}

#' Refresh S3 credentials
#' @param conn DuckDB connection
#' @details
#' This is a placeholder function. Override it with your own implementation
#' if your workflow requires refreshing S3 credentials (e.g., short-lived
#' STS tokens). By default it does nothing.
#' @keywords internal
refresh_secret <- function(conn) {
  invisible(NULL)
}
