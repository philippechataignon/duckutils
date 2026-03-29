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

#' Build a personal / user-specific directory path
#' @param ... Path components passed to [file.path()].
#' @return A character string with the constructed path.
#' @details
#' This is a thin wrapper around [file.path()] intended for building
#' user-specific paths (e.g. a personal S3 prefix or local directory).
#' Override this function in your project to point to your own root
#' directory if the default (`file.path(...)`) is not suitable.
#' @keywords internal
perso <- function(...) {
  file.path(...)
}
