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
