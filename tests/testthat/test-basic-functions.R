# duckdb_conn lifecycle tests
# These use an in-memory DuckDB database and require no network access.

test_that("duckdb_conn: is_connected() is FALSE on a new object before accessing $conn", {
  db <- duckdb_conn$new()
  expect_false(db$is_connected())
})

test_that("duckdb_conn: accessing $conn establishes a connection", {
  db <- duckdb_conn$new()
  conn <- db$conn
  expect_true(db$is_connected())
  db$disconnect()
})

test_that("duckdb_conn: is_connected() is FALSE after disconnect()", {
  db <- duckdb_conn$new()
  conn <- db$conn  # trigger connection
  db$disconnect()
  expect_false(db$is_connected())
})

test_that("duckdb_conn: disconnect() on a non-connected object does not error", {
  db <- duckdb_conn$new()
  expect_no_error(db$disconnect())
})

test_that("duckdb_conn: read-only fields ext and dbdir are accessible", {
  db <- duckdb_conn$new(ext = "none", dbdir = ":memory:")
  expect_equal(db$ext, "none")
  expect_equal(db$dbdir, ":memory:")
})

test_that("duckdb_conn: ext field is read-only", {
  db <- duckdb_conn$new()
  expect_error(db$ext <- "core", "modifi")
})

test_that("duckdb_conn: dbdir field is read-only", {
  db <- duckdb_conn$new()
  expect_error(db$dbdir <- "/tmp/test.duckdb", "modifi")
})
