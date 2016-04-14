
# Authenticate
credentials <- as.list(Sys.getenv(x = c("key", "secret", "project"), names = TRUE))

authenticate_datastore(key = credentials$key,
                       secret = credentials$secret,
                       project = credentials$project)

test_that("Logical equivalence", {
  x <- TRUE
  expect_that(x, equals(TRUE))
})
