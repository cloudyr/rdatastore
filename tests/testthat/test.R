
# Authenticate
authenticate_service_account("credentials", "andersen-lab")

test_that("Logical equivalence", {
  x <- TRUE
  expect_that(x, equals(TRUE))
})
