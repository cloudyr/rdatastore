
authenticate_service_account("credentials", "andersen-lab")

test_that("authenticate", {
  x <- TRUE
  expect_equal(TRUE, TRUE)
})


test_that("str_length is number of characters", {
  expect_equal(str_length("a"), 1)
  expect_equal(str_length("ab"), 2)
  expect_equal(str_length("abc"), 3)
})
