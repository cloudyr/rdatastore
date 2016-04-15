
test_that("commi entityt", {
  transaction_id <- commit("test",
                           "test-item",
                           string_prop = "string",
                           integer_prop = as.integer(28),
                           double_prop = 3.14,
                           timestamp_prop = Sys.time(),
                           boolean_prop = TRUE)
  expect_equal(nchar(transaction_id), 136)
})
