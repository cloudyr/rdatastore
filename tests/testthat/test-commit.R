
test_that("commit", {
  transaction_id <- commit("test",
                           "test-item",
                           string_prop = "string",
                           integer_prop = as.integer(28),
                           double_prop = 3.14,
                           timestamp = Sys.time())
  expect_equal(nchar(transaction_id), 136)
})
