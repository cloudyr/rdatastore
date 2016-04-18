
test_that("Perform GQL Query", {
  commit("test", "q1", q_item = as.integer(1))
  commit("test", "q2", q_item = as.integer(2))
  commit("test", "q3", q_item = as.integer(3))
  commit("test", "q4", q_item = as.integer(4))
  results <- gql("SELECT * FROM test WHERE q_item > 2")
  expect_equal(nrow(results), 2)
})


