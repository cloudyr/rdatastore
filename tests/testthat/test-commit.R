
test_that("commi entity", {
  item <- commit("test",
                           "test-item",
                           string_prop = "string",
                           integer_prop = as.integer(28),
                           double_prop = 3.14,
                           timestamp_prop = Sys.time(),
                           boolean_prop = TRUE)
  expect_equal(nchar(item$transaction_id), 136)
})


test_that("delete entity", {
  commit("test",
         "test-delete",
         string_prop = "string",
         integer_prop = as.integer(28),
         double_prop = 3.14,
         timestamp_prop = Sys.time(),
         boolean_prop = TRUE)
  commit("test", "test-delete", mutation_type = "delete")
  expect_equal(0, nrow(lookup("test", "test_delete")))
})

insertion_name <- Sys.time()

test_that("insert entity", {
  item <- commit("test",
         insertion_name,
         string_prop = "insertion_test",
         integer_prop = as.integer(28),
         double_prop = 3.14,
         timestamp_prop = Sys.time(),
         boolean_prop = TRUE,
         mutation_type = "insert")
  expect_equal(nchar(item$transaction_id), 136)
})


test_that("insert entity", {
  update <- commit("test",
         insertion_name,
         string_prop = "update_test",
         integer_prop = as.integer(10),
         double_prop = 100,
         timestamp_prop = Sys.time(),
         boolean_prop = FALSE,
         mutation_type = "update")
  updated_item <- lookup("test", insertion_name)
  expect_equal(updated_item, update$content)
})

