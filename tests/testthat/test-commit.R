
test_that("commit entity", {
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


test_that("update entity", {
  update <- commit("test",
         insertion_name,
         string_prop = "update_test",
         integer_prop = as.integer(10),
         double_prop = 100,
         timestamp_prop = Sys.time(),
         boolean_prop = FALSE,
         mutation_type = "update")
  updated_item <- lookup("test", insertion_name)

  # Convert dates to characters for comparison
  update$content$timestamp_prop <- substr(as.character(update$content$timestamp_prop), 1, 19)
  updated_item$timestamp_prop <-  substr(as.character(updated_item$timestamp_prop), 1, 19)

  expect_equivalent(updated_item, update$content)
})


test_that("upsert entity", {
  upsert <- commit("test",
         insertion_name,
         string_prop = "upsert_test",
         integer_prop = as.integer(300),
         double_prop = 250,
         timestamp_prop = Sys.time(),
         boolean_prop = TRUE,
         mutation_type = "upsert")

  upserted_item <- lookup("test", insertion_name)

  # Convert dates to characters for comparison
  upsert$content$timestamp_prop <- substr(as.character(update$content$timestamp_prop), 1, 19)
  upserted_item$timestamp_prop <-  substr(as.character(updated_item$timestamp_prop), 1, 19)

  expect_equal(upserted_item, upsert$content)
})


test_that("keep existing", {
  insertion_name <- paste0("test-existing:", Sys.time())
  commit("test", insertion_name, existing_value = "great")
  commit("test", insertion_name, new_value = "awesome")

  keep_existing_result <- lookup("test", insertion_name)
  expect_equal(ncol(keep_existing_result), 4)
})

test_that("do not keep existing", {
  insertion_name <- paste0("test-existing:", Sys.time())
  commit("test", insertion_name, existing_value = "great")
  commit("test", insertion_name, new_value = "awesome", keep_existing = F)

  keep_existing_result <- lookup("test", insertion_name)
  expect_equal(ncol(keep_existing_result), 3)
})



test_that("no name no data", {
  no_data <- commit("test", name = NULL)
  expect_equal(nrow(no_data$content), 0)
})





