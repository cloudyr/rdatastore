
test_that("lookup entity", {
  item <- lookup("test", "test-item")
  expect_is(item$integer_prop, "integer")
  expect_is(item$double_prop, "numeric")
  expect_is(item$boolean_prop, "logical")
  expect_is(item$string_prop, "character")
  expect_is(item$timestamp_prop, "POSIXct")
})


