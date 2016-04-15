print(getwd())
client_secret <- paste0(find.package("rdatastore"), "/client-secret.json")
authenticate_datastore_service(client_secret, "andersen-lab")

test_that("authenticate", {
  x <- TRUE
  expect_equal(TRUE, TRUE)
})

