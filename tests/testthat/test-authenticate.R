test_that("Authenticate", {
  client_secret <- paste0(find.package("rdatastore"), "/client-secret.json")
  authenticate_datastore_service(client_secret, Sys.getenv("project_id"))
  expect_equal(TRUE, !is.null(rdatastore_env$token))
})


