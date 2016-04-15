
client_secret <- paste0(find.package("rdatastore"), "/client-secret.json")
authenticate_datastore_service(client_secret, "andersen-lab")
print(dir("/home/travis/build/danielecook/rdatastore/rdatastore.Rcheck/"))

test_that("is true true?", {
  x <- TRUE
  expect_equal(TRUE, TRUE)
})


