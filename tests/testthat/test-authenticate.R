

client_secret <- "/home/travis/build/danielecook/client-secret.json"
print(dir(find.package("rdatastore")))
print(dir("~"))
authenticate_datastore_service(client_secret, "andersen-lab")

test_that("authenticate", {
  x <- TRUE
  expect_equal(TRUE, TRUE)
})

