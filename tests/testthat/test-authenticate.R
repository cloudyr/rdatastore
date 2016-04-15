

print(dir(find.package("rdatastore")))
print(getwd())
print(dir(getwd()))
print(dir(getwd("../")))
print(dir(getwd("../../")))
print(dir("~"))
authenticate_datastore_service("client-secret.json", "andersen-lab")

test_that("authenticate", {
  x <- TRUE
  expect_equal(TRUE, TRUE)
})


