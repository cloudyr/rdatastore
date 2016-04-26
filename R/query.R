
#' gql
#'
#' Query using the Google Query Language (GQL)
#'
#' @param query The query to run.
#'
#' @examples
#' gql("SELECT * FROM test")
#'
#' @return Data frame of results
#'
#' @seealso \url{https://cloud.google.com/datastore/docs/apis/gql/gql_reference} - The google query language
#' @importFrom dplyr %>%
#'
#' @export


gql <- function(query) {
  loadNamespace("rdatastore")
  body <- list(gqlQuery = list(queryString = query, allowLiterals = TRUE))


  req <- httr::POST(paste0(rdatastore_env$url, ":runQuery"),
                    httr::config(token = rdatastore_env$token),
                    body =  body,
                    encode = "json",
                    httr::verbose())

  # Return transaction id if successful, else error.
  if (req$status_code == 200) {
    results <- dplyr::bind_rows(lapply(httr::content(req)$batch$entityResults, function(row) {
      kind = row$entity$key$path[[1]]$kind
      if ("name" %in% names(row$entity$key$path[[1]])) {
        name = row$entity$key$path[[1]]$name
      } else {
        name = row$entity$key$path[[1]]$id
      }
      row <- format_from_results(row$entity$properties)
      row$kind = kind
      row$name = name
      row
    }))

    if (nrow(results) > 0) {
      dplyr::select(results, kind, name, everything())
    } else {
      results
    }

  } else {
    stop(paste0(httr::content(req)$error$code, ": ", httr::content(req)$error$message))
  }
}
