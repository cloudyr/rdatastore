process_result_set <- function(req) {

    df <- dplyr::tbl_df(format_from_results(req$batch$entityResults$entity$properties))
    paths <- dplyr::bind_rows(req$batch$entityResults$entity$key$path)
    if ("id" %in% names(paths)) {
      paths$name <- paths$id
    }
    df <- dplyr::bind_cols(df, paths)

    if (nrow(df) > 0) {
      dplyr::select(df, kind, name, dplyr::everything())
    } else {
      df
    }
}


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
  all_fetched = "NOT_FINISHED"
  result_list <- list()
  result_c <- 1
  offset = 0
  while(all_fetched == "NOT_FINISHED") {
    q <- paste(query, "OFFSET", offset)
    body <- list(gqlQuery = list(queryString = q, allowLiterals = TRUE))
    req <- rdatastore_env$query_ds(the_body = body)
    all_fetched <- req$batch$moreResults
    req <- process_result_set(req)
    if (nrow(req) == 0) {
      all_fetched = T
    }
    result_list[[result_c]] <- req
    result_c = result_c + 1
    offset = result_c*300 - 300
  }

  # Bind result sets together and return
  dplyr::bind_rows(result_list)
}
