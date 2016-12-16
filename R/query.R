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
  if (is.null(rdatastore_env$load_ds)) {
    stop("Please Authenticate")
  }
  all_fetched = "NOT_FINISHED"
  result_list <- list()
  result_c <- 1
  cursor <- NULL
  while(all_fetched == "NOT_FINISHED") {
    if (!is.null(cursor)) {
      q <- paste(query, "OFFSET @cursor")
      nb <- list("cursor" = list("cursor" = cursor))
      body <- list(gqlQuery = list(queryString = q, allowLiterals = FALSE, namedBindings = nb))
    } else {
      q <- query
      body <- list(gqlQuery = list(queryString = q, allowLiterals = TRUE))
    }
    req <- rdatastore_env$query_ds(the_body = body)
    n_results <- nrow(req$batch$entityResults)
    if (n_results > 0) {
      cursor <- req$batch$entityResults$cursor[n_results]
    } else {
      cursor <- NULL
    }
    all_fetched <- req$batch$moreResults
    req <- process_result_set(req)
    if (nrow(req) == 0) {
      all_fetched = T
    }
    result_list[[result_c]] <- req
    result_c = result_c + 1
  }

  # Bind result sets together and return
  dplyr::bind_rows(result_list)
}
