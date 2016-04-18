
#' commit_df
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



commit_df <- function(kind, name, df, mutation_type = "upsert", keep_existing = TRUE) {

  existing_data <- NA
  if (keep_existing & !is.null(name) & mutation_type != "delete") {
    # Fold in existing values
    existing_data <- lookup(kind, name) %>%
      dplyr::select(-kind,-name)
  }

  transaction_id <- transaction()

  # If name is null, autoallocate id.
  if (!is.null(name)) {
    path_item <- list(
      kind = kind,
      name = name
    )
  } else {
    path_item <- list(
      kind = kind
    )
  }

  # Setup properties
  if (length(list(...)) > 0) {
    key_obj <- c(list(key = list(path = path_item),
                      properties = format_to_properties(list(...), existing_data)))
  } else {
    key_obj <- c(list(key = list(path = path_item)))
  }
  # Generate mutation
  mutation = list()
  if (mutation_type == "delete") {
    mutation[[mutation_type]] =  list(path = path_item)
  } else {
    mutation[[mutation_type]] =  key_obj
  }


  body <- list(mutations = mutation,
               transaction = transaction_id
  )


  req <- httr::POST(paste0(rdatastore_env$url, ":commit"),
                    httr::config(token = rdatastore_env$token),
                    body =  body,
                    encode = "json")


  # Return transaction id if successful, else error.
  if (req$status_code == 200) {
    results <- dplyr::tbl_df(as.data.frame(list(...), stringsAsFactors = F)) %>%
      dplyr::mutate(kind = kind) %>%
      dplyr::select(kind, everything())

    if (!is.null(name)) {
      results <- dplyr::mutate(results, name = name) %>%
        dplyr::select(kind, name, everything())
    }

    list(content = results,
         transaction_id = transaction_id)
  } else {
    stop(paste0(httr::content(req)$error$code, ": ", httr::content(req)$error$message))
  }
}
