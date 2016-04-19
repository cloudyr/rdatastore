
#' commit_df
#'
#' Query using the Google Query Language (GQL)
#'
#' @param kind dataframe name
#' @param name name column to use.
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



commit_df <- function(kind = NULL, name = NULL, df, mutation_type = "upsert", keep_existing = TRUE) {

  if (is.NULL(kind)) {
    kind <- deparse(substitute(kind))
  }

  transaction_id <- transaction()

  mutation = list()

  keep_cols = which(!(names(df) == name))

  data <- lapply(1:nrow(df), function(i) {
    # If name is null, autoallocate id.
    if (!is.null(df[i,name])) {
      path_item <- list(
        kind = kind,
        name = df[i,name]$name
      )
    } else {
      path_item <- list(
        kind = kind
      )
    }
    properties <- df[i,keep_cols] %>%
                  unlist() %>%
                  format_to_properties()

    key_obj <- list(key = list(path = path_item),
                      properties = properties)

    c(list("upsert" = key_obj))
  })

   body <- list(mutations = c(data),
               transaction = transaction_id
  )


  req <- httr::POST(paste0(rdatastore_env$url, ":commit"),
                    httr::config(token = rdatastore_env$token),
                    body =  body,
                    encode = "json",
                    httr::verbose())


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
