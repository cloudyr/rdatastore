
#' commit_df
#'
#' Query using the Google Query Language (GQL)
#'
#' @param kind dataframe name
#' @param name name column to use. Use "row.names" to use row names.
#'
#' @examples
#' data(mtcars)
#' commit_df(kind = "test")
#'
#' @return Data frame of results
#'
#' @seealso \url{https://cloud.google.com/datastore/docs/apis/gql/gql_reference} - The google query language
#' @importFrom dplyr %>%
#'
#' @export

commit_df <- function(kind = NULL, name = NULL, df, mutation_type = "upsert", keep_existing = TRUE) {

  if (is.null(kind)) {
    kind <- deparse(substitute(kind))
  }
  # Maximum of 25 items can be added at once.
  df_groups <- split(df, (1:nrow(df)) %/% 25)

  sapply(df_groups, function(df_g) {
    transaction_id <- rdatastore_env$transaction()
    mutation = list()

    if (!is.null(name)) {
      keep_cols = which(!(names(df_g) == name))
    } else {
      keep_cols = 1:length(df_g)
    }
    data <- lapply(1:nrow(df_g), function(i) {
      # If name is null, autoallocate id.
      if (!is.null(name)) {
        if (name == "row.names") {
          name = row.names(df_g[i,])[1]
        } else {
          name <- df_g[i,name]$name
        }
        path_item <- list(
          kind = kind,
          name = df_g[i,name]$name
        )
      } else {
        path_item <- list(
          kind = kind
        )
      }
      properties <- df_g[i,keep_cols] %>%
                    unlist() %>%
                    format_to_properties()

      key_obj <- list(key = list(path = path_item),
                        properties = properties)

      c(list("upsert" = key_obj))
    })

     body <- list(mutations = c(data[1:nrow(df_g)]),
                  transaction = transaction_id)
    rdatastore_env$commit_ds(the_body = body)
  })
}
