# Constants
rdatastore_env <- new.env()
datastore_url <- "https://www.googleapis.com/auth/datastore"
datastore_types <- list("integer" = "integerValue",
                        "double" = "doubleValue",
                        "character" = "stringValue",
                        "logical" = "booleanValue",
                        "NULL" = "nullValue",
                        "date" = "timestampValue",
                        "binary" = "blobValue")
# Utility Functions
format_from_results <- function(results) {
  properties <- lapply(names(results), function(var_name, var_value) {
    var_type <- names(results[[var_name]])[[1]]
    if (var_type == "integerValue") {
      type_conv <- as.integer
    } else if (var_type == "stringValue") {
      type_conv <- as.character
    } else if (var_type == "blobValue") {
      type_conv <- function(x) {
        unserialize(base64enc::base64decode(x$blobValue))
      }
    } else if (var_type == "doubleValue") {
      type_conv <- as.double
    } else if (var_type == "booleanValue") {
      type_conv <- as.logical
    } else if (var_type == "timestampValue") {
      type_conv <- function(x) { lubridate::with_tz(lubridate::ymd_hms(x), Sys.timezone()) }
    } else if (var_type == "nullValue") {
      # Can't be null or data frame won't return correctly.
      type_conv <- as.logical
      results[[var_name]] <- NA
    } else if (var_type == "keyValue") {
      type_conv <- as.character
      q_kind <- results[[var_name]]$keyValue$path[[1]]$kind
      q_name  <- results[[var_name]]$keyValue$path[[1]]$name
      results[[var_name]] <- paste0("Key(", q_kind, ", '", q_name,"')")
    }

    type_conv(results[[var_name]][[1]])
  }, results)
  names(properties) <- names(results)
  properties
}


format_to_properties <- function(properties, existing_data = F) {
  # Replace edits and add new ones.
  if (is.data.frame(existing_data)) {
    new_items <- names(properties)
    existing_items <- names(existing_data)
    existing_data <- existing_data[setdiff(existing_items, new_items)]
    properties <- append(properties, as.list(existing_data))
  }
  # Format a list in R into nested list
  # structure required for REST API.
  results <- mapply(names(properties), FUN = function(var_name) {
    var_value <- properties[[var_name]]
    if (!is.na(var_value)) {
    if (lubridate::is.Date(var_value) ||
        lubridate::is.POSIXt(var_value) ||
        lubridate::is.timepoint(var_value)) {
      var_type <- datastore_types$date
      var_value <- strftime(var_value, format = "%FT%H:%M:%OSZ", tz = "GMT")
    } else if (!(typeof(var_value) %in% names(datastore_types))) {
      var_type <- datastore_types$binary
      var_value <- base64enc::base64encode(serialize(var_value, NULL, ascii=T))
    } else {
      var_type <- datastore_types[[typeof(var_value)]]
    }
    prop = list()
    prop[[var_type]] = var_value
    prop
    }
  }, SIMPLIFY = FALSE)
  results[!sapply(results, is.null)]
}


#===============#
# Datastore API #
#===============#

register_api <- function(project) {
  loadNamespace("rdatastore")
    base_url <- paste0("https://datastore.googleapis.com/v1/projects/", project)

    transaction <- googleAuthR::gar_api_generator(paste0(base_url, ":beginTransaction"),
                                                  "POST",
                                                  data_parse_function = function(x) x$transaction)

    commit_ds <- googleAuthR::gar_api_generator(paste0(base_url, ":commit"),
                                                "POST",
                                                data_parse_function = function(x) x)

    load_ds <- googleAuthR::gar_api_generator(paste0(base_url, ":lookup"),
                                              "POST",
                                              data_parse_function = function(x) x)

    query_ds <- googleAuthR::gar_api_generator(paste0(base_url, ":runQuery"),
                                               "POST",
                                               data_parse_function = function(resp) resp)

    assign("transaction", transaction, envir=rdatastore_env)
    assign("commit_ds", commit_ds, envir=rdatastore_env)
    assign("load_ds", load_ds, envir=rdatastore_env)
    assign("query_ds", query_ds, envir=rdatastore_env)
}

#' Authenticate Datastore using a service account
#'
#' Set credentials to the name of an environmental variable
#' or the location of your service account json key file.
#'
#' @param credentials Environmental variable or service account.
#' @param project Google cloud platform project id/name.
#'
#' @seealso \url{https://cloud.google.com/}
#' @seealso \url{https://cloud.google.com/datastore/docs/concepts/overview}
#' @seealso \url{https://developers.google.com/identity/protocols/OAuth2#basicsteps}
#'
#' @export

authenticate_datastore_service <- function(credentials, project) {
  options("googleAuthR.scopes.selected" = c("https://www.googleapis.com/auth/datastore"))
  register_api(project)
  googleAuthR::gar_auth_service(credentials)
}

#' Authenticate Datastore
#'
#' Authenticate datastore using OAuth 2.0. Create an application on the
#' \strong{google cloud platform}
#' and generate
#'
#' @param project Google cloud platform project id/name.
#'
#' @seealso \url{https://cloud.google.com/}
#' @seealso \url{https://cloud.google.com/datastore/docs/concepts/overview}
#' @seealso \url{https://developers.google.com/identity/protocols/OAuth2#basicsteps}
#'
#' @export

authenticate_datastore <- function(project) {
  options("googleAuthR.scopes.selected" = c("https://www.googleapis.com/auth/datastore"))
  assign("project_id", project, envir=rdatastore_env)
  register_api(project)
  googleAuthR::gar_auth()
}


# Authenticate for testing examples
if (Sys.getenv("travis") == TRUE) {
  client_secret <- paste0(find.package("rdatastore"), "/client-secret.json")
  authenticate_datastore_service(client_secret, Sys.getenv("project_id"))
}

#' Lookup
#'
#' @param kind The entity kind
#' @param name The entity name.
#' @param id The entity id.
#' @param project Google cloud platform project id/name.
#'
#' @examples
#' lookup("test", "m")
#'
#' @seealso \url{https://cloud.google.com/datastore/docs/concepts/entities} - Entities, Properties, and Keys
#' @return dataframe of entity.
#' @importFrom dplyr %>%
#' @export

lookup <- function(kind, name = NULL, id = NULL) {

  if (is.null(rdatastore_env$load_ds)) {
    stop("Please Authenticate")
  }

  if(all(is.null(name), is.null(id))) {
    stop("Must specify a name or id. Set name = NULL to auto-allocate id.")
  }

  if (is.null(id)) {
    path_item <- list(kind = kind, name = name)
  } else if (is.null(name)) {
    path_item <- list(kind = kind, id = as.character(id))
  } else {
    stop("Must specify name or id. You can not specify both.")
  }

  resp <- rdatastore_env$load_ds(the_body = list(keys = list(path = path_item)))
  if ("found" %in% names(resp)) {
    resp <- resp$found
    variables <- names(resp$entity$properties)
    values <- resp$entity$properties
    results <- resp$entity$properties

    # Convert Variable types and return as data frame.
    results <- format_from_results(results)
    dplyr::tbl_df(as.data.frame(results, stringsAsFactors = F))  %>%
      dplyr::mutate(kind = kind, name = name) %>%
      dplyr::select(kind, name, dplyr::everything())
  }
}


#' commit
#'
#' Commit a single entity to the datastore.
#' Specify a \code{kind} and optionally a \code{name} (setting \code{name = NULL} will result in an id being assigned as a substitute for name).
#' Additional arguments are treated as properties of the entity. \code{mutation_type} can be set to one of the following:
#'
#'
#'
#' @param kind The entity kind
#' @param name The name of the entity. If NULL, id is autogenerated.
#' @param ... Additional arguments to store as properties.
#' @param mutation_type The type of mutation. One of \strong{insert}, \strong{update}, \strong{upsert}, or \strong{delete}. Default is upsert.
#' @param keep_existing Preserve/edit existing data. The entity will be retrieved and specified properties replaced or new properties added. Existing properties will be retained. Default is TRUE.
#'
#' @examples
#' commit("test", "entity_name", int_column = as.integer(1), str_column = "awesome!", float_column = 3.14)
#'
#' @return list of content and the transaction id
#'
#' @seealso \url{https://cloud.google.com/datastore/docs/concepts/entities} - Entities, Properties, and Keys
#' @importFrom dplyr %>%
#'
#' @export


commit <- function(kind, name = NULL, ..., mutation_type = "upsert", keep_existing = TRUE) {
  if (is.null(rdatastore_env$load_ds)) {
    stop("Please Authenticate")
  }

  # Check to ensure "id" not in ...
  if ("id" %in% names(list(...))) {
    stop("Can't use id as a variable name; Use name = NULL to autogenerate id.")
  }

  existing_data <- NA
  if (keep_existing & !is.null(name) & mutation_type != "delete") {
    # Fold in existing values
    existing <- lookup(kind, name)
    if (!is.null(existing)) {
      existing_data <- existing %>%
                       dplyr::select(-kind,-name)
    }
  }


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
  } else if (!is.na(existing_data)) {
    key_obj <- c(list(key = list(path = path_item),
                      properties = format_to_properties(existing_data)))
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

  transaction_id <-  rdatastore_env$transaction()

  body <- list(mutations = mutation,
               transaction = transaction_id
  )
  rdatastore_env$commit_ds(the_body = body)

  results <- dplyr::tbl_df(as.data.frame(list(...), stringsAsFactors = F)) %>%
             dplyr::mutate(kind = kind) %>%
             dplyr::select(kind, dplyr::everything())

  if (!is.null(name)) {
    results <- dplyr::mutate(results, name = name) %>%
               dplyr::select(kind, name, dplyr::everything())
  }

  if (!is.null(names(existing_data))) {
    existing_data <- existing_data[,!names(existing_data) %in% names(results)]
    results <- results %>% dplyr::bind_cols(existing_data)
  }

  list(content = results,
       transaction_id = transaction_id)
}
