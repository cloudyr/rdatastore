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
  mapply(names(properties), FUN = function(var_name) {
    var_value <- properties[[var_name]]
    if (lubridate::is.Date(var_value) ||
        lubridate::is.POSIXt(var_value) ||
        lubridate::is.timepoint(var_value)) {
      var_type <- datastore_types$date
      var_value <- strftime(var_value, format = "%FT%H:%M:%OSZ", tz = "GMT")
    } else if (!(typeof(var_value) %in% names(datastore_types))) {
      var_type <- datastore_types$binary
      print(var_value)
      var_value <- base64enc::base64encode(serialize(var_value, NULL, ascii=T))
    } else {
      var_type <- datastore_types[[typeof(var_value)]]
    }
    prop = list()
    prop[[var_type]] = var_value
    prop
  }, SIMPLIFY = FALSE)
}


# Return transaction ID
transaction <- function() {
  req <- httr::POST(paste0(rdatastore_env$url, ":beginTransaction"),
                    httr::config(token = rdatastore_env$token),
                    encode = "json")
  if (req$status_code != 200) {
    stop(httr::content(req)$error$message)
  }
  httr::content(req)$transaction
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
  # Locate Credentials
  if (file.exists(as.character(credentials))) {
    credentials <- jsonlite::fromJSON(credentials)
  } else if (Sys.getenv(credentials) != "") {
    credentials <- jsonlite::fromJSON(Sys.getenv(credentials))
  } else {
    stop("Could not find credentials")
  }

  google_token <- httr::oauth_service_token(httr::oauth_endpoints("google"),
                                            credentials,
                                            scope = datastore_url)

  url <- paste0("https://datastore.googleapis.com/v1beta3/projects/", project)
  # Create global variable for project id
  assign("project_id", project, envir=rdatastore_env)
  assign("token", google_token, envir=rdatastore_env)
  assign("url", url, envir=rdatastore_env)
}

#' Authenticate Datastore
#'
#' Authenticate datastore using OAuth 2.0. Create an application on the
#' \strong{google cloud platform}
#' and generate
#'
#' @param key OAuth 2.0 credential key
#' @param secret OAuth credential secret key
#' @param project Google cloud platform project id/name.
#'
#' @seealso \url{https://cloud.google.com/}
#' @seealso \url{https://cloud.google.com/datastore/docs/concepts/overview}
#' @seealso \url{https://developers.google.com/identity/protocols/OAuth2#basicsteps}
#'
#' @export

authenticate_datastore <- function(key, secret, project) {
  # Authorize app
  app <- httr::oauth_app("google",
                         key = key,
                         secret = secret)

  # Fetch token
  google_token <- httr::oauth2.0_token(httr::oauth_endpoints("google"),
                                       app,
                                       scope = datastore_url)
  url <- paste0("https://datastore.googleapis.com/v1beta3/projects/", project)
  # Create global variable for project id
  assign("project_id", project, envir=rdatastore_env)
  assign("token", google_token, envir=rdatastore_env)
  assign("url", url, envir=rdatastore_env)
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

  if(all(is.null(name), is.null(id))) {
    stop("Must specify a name or id. Set name = NULL to auto-allocate id.")
  }

  if (is.null(id)) {
    lookup_q <- list(kind = kind, name = name)
  } else if (is.null(name)) {
    lookup_q <- list(kind = kind, id = as.character(id))
  } else {
    stop("Must specify name or id. You can not specify both.")
  }

  req <- httr::POST(paste0(rdatastore_env$url, ":lookup"),
                    httr::config(token = rdatastore_env$token),
                    body = list(keys = list(path = lookup_q)),
                    encode = "json")

  if (req$status_code != 200) {
    stop(paste0(httr::content(req)$error$code, ": ", httr::content(req)$error$message))
  }

  resp <- jsonlite::fromJSON(httr::content(req, as = "text"))$found

  variables <- names(resp$entity$properties)
  values <- resp$entity$properties
  results <- resp$entity$properties

  # Convert Variable types and return as data frame.
  results <- format_from_results(results)
  dplyr::tbl_df(as.data.frame(results, stringsAsFactors = F))  %>%
    dplyr::mutate(kind = kind, name = name) %>%
    dplyr::select(kind, name, everything())
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
