# Constants
Rdatastore <- new.env()
datastore_url <- "https://www.googleapis.com/auth/datastore"
datastore_types <- list("integer" = "integerValue",
                        "double" = "doubleValue",
                        "character" = "stringValue",
                        "logical" = "booleanValue",
                        "NULL" = "nullValue",
                        "date" = "timestampValue")

# Utility Functions
format_from_results <- function(results) {
  properties <- lapply(names(results), function(var_name, var_value) {
    var_type <- names(results[[var_name]])
    if (var_type == "integerValue") {
      type_conv <- as.integer
    } else if (var_type == "stringValue") {
      type_conv <- as.character
    } else if (var_type == "blobValue") {
      type_conv <- as.character
    } else if (var_type == "doubleValue") {
      type_conv <- as.double
    } else if (var_type == "booleanValue") {
      type_conv <- as.logical
    } else if (var_type == "timestampValue") {
      type_conv <- lubridate::ymd_hms
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

    type_conv(results[[var_name]])
  }, results)
  names(properties) <- names(results)
  properties
}


format_to_properties <- function(properties) {
  # Format a list in R into nested list
  # structure required for REST API.
  mapply(names(properties), FUN = function(var_name) {
    var_value <- properties[[var_name]]
    if (lubridate::is.Date(var_value) ||
        lubridate::is.POSIXt(var_value)) {
      var_type <- datastore_types["date"]
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
  req <- httr::POST(paste0(Rdatastore$url, ":beginTransaction"),
                    httr::config(token = Rdatastore$token),
                    encode = "json")
  httr::content(req)$transaction
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
  assign("project_id", project, envir=Rdatastore)
  assign("token", google_token, envir=Rdatastore)
  assign("url", url, envir=Rdatastore)
}

if (file.exists("credentials.json")) {
  credentials <- jsonlite::fromJSON("credentials.json")
  authenticate_datastore(key = credentials$key,
                         secret = credentials$secret,
                         project = credentials$project)
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


authenticate_service_account <- function(credentials, project) {

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
  assign("project_id", project, envir=Rdatastore)
  assign("token", google_token, envir=Rdatastore)
  assign("url", url, envir=Rdatastore)
}

#' Lookup
#'
#' @param kind The entity kind
#' @param name The entity name.
#' @param id The entity id.
#' @param project Google cloud platform project id/name.
#'
#' @seealso \url{https://cloud.google.com/datastore/docs/concepts/entities} - Entities, Properties, and Keys
#' @return dataframe of entity.
#' @export

lookup <- function(kind, name = NULL, id = NULL) {

  if(all(is.null(name), is.null(id))) {
    stop("Must specify a name or id")
  }

  if (is.null(id)) {
    lookup_q <- list(kind = kind, name = name)
  } else if (is.null(name)) {
    lookup_q <- list(kind = kind, id = as.character(id))
  } else {
    stop("Must specify name or id. You can not specify both.")
  }

  req <- httr::POST(paste0(Rdatastore$url, ":lookup"),
                    httr::config(token = Rdatastore$token),
                    body = list(keys = list(path = lookup_q)),
                    encode = "json")

  resp <- jsonlite::fromJSON(httr::content(req, as = "text"))$found
  variables <- names(resp$entity$properties)
  values <- resp$entity$properties
  results <- resp$entity$properties

  # Convert Variable types and return as data frame.
  results <- format_from_results(results)
  dplyr::tbl_df(as.data.frame(results, stringsAsFactors = F))
}


#' commit
#'
#' Commit a single entity to the datastore.
#'
#' @param kind The entity kind
#' @param name The name of the entity. If NULL, id is autogenerated.
#' @param ... Additional arguments to store as properties.
#' @param mutation_type The type of mutation. One of \strong{insert}, \strong{update}, \strong{upsert}, or \strong{delete}. Default is upsert.
#'
#' @seealso \url{https://cloud.google.com/datastore/docs/concepts/entities} - Entities, Properties, and Keys
#'
#' @export


commit <- function(kind, name = NULL, ..., mutation_type = "upsert") {

  transaction_id <- transaction()

  # Generate mutation
  mutation = list()
  mutation[[mutation_type]] =  c(
      list(key = list(path = list(
        kind = kind,
        name = name
      )),
      properties = format_to_properties(list(...))
      )
  )


  body <- list(mutations = mutation,
               transaction = transaction_id
               )

  req <- httr::POST(paste0(Rdatastore$url, ":commit"),
                    httr::config(token = Rdatastore$token),
                    body =  body,
                    encode = "json")

  # Return transaction id if successful, else error.
  if (req$status_code == 200) {
    transaction_id
  } else {
    stop(paste0(content(req)$error$code, ": ", content(req)$error$message))
  }
}
