
Rdatastore <- new.env()

#' Authenticate Datastore
#'
#' Authenticate datastore. Create an application on the
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

  datastore_url <- "https://www.googleapis.com/auth/datastore"
  # Fetch token
  google_token <- httr::oauth2.0_token(httr::oauth_endpoints("google"),
                                       app,
                                       scope = datastore_url)

  # Create global variable for project id
  assign("project_id", project, envir=Rdatastore)
  assign("token", google_token, envir=Rdatastore)
}

if (file.exists("credentials.json")) {
  credentials <- jsonlite::fromJSON("credentials.json")
  authenticate_datastore(key = credentials$key,
                         secret = credentials$secret,
                         project = credentials$project)
}



#' Lookup
#'
#' @param kind The entity kind
#' @param name The name or id
#' @param project Google cloud platform project id/name.
#'
#' @seealso \url{https://cloud.google.com/datastore/docs/concepts/entities} - Entities, Properties, and Keys
#'
#' @export


list_to_df <- function(listfordf){
  if(!is.list(named.list)) stop("it should be a list")

  df <- list(list.element = listfordf)
  class(df) <- c("tbl_df", "data.frame")
  attr(df, "row.names") <- .set_row_names(length(listfordf))

  if (!is.null(names(listfordf))) {
    df$name <- names(listfordf)
  }

  df
}

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

  url <- paste0("https://datastore.googleapis.com/v1beta3/projects/", Rdatastore$project_id, ":lookup")
  req <- httr::POST(url,
                    httr::config(token = Rdatastore$token),
                    body = list(keys = list(path = lookup_q)),
                    encode = "json")

  resp <- jsonlite::fromJSON(httr::content(req, as = "text"))$found
  variables <- names(resp$entity$properties)
  values <- resp$entity$properties
  results <- resp$entity$properties

  # Convert Variable types and return as data frame.
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
  dplyr::tbl_df(as.data.frame(properties, stringsAsFactors = F))
}
