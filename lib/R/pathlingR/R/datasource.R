getDataSources <- function() {
  SparkR::sparkR.callJMethod(getPathlingCtx(), 'datasources')
}

#'@export

read_ndjson <- function(ndjson_dir_uri) {
  SparkR::sparkR.callJMethod(getDataSources(), 'fromNdjsonDir', ndjson_dir_uri)
}
