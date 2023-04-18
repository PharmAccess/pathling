

#'@export
encode_resource <-function(text_df, resource_type) {
  sdf <- SparkR::sparkR.callJMethod(getPathlingCtx(), 'encode', text_df@sdf, resource_type)
  SparkR:::dataFrame(sdf)
}
