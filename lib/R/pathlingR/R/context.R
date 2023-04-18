

.pathlingEnv <- new.env()

#' @export
pathlingR.init <- function(max_nesting_level=3, enable_extensions=FALSE) {
#  encoders_config <-invoke_static(sc, "au.csiro.pathling.config.EncodingConfiguration", "builder") %>%
#    invoke("maxNestingLevel", as.integer(max_nesting_level)) %>%
#    invoke("enableExtensions", as.logical(enable_extensions)) %>%
#    invoke("build")

  jctx <- SparkR::sparkR.callJStatic("au.csiro.pathling.library.PathlingContext", "create",
                             SparkR:::getSparkSession())

  assign(".pathlingCtx", jctx, envir=.pathlingEnv)
}


getPathlingCtx <- function() {
  stopifnot(exists(".pathlingCtx", envir = .pathlingEnv))
  get(".pathlingCtx", envir = .pathlingEnv)
}
