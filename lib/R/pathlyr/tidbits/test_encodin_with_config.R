library(sparkhello)
library(sparklyr)


ptl_create <- function(sc, max_nesting_level=2, enable_extensions=FALSE) {


  invoke <- sparklyr::invoke

  open_types <- invoke_static(sc, 'java.util.Set', 'of', jarray(sc, c("integer", "boolean", "string"), 'java.lang.String'))
  print(open_types)

  # Build an encoders configuration object from the provided parameters.
  encoders_config <-invoke_static(sc, "au.csiro.pathling.config.EncodingConfiguration", "builder") %>%
    invoke("maxNestingLevel", as.integer(max_nesting_level)) %>%
    invoke("enableExtensions", as.logical(enable_extensions)) %>%
    invoke("openTypes",open_types) %>%
    invoke("build")

  print(encoders_config)

  invoke_static(sc, "au.csiro.pathling.library.PathlingContext", "create",
                spark_session(sc), encoders_config)
}

sc <- spark_connect(master = "local")

pc <- ptl_create(sc)
print(pc)

json_resources <- spark_read_text(sc, path='~/dev/pathling/library-api/src/test/resources/test-data/ndjson')
condition_sdf <- sdf_register(invoke(pc, 'encode', spark_dataframe(json_resources), 'Condition'))
condition_sdf %>% show()


