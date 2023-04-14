library(sparkhello)
library(sparklyr)


ptl_create <- function(spark_connection) {
  invoke_static(sc, "au.csiro.pathling.library.PathlingContext", "create",
                spark_session(spark_connection))
}


ptl_sanitize <-function(sdf) {
  sdf %>% select(-`_extension`)
}

sc <- spark_connect(master = "local")
pc <- ptl_create(sc)

print(pc)
ds <- invoke(pc, "datasources")

print(ds)


json_resources <- spark_read_text(sc, path='~/dev/pathling/library-api/src/test/resources/test-data/ndjson')
patient_sdf <- spark_read_parquet(sc, path='~/dev/pathling/library-api/src/test/resources/test-data/delta/Patient.parquet/') %>% ptl_sanitize()

raw_condition_sdf <- sdf_register(invoke(pc, 'encode', spark_dataframe(json_resources), 'Condition'))
condition_sdf <- raw_condition_sdf %>% ptl_sanitize()

patient_sdf %>% show()
condition_sdf %>% show()


spark_write_json(condition_sdf, '/tmp/condition.json', mode = 'overwrite')
spark_write_json(condition_sdf, '/tmp/raw_condition.json', mode = 'overwrite')


