library(sparkhello)
library(sparklyr)



ptl_create <- function(spark_connection) {
  invoke_static(sc, "au.csiro.pathling.library.PathlingContext", "create",
                spark_session(spark_connection))
}

sc <- spark_connect(master = "local")
pc <- ptl_create(sc)

print(pc)
ds <- invoke(pc, "datasources")

print(ds)


json_resources <- spark_read_text(sc, path='~/dev/pathling/library-api/src/test/resources/test-data/ndjson')
rds <-invoke(ds, "fromNdjsonDir", "file:///Users/szu004/dev/pathling/library-api/src/test/resources/test-data/ndjson")
q <-invoke(rds,"aggregate", 'Patient')
q <-invoke(q, "withAggregation", "count()", "patientCount")
q <-invoke(q, "withGrouping", "gender")

tbl <- sdf_register(invoke(q, "execute"))
print(tbl)
