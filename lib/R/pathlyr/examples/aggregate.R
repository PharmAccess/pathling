library(sparklyr)
library(pathlyr)

sc <- spark_connect(master = "local")
pc <- ptl_connect(sc)

NDJSON_DIR_URI <- 'file:///Users/szu004/dev/pathling/library-api/src/test/resources/test-data/ndjson'

data_source <- pc %>% ds_from_ndjson_dir(NDJSON_DIR_URI)



data_source %>% aggregate('Patient',
              aggregations = c(patientCount='count()', 'id.count()'),
              groupings = c('gender', givenName='name.given')
        ) %>% show()

