Sys.setenv(SPARK_HOME = "/Users/szu004/spark/spark-3.3.1-bin-hadoop2")
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))

library(pathlingR)

sparkR.session(master='local[*]', sparkJars=pathlingR.find_jar())
pathlingR.init()

NDJSON_DIR_URI <- paste0('file://', system.file('data','ndjson', package='pathlyr'))

print(sprintf('Using ndjson resources from: %s', NDJSON_DIR_URI))

data_source <- read_ndjson(NDJSON_DIR_URI)
result_df <- aggregate(data_source, 'Patient',
              aggregations = c(patientCount='count()', idCount='id.count()'),
              groupings = c('gender', givenName='name.given')
        )


print(head(result_df))
