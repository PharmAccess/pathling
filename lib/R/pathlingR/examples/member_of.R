Sys.setenv(SPARK_HOME = "/Users/szu004/spark/spark-3.3.1-bin-hadoop2")
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))

library(pathlingR)

sparkR.session(master='local[*]', sparkJars=pathlingR.find_jar())
pathlingR.init()


data_df <- read.df(system.file('data', 'csv', 'conditions.csv', package='pathlingR'),"csv", header = TRUE)

print(head(data_df))

TEST_VALUE_SET='http://snomed.info/sct?fhir_vs=ecl/%20%20%20%20%3C%3C%2064572001%7CDisease%7C%20%3A%20(%0A%20%20%20%20%20%20%3C%3C%20370135005%7CPathological%20process%7C%20%3D%20%3C%3C%20441862004%7CInfectious%20process%7C%2C%0A%20%20%20%20%20%20%3C%3C%20246075003%7CCausative%20agent%7C%20%3D%20%3C%3C%2049872002%7CVirus%7C%0A%20%20%20%20)'


result_df <- withColumn(data_df, 'IS_MEMBER',
                        trm_member_of(snomed_code(data_df$CODE),TEST_VALUE_SET))


print(head(result_df))



