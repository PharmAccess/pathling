library(sparklyr)
library(sparkhello)



ptl_create <- function(spark_connection) {
  invoke_static(sc, "au.csiro.pathling.library.PathlingContext", "create",
                spark_session(spark_connection))
}

sc <- spark_connect(master = "local")
pc <- ptl_create(sc)


data_sdf <- spark_read_csv(sc, '~/dev/pathling/lib/python/examples/data/csv/conditions.csv', header = TRUE)


SNOMED_URI = 'http://snomed.info/sct'

snomed_code <-function(code, UNUSED=NULL) {
  if (is.null(UNUSED) ) {
    expr(struct(NULL, SNOMED_URI, NULL, string({{code}}), NULL, NULL))
  } else {
    expr(struct(NULL, SNOMED_URI, NULL, string({{code}}), "", NULL))
  }
}


trm_member_of <-function(coding, value_set) {
  expr(member_of({{coding}}, {{value_set}}))
}

#rs <- data_sdf %>% mutate(
#  READ_CODE = member_of(struct(id=NULL, system='http://snomed.info/sct', version=NULL, as.character(CODE), NULL, NULL),
#                        'http://snomed.info/sct?fhir_vs=ecl/%20%20%20%20%3C%3C%2064572001%7CDisease%7C%20%3A%20(%0A%20%20%20%20%20%20%3C%3C%20370135005%7CPathological%20process%7C%20%3D%20%3C%3C%20441862004%7CInfectious%20process%7C%2C%0A%20%20%20%20%20%20%3C%3C%20246075003%7CCausative%20agent%7C%20%3D%20%3C%3C%2049872002%7CVirus%7C%0A%20%20%20%20)'))


TEST_VALUE_SET='http://snomed.info/sct?fhir_vs=ecl/%20%20%20%20%3C%3C%2064572001%7CDisease%7C%20%3A%20(%0A%20%20%20%20%20%20%3C%3C%20370135005%7CPathological%20process%7C%20%3D%20%3C%3C%20441862004%7CInfectious%20process%7C%2C%0A%20%20%20%20%20%20%3C%3C%20246075003%7CCausative%20agent%7C%20%3D%20%3C%3C%2049872002%7CVirus%7C%0A%20%20%20%20)'


# call the UDF directly
rs <- data_sdf %>% mutate(
  READ_CODE = member_of(!!snomed_code(CODE), TEST_VALUE_SET))
rs %>% show()


# call the UDF via an R wrapper function (note the !!)
rs <- data_sdf %>% mutate(
  READ_CODE = !!trm_member_of(!!snomed_code(CODE), TEST_VALUE_SET))

rs %>% show()
