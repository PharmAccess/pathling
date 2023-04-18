#
# This file is part of the Pathlyr package for R.
# Terminology functions
#

#' @export
SNOMED_URI = 'http://snomed.info/sct'


# TODO: see https://github.com/apache/spark/blob/master/R/pkg/R/functions.R
# for the genrics based implementation that could also support other types like:
# "characteOrColumn"


#' @export
snomed_code <-function(code) {
  struct(SparkR::lit(NULL), SparkR::lit(SNOMED_URI), SparkR::lit(NULL),
         SparkR::cast(code, 'string'), SparkR::lit(NULL), SparkR::lit(NULL))
}

#' @export
trm_member_of <-function(coding, value_set) {
  jc <- SparkR::sparkR.callJStatic("au.csiro.pathling.sql.Terminology",
                                   "member_of", coding@jc, value_set)
  SparkR:::column(jc)
}
