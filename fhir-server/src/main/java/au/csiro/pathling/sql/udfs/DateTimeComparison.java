package au.csiro.pathling.sql.udfs;

import org.hl7.fhir.r4.model.DateTimeType;

/**
 * The stronly typed version of the operartion
 */
public class DateTimeComparison implements Comparison<String> {

  @Override
  public Boolean lessThan(final String left, final String right) {
    return new DateTimeType(left).before(new DateTimeType(right));
  }
}
