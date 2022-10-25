/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.sql.dates.datetime;

import java.util.function.BiFunction;
import org.hl7.fhir.r4.model.DateTimeType;
import org.hl7.fhir.r4.model.Quantity;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * Subtracts a duration from a datetime.
 *
 * @author John Grimes
 */
@Component
@Profile("core | unit-test")
public class DateTimeSubtractDurationFunction extends DateTimeArithmeticFunction {

  private static final long serialVersionUID = -5922228168177608861L;

  public static final String FUNCTION_NAME = "datetime_subtract_duration";

  @Override
  protected BiFunction<DateTimeType, Quantity, DateTimeType> getOperationFunction() {
    return this::performSubtraction;
  }

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }


}
