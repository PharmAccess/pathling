/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.sql.dates.datetime;

import java.util.function.BiFunction;
import javax.annotation.Nonnull;
import org.hl7.fhir.r4.model.BaseDateTimeType;
import org.hl7.fhir.r4.model.DateTimeType;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * Determines whether one datetime is before another.
 *
 * @author John Grimes
 */
@Component
@Profile("core | unit-test")
public class DateTimeLessThanFunction extends DateTimeComparisonFunction {

  private static final long serialVersionUID = -4688679934965980217L;

  public static final String FUNCTION_NAME = "datetime_lt";

  @Nonnull
  @Override
  protected BiFunction<DateTimeType, DateTimeType, Boolean> getOperationFunction() {
    return BaseDateTimeType::before;
  }

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

}
