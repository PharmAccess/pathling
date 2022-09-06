/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.sql.dates.time;

import java.time.LocalTime;
import java.util.function.BiFunction;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("core | unit-test")
public class TimeLessThanFunction extends TimeComparisonFunction {

  private static final long serialVersionUID = 5957315745114053261L;

  public static final String FUNCTION_NAME = "time_lt";

  @Override
  protected BiFunction<LocalTime, LocalTime, Boolean> getOperationFunction() {
    return LocalTime::isBefore;
  }

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

}
