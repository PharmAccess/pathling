package au.csiro.pathling.sql.dates.datetime;

import au.csiro.pathling.sql.AbstractUDFRegistrar;
import au.csiro.pathling.sql.dates.date.DateAddDurationFunction;
import au.csiro.pathling.sql.dates.date.DateSubtractDurationFunction;

public class DateTimeUDFRegistrar extends AbstractUDFRegistrar {

  @Override
  protected void registerUDFs(final UDFRegistrar udfRegistrar) {
    udfRegistrar
        .register(new DateTimeAddDurationFunction())
        .register(new DateTimeSubtractDurationFunction())
        .register(new DateTimeEqualsFunction())
        .register(new DateTimeGreaterThanFunction())
        .register(new DateTimeGreaterThanOrEqualToFunction())
        .register(new DateTimeLessThanFunction())
        .register(new DateTimeLessThanOrEqualToFunction());
  }
}
