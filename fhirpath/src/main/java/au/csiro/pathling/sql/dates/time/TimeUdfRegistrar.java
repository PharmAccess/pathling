package au.csiro.pathling.sql.dates.time;

import au.csiro.pathling.sql.AbstractUDFRegistrar;

public class TimeUdfRegistrar extends AbstractUDFRegistrar {

  protected void registerUDFs(UDFRegistrar udfRegistrar) {
    udfRegistrar
        .register(new TimeEqualsFunction())
        .register(new TimeGreaterThanFunction())
        .register(new TimeGreaterThanOrEqualToFunction())
        .register(new TimeLessThanFunction())
        .register(new TimeLessThanOrEqualToFunction());
  }
}
