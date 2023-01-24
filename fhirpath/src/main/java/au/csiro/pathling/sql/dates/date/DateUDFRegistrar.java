package au.csiro.pathling.sql.dates.date;

import au.csiro.pathling.sql.AbstractUDFRegistrar;

public class DateUDFRegistrar extends AbstractUDFRegistrar {

  @Override
  protected void registerUDFs(final UDFRegistrar udfRegistrar) {
    udfRegistrar
        .register(new DateAddDurationFunction())
        .register(new DateSubtractDurationFunction());
  }
}
