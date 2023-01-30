package au.csiro.pathling.sql.misc;

import au.csiro.pathling.sql.AbstractUDFRegistrar;
import au.csiro.pathling.sql.dates.datetime.*;

public class MiscUDFRegistrar extends AbstractUDFRegistrar {

  @Override
  protected void registerUDFs(final UDFRegistrar udfRegistrar) {
    udfRegistrar
        .register(new CodingToLiteral())
        .register(new TemporalDifferenceFunction());
  }
}
