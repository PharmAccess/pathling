package au.csiro.pathling.sql;

import au.csiro.pathling.spark.SparkConfigurer;
import au.csiro.pathling.sql.udf.SqlFunction2;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.UDFRegistration;
import javax.annotation.Nonnull;

public abstract class AbstractUDFRegistrar implements SparkConfigurer {

  public static class UDFRegistrar {

    private final UDFRegistration udfRegistration;

    public UDFRegistrar(@Nonnull SparkSession spark) {
      this.udfRegistration = spark.udf();
    }

    public UDFRegistrar register(@Nonnull SqlFunction2<?, ?, ?> udf2) {
      udfRegistration.register(udf2.getName(), udf2, udf2.getReturnType());
      return this;
    }
  }

  @Override
  public void configure(@Nonnull final SparkSession spark) {
    registerUDFs(new UDFRegistrar(spark));
  }

  abstract protected void registerUDFs(UDFRegistrar udfRegistrar);
}
