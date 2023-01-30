package au.csiro.pathling.sql;

import au.csiro.pathling.spark.SparkConfigurer;
import au.csiro.pathling.sql.udf.SqlFunction1;
import au.csiro.pathling.sql.udf.SqlFunction2;
import au.csiro.pathling.sql.udf.SqlFunction3;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.UDFRegistration;
import javax.annotation.Nonnull;

public abstract class AbstractUDFRegistrar implements SparkConfigurer {

  public static class UDFRegistrar {

    private final UDFRegistration udfRegistration;

    public UDFRegistrar(@Nonnull SparkSession spark) {
      this.udfRegistration = spark.udf();
    }

    public UDFRegistrar register(@Nonnull SqlFunction1<?, ?> udf1) {
      udfRegistration.register(udf1.getName(), udf1, udf1.getReturnType());
      return this;
    }

    public UDFRegistrar register(@Nonnull SqlFunction2<?, ?, ?> udf2) {
      udfRegistration.register(udf2.getName(), udf2, udf2.getReturnType());
      return this;
    }

    public UDFRegistrar register(@Nonnull SqlFunction3<?, ?, ?, ?> udf3) {
      udfRegistration.register(udf3.getName(), udf3, udf3.getReturnType());
      return this;
    }
  }

  @Override
  public void configure(@Nonnull final SparkSession spark) {
    registerUDFs(new UDFRegistrar(spark));
  }

  abstract protected void registerUDFs(UDFRegistrar udfRegistrar);
}
