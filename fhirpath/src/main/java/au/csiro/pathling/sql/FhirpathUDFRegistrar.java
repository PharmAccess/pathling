package au.csiro.pathling.sql;

import au.csiro.pathling.spark.SparkConfigurer;
import au.csiro.pathling.sql.dates.date.DateUDFRegistrar;
import au.csiro.pathling.sql.dates.datetime.DateTimeUDFRegistrar;
import au.csiro.pathling.sql.dates.time.TimeUdfRegistrar;
import org.apache.spark.sql.SparkSession;
import javax.annotation.Nonnull;
import java.util.List;

public class FhirpathUDFRegistrar implements SparkConfigurer {

  private final List<SparkConfigurer> children = List.of(
      new DateUDFRegistrar(), new TimeUdfRegistrar(), new DateTimeUDFRegistrar()
  );

  @Override
  public void configure(@Nonnull final SparkSession spark) {
    children.forEach(child -> child.configure(spark));
  }

  public static void registerUDFs(@Nonnull final SparkSession spark) {
    new FhirpathUDFRegistrar().configure(spark);
  }
}
