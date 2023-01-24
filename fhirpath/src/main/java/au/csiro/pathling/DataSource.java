package au.csiro.pathling;

import javax.annotation.Nonnull;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

public interface DataSource {

  @Nonnull
  Dataset<Row> read(@Nonnull final ResourceType resourceType);
}
