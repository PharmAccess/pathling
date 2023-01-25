package au.csiro.pathling.query;

import au.csiro.pathling.extract.ExtractRequest;
import au.csiro.pathling.extract.ExtractRequest.Builder;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

import static java.util.Objects.requireNonNull;


public class ExtractQuery {

  
  @Nullable
  private PathlingClient pathlingClient  = null;
  
  
  @Nonnull
  private final ExtractRequest.Builder builder;

  private ExtractQuery(@Nonnull final Builder builder) {
    this.builder = builder;
  }


  @Nonnull
  public ExtractQuery withClient(@Nonnull final PathlingClient pathlingClient) {
    this.pathlingClient = pathlingClient;
    return this;
  }


  @Nonnull
  public ExtractQuery withLimit(int limit) {
    builder.withLimit(limit);
    return this;
  }
  
  @Nonnull
  public ExtractQuery withFilter(@Nonnull final String filterFhirpath) {
    builder.withFilter(filterFhirpath);
    return this;
  }

  @Nonnull
  public ExtractQuery withColumn(@Nonnull final String columnFhirpath) {
    builder.withColumn(columnFhirpath);
    return this;
  }
  
  @Nonnull
  public Dataset<Row> execute(){
    return requireNonNull(this.pathlingClient).execute(builder.build());
  }


  @Nonnull
  public Dataset<Row> execute(@Nonnull final PathlingClient pathlingClient) {
    return pathlingClient.execute(builder.build());
  }
  
  
  
  public static ExtractQuery of(@Nonnull final ResourceType resourceType) {
    return new ExtractQuery(ExtractRequest.builderFor(resourceType));
  }

}
