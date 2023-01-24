package au.csiro.pathling.api;

import au.csiro.pathling.DataSource;
import au.csiro.pathling.SimpleDataSource;
import au.csiro.pathling.config.QueryConfiguration;
import au.csiro.pathling.config.StorageConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.extract.ExtractQueryExecutor;
import au.csiro.pathling.extract.ExtractRequest;
import au.csiro.pathling.io.Database;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import ca.uhn.fhir.context.FhirContext;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Value;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;


@Value
public class PathlingClient {

  @Nonnull
  FhirContext fhirContext;
  @Nonnull
  SparkSession sparkSession;
  @Nonnull
  DataSource dataSource;
  @Nonnull
  QueryConfiguration configuration;
  @Nonnull
  Optional<TerminologyServiceFactory> terminologyClientFactory;

  public static class Builder {

    @Nonnull
    final FhirContext fhirContext;
    @Nonnull
    final SparkSession sparkSession;
    @Nonnull
    final TerminologyServiceFactory terminologyClientFactory;

    @Nonnull
    QueryConfiguration queryConfiguration = QueryConfiguration.builder().build();

    @Nullable
    SimpleDataSource.Builder simpleBuilder;

    @Nullable
    StorageConfiguration storageConfiguration;

    public Builder(@Nonnull final FhirContext fhirContext,
        @Nonnull final SparkSession sparkSession,
        @Nonnull final TerminologyServiceFactory terminologyClientFactory) {
      this.fhirContext = fhirContext;
      this.sparkSession = sparkSession;
      this.terminologyClientFactory = terminologyClientFactory;
    }

    @Nonnull
    public Builder withResource(@Nonnull final ResourceType resourceType,
        @Nonnull final Dataset<Row> dataset) {
      getSimpleBuilder().withResource(resourceType, dataset);
      return this;
    }

    @Nonnull
    public Builder withStorage(@Nonnull final StorageConfiguration storageConfiguration) {
      this.storageConfiguration = storageConfiguration;
      this.simpleBuilder = null;
      return this;
    }
    
    @Nonnull
    PathlingClient build() {
      return new PathlingClient(fhirContext, sparkSession, buildDataSource(), queryConfiguration,
          Optional.of(terminologyClientFactory));
    }

    @Nonnull
    private DataSource buildDataSource() {
      if (simpleBuilder != null) {
        return simpleBuilder.build();
      } else if (storageConfiguration != null) {
        return new Database(storageConfiguration, sparkSession, FhirEncoders.forR4().getOrCreate(),
            null);
      } else {
        throw new IllegalStateException("DataSource has not been defined");
      }
    }

    @Nonnull
    private SimpleDataSource.Builder getSimpleBuilder() {
      this.storageConfiguration = null;
      if (simpleBuilder == null) {
        simpleBuilder = new SimpleDataSource.Builder();
      }
      return simpleBuilder;
    }
  }
  
  @Nonnull
  public Dataset<Row> execute(@Nonnull final ExtractRequest extractRequest) {
    return new ExtractQueryExecutor(
        configuration,
        fhirContext,
        sparkSession,
        dataSource,
        terminologyClientFactory
    ).buildQuery(extractRequest);
  }
}
