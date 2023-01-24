package au.csiro.pathling.api;

import au.csiro.pathling.DataSource;
import au.csiro.pathling.config.EncodingConfiguration;
import au.csiro.pathling.config.StorageConfiguration;
import au.csiro.pathling.config.TerminologyConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.io.Database;
import au.csiro.pathling.terminology.DefaultTerminologyServiceFactory;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import lombok.Value;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;

@Value
public class ParserFactory {

  @Nonnull
  SparkSession spark;
  @Nonnull
  FhirContext fhirContext;
  @Nonnull
  TerminologyServiceFactory terminologyServiceFactory;
  @Nonnull
  DataSource dataSource;

  public static class Builder {

    @Nullable
    SparkSession spark;

    @Nullable
    private EncodingConfiguration encodingConfiguration;
    @Nullable
    private StorageConfiguration storageConfiguration;
    @Nullable
    private TerminologyConfiguration terminologyConfiguration;

    @Nullable
    private FhirEncoders fhirEncoders;
    @Nullable
    private DataSource dataSource;
    @Nullable
    private TerminologyServiceFactory terminologyServiceFactory;
    
    @Nonnull
    public Builder withSpark(@Nonnull final  SparkSession sparkSession) {
      this.spark = spark;
      return  this;
    }
    @Nonnull
    public Builder withEncoders(@Nonnull final FhirEncoders fhirEncoders) {
      this.fhirEncoders = fhirEncoders;
      this.encodingConfiguration = null;
      return this;
    }

    @Nonnull
    public Builder withEncodingConfiguration(
        @Nonnull final EncodingConfiguration encodingConfiguration) {
      this.fhirEncoders = null;
      this.encodingConfiguration = encodingConfiguration;
      return this;
    }

    @Nonnull
    public Builder withDataSource(
        @Nonnull final DataSource dataSource) {
      this.dataSource = dataSource;
      this.storageConfiguration = null;
      return this;
    }

    @Nonnull
    public Builder withStorageConfiguration(
        @Nonnull final StorageConfiguration storageConfiguration) {
      this.dataSource = null;
      this.storageConfiguration = storageConfiguration;
      return this;
    }

    @Nonnull
    public Builder withTerminologyServiceFactory(
        @Nonnull final TerminologyServiceFactory terminologyServiceFactory) {
      this.terminologyServiceFactory = terminologyServiceFactory;
      this.terminologyConfiguration = null;
      return this;
    }

    @Nonnull
    public Builder withTerminologyConfiguration(
        @Nonnull final TerminologyConfiguration terminologyConfiguration) {
      this.terminologyServiceFactory = null;
      this.terminologyConfiguration = terminologyConfiguration;
      return this;
    }


    private void ensureDependencies() {

      if (isNull(spark)) {
        spark = SparkSession.active();
      }

      if (isNull(fhirEncoders)) {
        fhirEncoders = FhirEncoders.forR4().withConfiguration(requireNonNull(encodingConfiguration))
            .getOrCreate();
      }

      if (isNull(terminologyServiceFactory)) {
        new DefaultTerminologyServiceFactory(fhirEncoders.getFhirVersion(),
            requireNonNull(terminologyConfiguration));
      }

      if (isNull(dataSource)) {
        dataSource = new Database(requireNonNull(storageConfiguration), requireNonNull(spark),
            requireNonNull(fhirEncoders), null);
      }
    }

    @Nonnull
    public ParserFactory build() {
      ensureDependencies();
      final FhirContext fhirContext = fhirEncoders.getContext();
      return new ParserFactory(spark, fhirContext, terminologyServiceFactory, dataSource);
    }


  }
  
  @Nonnull
  public static Builder custom() {
    return new Builder();
  }


  @Nonnull
  public Parser create(@Nonnull final FhirPath inputContext,
      @Nonnull final List<Column> groupingColumns) {
    final ParserContext parserContext = new ParserContext(inputContext, fhirContext, spark,
        dataSource, Optional.of(terminologyServiceFactory), groupingColumns, new HashMap<>());
    return new Parser(parserContext);
  }

  @Nonnull
  public Parser create(@Nonnull final ResourcePath resourcePath) {
    return create(resourcePath, Collections.singletonList(resourcePath.getIdColumn()));
  }

  @Nonnull
  public Parser create(@Nonnull final ResourceType resourceType) {
    final ResourcePath subjectResource = ResourcePath
        .build(fhirContext, dataSource, resourceType, resourceType.toCode(), true);
    return create(subjectResource);
  }
}
