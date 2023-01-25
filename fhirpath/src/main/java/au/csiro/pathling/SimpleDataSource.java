package au.csiro.pathling;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;

public class SimpleDataSource implements DataSource {

  @Nonnull
  private final Map<ResourceType, Dataset<Row>> resourceMap;

  public static class Builder {

    private final Map<ResourceType, Dataset<Row>> resourceMap = new HashMap<>();

    @Nonnull
    public Builder withResource(@Nonnull final ResourceType resourceType,
        @Nonnull final Dataset<Row> dataset) {
      resourceMap.put(resourceType, dataset);
      return this;
    }

    @Nonnull
    public SimpleDataSource build() {
      return new SimpleDataSource(resourceMap);
    }
  }

  public SimpleDataSource(
      @Nonnull final Map<ResourceType, Dataset<Row>> resourceMap) {
    this.resourceMap = new HashMap<>(resourceMap);
  }

  @Nonnull
  @Override
  public Dataset<Row> read(@Nonnull final ResourceType resourceType) {
    return resourceMap.computeIfAbsent(resourceType, key -> {
      throw new IllegalStateException(
          String.format("Cannot find data for resource of type: %s", key));
    });
  }
}
