package au.csiro.pathling.api;

import au.csiro.pathling.extract.ExtractQueryExecutor;
import au.csiro.pathling.extract.ExtractRequest;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;

public class ExtractQuery {

  @Nonnull 
  final ExtractQueryExecutor executor;
  
  @Nonnull
  ResourceType subjectResource;

  @Nonnull
  List<String> columns;

  @Nonnull
  List<String> filters;

  @Nonnull
  Optional<Integer> limit;

  private ExtractQuery(@Nonnull final ExtractQueryExecutor executor) {
    this.executor = executor;
  }
  
  @Nonnull
  public ExtractQuery withFilter(@Nonnull final String filter) {
    this.filters.add(filter);
    return this;
  }

  @Nonnull
  public ExtractQuery withColum(@Nonnull final String column, @Nonnull final String alias) {
    columns.add(column);
    return this;
  }
  
  @Nonnull
  public Dataset<Row> execute() {
    return executor.buildQuery(new ExtractRequest(subjectResource, Optional.of(columns), Optional.of(filters), limit));
  }
    
}
