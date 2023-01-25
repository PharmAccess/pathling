/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.extract;

import static au.csiro.pathling.utilities.Preconditions.checkUserInput;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import au.csiro.pathling.utilities.Preconditions;
import lombok.Value;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Represents the information provided as part of an invocation of the "extract" operation.
 *
 * @author John Grimes
 */
@Value
public class ExtractRequest {

  @Value
  public static class ExpressionWithLabel {
    @Nonnull
    String expression;
    
    @Nullable
    String label;
  }
  
  @Nonnull
  ResourceType subjectResource;

  @Nonnull
  List<String> columns;

  @Nonnull
  List<String> filters;

  @Nonnull
  Optional<Integer> limit;

  public static class Builder {

    @Nonnull
    final ResourceType subjectResource;

    @Nonnull
    List<String> columns = new ArrayList<>();

    @Nonnull
    List<String> filters = new ArrayList<>();

    @Nonnull
    Optional<Integer> limit;

    public Builder(@Nonnull final ResourceType subjectResource) {
      this.subjectResource = subjectResource;
    }

    @Nonnull
    public Builder withLimit(int limit) {
      this.limit = Optional.of(limit);
      return this;
    }

    @Nonnull
    public Builder withFilter(@Nonnull final String filterFhirpath) {
      filters.add(Preconditions.checkNotBlank(filterFhirpath));
      return this;
    }

    @Nonnull
    public ExtractRequest build() {
      return new ExtractRequest(subjectResource,
          columns.isEmpty()
          ? Collections.emptyList()
          : columns,
          filters.isEmpty()
          ? Collections.emptyList()
          : filters,
          limit);
    }

    @Nonnull
    public Builder withColumn(@Nonnull final String columnFhirpath) {
      return withColumn(columnFhirpath, columnFhirpath);
    }

    @Nonnull
    public Builder withColumn(@Nonnull final String columnFhirpath, @Nonnull final String columnAlias) {
      columns.add(Preconditions.checkNotBlank(columnFhirpath));
      return this;
    }


  }

  /**
   * @param subjectResource the resource which will serve as the input context for each expression
   * @param columns a set of columns expressions to execute over the data
   * @param filters the criteria by which the data should be filtered
   * @param limit the maximum number of rows to return
   */
  public ExtractRequest(@Nonnull final ResourceType subjectResource,
      @Nonnull final Optional<List<String>> columns, @Nonnull final Optional<List<String>> filters,
      @Nonnull final Optional<Integer> limit) {
    this(subjectResource, columns.orElse(Collections.emptyList()),
        filters.orElse(Collections.emptyList()), limit);
  }

  public ExtractRequest(@Nonnull final ResourceType subjectResource,
      @Nonnull final List<String> columns, @Nonnull final List<String> filters,
      @Nonnull final Optional<Integer> limit) {
    checkUserInput(columns.size() > 0,
        "Query must have at least one column expression");
    checkUserInput(columns.stream().noneMatch(String::isBlank),
        "Column expression cannot be blank");
    checkUserInput(filters.stream().noneMatch(String::isBlank),
        "Filter expression cannot be blank");
    limit.ifPresent(l -> checkUserInput(l > 0, "Limit must be greater than zero"));
    this.subjectResource = subjectResource;
    this.columns = columns;
    this.filters = filters;
    this.limit = limit;
  }

  @Nonnull
  public static Builder builderFor(@Nonnull final ResourceType subjectResource) {
    return new Builder(subjectResource);
  }

}
