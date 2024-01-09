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

package au.csiro.pathling.view;

import static au.csiro.pathling.extract.ExtractResultType.FLAT;
import static au.csiro.pathling.utilities.Functions.maybeCast;

import au.csiro.pathling.extract.ExtractResultType;
import au.csiro.pathling.fhirpath.StringCoercible;
import au.csiro.pathling.fhirpath.collection.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

@Value
@AllArgsConstructor
public class ExtractView {

  @Nonnull
  ResourceType subjectResource;
  @Nonnull
  List<Selection> selections;
  @Nonnull
  Optional<Selection> where;
  @Nonnull
  ExtractResultType resultType;


  public ExtractView(@Nonnull final ResourceType subjectResource,
      @Nonnull final Selection selection, @Nonnull final Optional<Selection> where) {
    this(subjectResource, selection, where, ExtractResultType.UNCONSTRAINED);
  }

  public ExtractView(@Nonnull final ResourceType subjectResource,
      @Nonnull final Selection selection, @Nonnull final Optional<Selection> where,
      @Nonnull final ExtractResultType resultType) {
    this(subjectResource, List.of(selection), where, resultType);
  }

  public ExtractView(@Nonnull final ResourceType subjectResource,
      @Nonnull final List<Selection> selections, @Nonnull final Optional<Selection> where) {
    this(subjectResource, selections, where, ExtractResultType.UNCONSTRAINED);
  }

  @Nonnull
  public Selection getSelection() {
    return selections.get(0);
  }


  public Dataset<Row> evaluate(@Nonnull final ExecutionContext context) {
    final DefaultProjectionContext projectionContext = DefaultProjectionContext.of(context,
        subjectResource);

    final Stream<DatasetResult<Column>> selectionResults = selections.stream()
        .map(s -> s.evaluate(projectionContext).map(this::toColumn));

    final Optional<DatasetResult<Column>> filterResult = where.map(projectionContext::evaluate)
        .map(dr -> dr.toFilter(cr -> cr.getCollection().asSingular().getCtx().getValue()));

    return selectionResults
        .map(sr -> filterResult.map(sr::andThen).orElse(sr)
            .select(projectionContext.getDataset(), Function.identity()))
        .reduce(Dataset::union).orElseThrow();
  }

  public void printTree() {
    if (selections.size() > 1) {
      System.out.println("union:");
      selections.forEach(sl -> {
        System.out.println("  selection:");
        sl.toTreeString()
            .forEach(s -> System.out.println("     " + s));
      });
    } else {
      System.out.println("select:");
      getSelection().toTreeString()
          .forEach(s -> System.out.println("  " + s));    
    }
    where.ifPresent(w -> {
      System.out.println("where:");
      w.toTreeString()
          .forEach(s -> System.out.println("  " + s));
    });
  }


  @Nonnull
  private Column toColumn(@Nonnull final CollectionResult result) {
    final Collection collection = result.getCollection();
    final PrimitiveSelection info = result.getSelection();

    final Collection finalResult = FLAT.equals(resultType)
                                   ? Optional.of(collection)
                                       .flatMap(maybeCast(StringCoercible.class))
                                       .map(StringCoercible::asStringPath).orElseThrow()
                                   : collection;

    final Column columnResult = info.isAsCollection()
                                ? finalResult.getColumnCtx().getValue()
                                : finalResult.asSingular().getColumnCtx().getValue();
    return info.getAlias().map(columnResult::alias).orElse(columnResult);
  }
}
