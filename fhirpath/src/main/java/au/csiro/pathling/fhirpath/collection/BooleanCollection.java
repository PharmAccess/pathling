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

package au.csiro.pathling.fhirpath.collection;

import au.csiro.pathling.fhirpath.Comparable;
import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.Materializable;
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import au.csiro.pathling.fhirpath.definition.NodeDefinition;
import com.google.common.collect.ImmutableSet;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Represents a collection of Boolean-typed elements.
 *
 * @author John Grimes
 */
public class BooleanCollection extends Collection implements Materializable<BooleanType>,
    Comparable {

  private static final ImmutableSet<Class<? extends Comparable>> COMPARABLE_TYPES = ImmutableSet
      .of(BooleanCollection.class);
  private static final BooleanCollection FALSE_COLLECTION = BooleanCollection.fromValue(false);

  protected BooleanCollection(@Nonnull final ColumnRepresentation columnRepresentation,
      @Nonnull final Optional<FhirPathType> type,
      @Nonnull final Optional<FHIRDefinedType> fhirType,
      @Nonnull final Optional<? extends NodeDefinition> definition) {
    super(columnRepresentation, type, fhirType, definition);
  }

  /**
   * Returns a new instance with the specified column and definition.
   *
   * @param columnRepresentation The column to use
   * @param definition The definition to use
   * @return A new instance of {@link BooleanCollection}
   */
  @Nonnull
  public static BooleanCollection build(@Nonnull final ColumnRepresentation columnRepresentation,
      @Nonnull final Optional<NodeDefinition> definition) {
    return new BooleanCollection(columnRepresentation, Optional.of(FhirPathType.BOOLEAN),
        Optional.of(FHIRDefinedType.BOOLEAN), definition);
  }

  @Nonnull
  public static BooleanCollection build(@Nonnull final ColumnRepresentation value) {
    return BooleanCollection.build(value, Optional.empty());
  }

  /**
   * Returns a new instance, parsed from a FHIRPath literal.
   *
   * @param literal The FHIRPath representation of the literal
   * @return A new instance of {@link BooleanCollection}
   */
  @Nonnull
  public static BooleanCollection fromLiteral(@Nonnull final String literal) {
    return BooleanCollection.fromValue(literal.equals("true"));
  }

  @Nonnull
  public static BooleanCollection fromValue(final boolean value) {
    return BooleanCollection.build(ColumnRepresentation.literal(value));
  }

  @Nonnull
  public static BooleanCollection falseCollection() {
    return FALSE_COLLECTION;
  }

  @Nonnull
  @Override
  public Optional<BooleanType> getFhirValueFromRow(@Nonnull final Row row, final int columnNumber) {
    if (row.isNullAt(columnNumber)) {
      return Optional.empty();
    }
    return Optional.of(new BooleanType(row.getBoolean(columnNumber)));
  }

  @Override
  public boolean isComparableTo(@Nonnull final Collection path) {
    return COMPARABLE_TYPES.contains(path.getClass());
  }
}
