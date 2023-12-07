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

import static au.csiro.pathling.utilities.Preconditions.checkPresent;
import static org.apache.spark.sql.functions.lit;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.Comparable;
import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.Materializable;
import au.csiro.pathling.fhirpath.Numeric;
import au.csiro.pathling.fhirpath.StringCoercible;
import au.csiro.pathling.fhirpath.column.ColumnCtx;
import au.csiro.pathling.fhirpath.definition.NodeDefinition;
import com.google.common.collect.ImmutableSet;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.LongType;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.IntegerType;
import org.hl7.fhir.r4.model.PositiveIntType;
import org.hl7.fhir.r4.model.PrimitiveType;
import org.hl7.fhir.r4.model.UnsignedIntType;

/**
 * Represents a FHIRPath expression which refers to an integer typed element.
 *
 * @author John Grimes
 */
public class IntegerCollection extends Collection implements
    Materializable<PrimitiveType<?>>, Comparable, Numeric, StringCoercible {

  private static final ImmutableSet<Class<? extends Comparable>> COMPARABLE_TYPES = ImmutableSet
      .of(IntegerCollection.class, DecimalCollection.class);

  protected IntegerCollection(@Nonnull final ColumnCtx columnCtx,
      @Nonnull final Optional<FhirPathType> type,
      @Nonnull final Optional<FHIRDefinedType> fhirType,
      @Nonnull final Optional<? extends NodeDefinition> definition) {
    super(columnCtx, type, fhirType, definition);
  }

  /**
   * Returns a new instance with the specified columnCtx and definition.
   *
   * @param columnCtx The columnCtx to use
   * @param definition The definition to use
   * @return A new instance of {@link IntegerCollection}
   */
  @Nonnull
  public static IntegerCollection build(@Nonnull final ColumnCtx columnCtx,
      @Nonnull final Optional<NodeDefinition> definition) {
    return new IntegerCollection(columnCtx, Optional.of(FhirPathType.INTEGER),
        Optional.of(FHIRDefinedType.INTEGER), definition);
  }

  @Nonnull
  public static IntegerCollection build(final ColumnCtx columnCtx) {
    return build(columnCtx, Optional.empty());
  }


  @Nonnull
  public static IntegerCollection fromValue(final int value) {
    return IntegerCollection.build(ColumnCtx.literal(value));
  }


  /**
   * Returns a new instance, parsed from a FHIRPath literal.
   *
   * @param integerLiteral The FHIRPath representation of the literal
   * @return A new instance of {@link IntegerCollection}
   * @throws NumberFormatException if the literal is malformed
   */
  @Nonnull
  public static IntegerCollection fromLiteral(@Nonnull final String integerLiteral)
      throws NumberFormatException {
    return IntegerCollection.fromValue(Integer.parseInt(integerLiteral));
  }

  @Nonnull
  @Override
  public Optional<PrimitiveType<?>> getFhirValueFromRow(@Nonnull final Row row,
      final int columnNumber) {
    if (row.isNullAt(columnNumber)) {
      return Optional.empty();
    }
    final int value;
    if (row.schema().fields()[columnNumber].dataType() instanceof LongType) {
      try {
        // Currently, some functions such as count currently return an Integer type, even though
        // their return values can theoretically exceed the maximum value permitted for an integer.
        // This guard allows us to handle this situation in a safe way. In the future, we will
        // implement the "as" operator to allow the user to explicitly use a Decimal where large
        // values are possible.
        value = Math.toIntExact(row.getLong(columnNumber));
      } catch (final ArithmeticException e) {
        throw new InvalidUserInputError(
            "Attempt to return an Integer value greater than the maximum value permitted for this type");
      }
    } else {
      value = row.getInt(columnNumber);
    }
    switch (getFhirType().orElse(FHIRDefinedType.NULL)) {
      case UNSIGNEDINT:
        return Optional.of(new UnsignedIntType(value));
      case POSITIVEINT:
        return Optional.of(new PositiveIntType(value));
      default:
        return Optional.of(new IntegerType(value));
    }
  }

  @Nonnull
  public static ImmutableSet<Class<? extends Comparable>> getComparableTypes() {
    return COMPARABLE_TYPES;
  }

  @Override
  public boolean isComparableTo(@Nonnull final Collection path) {
    return COMPARABLE_TYPES.contains(path.getClass());
  }

  @Nonnull
  @Override
  public Function<Numeric, Collection> getMathOperation(@Nonnull final MathOperation operation) {
    return buildMathOperation(this, operation);
  }

  @Nonnull
  @Override
  public Optional<Column> getNumericValueColumn() {
    return Optional.ofNullable(this.getColumnCtx().cast(DataTypes.LongType).getValue());
  }

  @Nonnull
  @Override
  public Optional<Column> getNumericContextColumn() {
    return getNumericValueColumn();
  }

  /**
   * Builds a math operation result for a collection of Integers.
   *
   * @param source The left operand for the operation
   * @param operation The type of {@link au.csiro.pathling.fhirpath.Numeric.MathOperation}
   * @return A {@link Function} that takes a {@link Numeric} as a parameter, and returns a
   * {@link Collection}
   */
  @Nonnull
  public static Function<Numeric, Collection> buildMathOperation(@Nonnull final Numeric source,
      @Nonnull final MathOperation operation) {
    return target -> {
      final Column sourceNumeric = checkPresent(source.getNumericValueColumn());
      final Column targetNumeric = checkPresent(target.getNumericValueColumn());
      Column valueColumn = operation.getSparkFunction().apply(sourceNumeric, targetNumeric);

      switch (operation) {
        case ADDITION:
        case SUBTRACTION:
        case MULTIPLICATION:
        case MODULUS:
          if (target instanceof DecimalCollection) {
            valueColumn = valueColumn.cast(DataTypes.LongType);
          }
          return IntegerCollection.build(ColumnCtx.of(valueColumn));
        case DIVISION:
          final Column numerator = source.getColumnCtx().cast(DecimalCollection.getDecimalType())
              .getValue();
          valueColumn = operation.getSparkFunction().apply(numerator, targetNumeric);
          return DecimalCollection.build(ColumnCtx.of(valueColumn));
        default:
          throw new AssertionError("Unsupported math operation encountered: " + operation);
      }
    };
  }

  @Override
  @Nonnull
  public StringCollection asStringPath() {
    return map(ColumnCtx::asString, StringCollection::build);
  }

}
