/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.comparison;

import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.fhirpath.Comparable;
import au.csiro.pathling.fhirpath.Comparable.ComparisonOperation;
import au.csiro.pathling.fhirpath.Comparable.SqlComparator;
import au.csiro.pathling.fhirpath.encoding.QuantityEncoding;
import au.csiro.pathling.sql.types.FlexiDecimal;
import java.util.function.BiFunction;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;

/**
 * Implementation of comparator for the Quantity type. It uses canonicalized values and units for
 * comparison rather than the original values.
 *
 * @author Piotr Szul
 */
public class QuantitySqlComparator implements SqlComparator {

  private final static QuantitySqlComparator INSTANCE = new QuantitySqlComparator();

  public QuantitySqlComparator() {
  }

  private static BiFunction<Column, Column, Column> wrap(
      @Nonnull final BiFunction<Column, Column, Column> function) {

    return (left, right) -> {
      final Column sourceCode = left.getField(
          QuantityEncoding.CANONICALIZED_CODE_COLUMN);
      final Column targetCode = right.getField(
          QuantityEncoding.CANONICALIZED_CODE_COLUMN);
      final Column sourceValue = left.getField(
          QuantityEncoding.CANONICALIZED_VALUE_COLUMN);
      final Column targetValue = right.getField(
          QuantityEncoding.CANONICALIZED_VALUE_COLUMN);
      return when(sourceCode.equalTo(targetCode),
          function.apply(sourceValue, targetValue)).otherwise(
          null);
    };
  }

  @Override
  public Column equalsTo(@Nonnull final Column left, @Nonnull final Column right) {
    return wrap(FlexiDecimal::equals).apply(left, right);
  }

  @Override
  public Column lessThan(@Nonnull final Column left, @Nonnull final Column right) {
    return wrap(FlexiDecimal::lt).apply(left, right);
  }

  @Override
  public Column lessThanOrEqual(@Nonnull final Column left, @Nonnull final Column right) {
    return wrap(FlexiDecimal::lte).apply(left, right);
  }

  @Override
  public Column greaterThan(@Nonnull final Column left, @Nonnull final Column right) {
    return wrap(FlexiDecimal::gt).apply(left, right);
  }

  @Override
  public Column greaterThanOrEqual(@Nonnull final Column left, @Nonnull final Column right) {
    return wrap(FlexiDecimal::gte).apply(left, right);
  }

  /**
   * Builds a comparison function for quantity like paths.
   *
   * @param source the path to build the comparison function for
   * @param operation the {@link ComparisonOperation} that should be built
   * @return a new {@link Function}
   */
  @Nonnull
  public static Function<Comparable, Column> buildComparison(@Nonnull final Comparable source,
      @Nonnull final ComparisonOperation operation) {
    return Comparable.buildComparison(source, operation, INSTANCE);
  }
}
