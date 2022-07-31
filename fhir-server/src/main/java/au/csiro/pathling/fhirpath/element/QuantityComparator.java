package au.csiro.pathling.fhirpath.element;

import au.csiro.pathling.fhirpath.Comparable.Comparator;
import au.csiro.pathling.fhirpath.encoding.QuantityEncoding;
import org.apache.commons.lang3.function.TriFunction;
import org.apache.spark.sql.Column;

import java.util.function.BiFunction;

import static au.csiro.pathling.fhirpath.Comparable.STD_COMPARATOR;
import static org.apache.spark.sql.functions.when;

public class QuantityComparator implements Comparator {

  public final static QuantityComparator INSTANCE = new QuantityComparator(STD_COMPARATOR);

  private final Comparator defaultComparator;

  public QuantityComparator(final Comparator defaultComparator) {
    this.defaultComparator = defaultComparator;
  }

  private BiFunction<Column, Column, Column> wrap(
      final BiFunction<Column, Column, Column> function) {

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
  public Column equalsTo(final Column left, final Column right) {
    return wrap(defaultComparator::equalsTo).apply(left, right);
  }

  @Override
  public Column lessThan(final Column left, final Column right) {
    return wrap(defaultComparator::lessThan).apply(left, right);
  }
}
