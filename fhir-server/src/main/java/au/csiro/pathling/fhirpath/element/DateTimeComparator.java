package au.csiro.pathling.fhirpath.element;

import au.csiro.pathling.fhirpath.Comparable.Comparator;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.BaseDateTimeType;
import org.hl7.fhir.r4.model.DateTimeType;
import javax.annotation.Nonnull;
import java.util.function.BiFunction;

public class DateTimeComparator implements Comparator {

  public final static DateTimeComparator INSTANCE = new DateTimeComparator();

  @Nonnull
  private static UserDefinedFunction wrapInUdf(
      BiFunction<DateTimeType, DateTimeType, Boolean> function) {
    final UDF2<String, String, Boolean> udf = (left, right) ->
        function.apply(new DateTimeType(left), new DateTimeType(right));
    return functions.udf(udf, DataTypes.BooleanType);
  }

  private final UserDefinedFunction equalsToUDF = wrapInUdf(DateTimeType::equalsUsingFhirPathRules);
  private final UserDefinedFunction lessThanUDF = wrapInUdf(DateTimeType::before);

  @Override
  public Column equalsTo(final Column left, final Column right) {
    return equalsToUDF.apply(left, right);
  }

  @Override
  public Column lessThan(final Column left, final Column right) {
    // or return functions.callUDF("datetime_lt", left, right);
    return lessThanUDF.apply(left, right);

  }
}
