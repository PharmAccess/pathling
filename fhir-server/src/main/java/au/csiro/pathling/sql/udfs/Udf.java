package au.csiro.pathling.sql.udfs;

import org.apache.spark.sql.types.DataType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface Udf {

  String value();
  // Return Type can be added here as well with the default inferred from the method return type
  // but it would have to a a string representation of the type (e.g. as DDL)

}
