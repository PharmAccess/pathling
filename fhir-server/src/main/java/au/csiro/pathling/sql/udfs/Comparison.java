package au.csiro.pathling.sql.udfs;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import java.util.Arrays;

public interface Comparison<T> {

  @Udf("lt")
  Boolean lessThan(T left, T right);


  public static void register(SparkSession spark, Comparison<?> functions) {

    // can also add "namespace" such as datetime
    
    Arrays.stream(functions.getClass().getMethods()).forEach(m ->
    {
      final Udf udfAnnotation = m.getAnnotation(Udf.class);
      if (udfAnnotation != null) {

        // derrive the return type from the method or the annotation
        switch (m.getParameterCount()) {
          case 1:
            spark.udf().register(udfAnnotation.value(), p1 -> m.invoke(functions, p1),
                DataTypes.BooleanType);
            break;

          case 2:
            spark.udf().register(udfAnnotation.value(), (p1, p2) -> m.invoke(functions, p1, p2),
                DataTypes.BooleanType);
            break;
          //... 
        }
      }
    });

  }
}


