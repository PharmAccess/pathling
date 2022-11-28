/*
 * Copyright 2022 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.sql.udf;

import au.csiro.pathling.fhirpath.encoding.CodingEncoding;
import org.apache.hadoop.shaded.org.apache.curator.shaded.com.google.common.collect.Streams;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Parameters;
import scala.collection.JavaConverters;
import scala.collection.mutable.WrappedArray;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.stream.Stream;

import static java.util.Objects.nonNull;

/**
 * Helper functions for terminology UDFs.
 */
public final class TerminologyUdfHelpers {

  private TerminologyUdfHelpers() {
    // Utility class
  }
  
  @Nullable
  static Row[] encodeMany(@Nullable final Stream<Coding> codings) {
    return codings != null
           ? codings.map(CodingEncoding::encode).toArray(Row[]::new)
           : null;
  }

  @Nullable
  public static Stream<Coding> decodeOneOrMany(final @Nullable Object codingRowOrArray,
      final int argumentIndex) {
    if (codingRowOrArray instanceof WrappedArray<?>) {
      //noinspection unchecked
      return decodeMany((WrappedArray<Row>) codingRowOrArray);
    } else if (codingRowOrArray instanceof Row || codingRowOrArray == null) {
      return decodeOne((Row) codingRowOrArray);
    } else {
      throw new IllegalArgumentException(
          String.format("Row or WrappedArray<Row> column expected in argument %s, but given: %s,",
              argumentIndex, codingRowOrArray.getClass()));
    }
  }

  @Nullable
  public static Stream<Coding> decodeOneOrMany(final @Nullable Object codingRowOrArray) {
    return decodeOneOrMany(codingRowOrArray, 0);
  }

  @Nullable
  public static Stream<Coding> decodeOne(final @Nullable Row codingRow) {
    return codingRow != null
           ? Stream.of(CodingEncoding.decode(codingRow))
           : null;
  }

  @Nullable
  public static Stream<Coding> decodeMany(final @Nullable WrappedArray<Row> codingsRow) {
    return codingsRow != null
           ? Streams.stream(JavaConverters.asJavaIterable(codingsRow)).filter(Objects::nonNull)
               .map(CodingEncoding::decode)
           : null;
  }

  public static boolean isValidCoding(@Nullable Coding coding) {
    return nonNull(coding) && nonNull(coding.getSystem()) && nonNull(coding.getCode());
  }

  @Nonnull
  public static Stream<Coding> validCodings(@Nonnull final Stream<Coding> codings) {
    return codings.filter(TerminologyUdfHelpers::isValidCoding);
  }
}
