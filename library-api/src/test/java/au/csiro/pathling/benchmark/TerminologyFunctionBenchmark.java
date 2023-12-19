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

package au.csiro.pathling.benchmark;

import static au.csiro.pathling.library.TerminologyHelpers.SNOMED_URI;
import static au.csiro.pathling.library.TerminologyHelpers.toCoding;
import static au.csiro.pathling.library.TerminologyHelpers.toEclValueSet;
import static au.csiro.pathling.library.TerminologyHelpers.toSnomedCoding;
import static au.csiro.pathling.sql.Terminology.member_of;
import static au.csiro.pathling.sql.Terminology.property_of;
import static org.apache.spark.sql.functions.lit;

import au.csiro.pathling.fhirpath.encoding.CodingEncoding;
import au.csiro.pathling.sql.Terminology;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Coding;
import org.openjdk.jmh.annotations.Benchmark;

public class TerminologyFunctionBenchmark {

  @Benchmark
  public List<Row> memberOf(final PathlingBenchmarkState state) {
    final String path = state.getTestDataPath().resolve("csv/conditions.csv").toString();
    final Dataset<Row> csv = state.getPathlingContext().getSpark().read().option("header", "true")
        .csv(path);
    final String ecl = "<< 64572001|Disease| : (\n"
        + "      << 370135005|Pathological process| = << 441862004|Infectious process|,\n"
        + "      << 246075003|Causative agent| = << 49872002|Virus|\n"
        + "    )";
    final Dataset<Row> result = csv.select(
        csv.col("CODE"),
        csv.col("DESCRIPTION"),
        member_of(toSnomedCoding(csv.col("CODE")),
            toEclValueSet(ecl)).alias("VIRAL_INFECTION")
    );
    return result.collectAsList();
  }

  @Benchmark
  public List<Row> translate(final PathlingBenchmarkState state) {
    final String path = state.getTestDataPath().resolve("csv/conditions.csv").toString();
    final Dataset<Row> csv = state.getPathlingContext().getSpark().read().option("header", "true")
        .csv(path);

    final Dataset<Row> result = csv.withColumn(
        "READ_CODES",
        Terminology.translate(
            toCoding(csv.col("CODE"), "https://snomed.info/sct"),
            "http://snomed.info/sct/900000000000207008?fhir_cm=900000000000497000",
            false, null
        ).getField("code")
    );
    return result.collectAsList();
  }

  @Benchmark
  public List<Row> subsumes(final PathlingBenchmarkState state) {
    final String path = state.getTestDataPath().resolve("csv/conditions.csv").toString();
    final Dataset<Row> csv = state.getPathlingContext().getSpark().read().option("header", "true")
        .csv(path);

    final Dataset<Row> result = csv.select(
        csv.col("CODE"),
        // 232208008 |Ear, nose and throat disorder|
        Terminology.subsumes(
            CodingEncoding.toStruct(
                lit(null),
                lit(SNOMED_URI),
                lit(null),
                lit("232208008"),
                lit(null),
                lit(null)
            ),
            toSnomedCoding(csv.col("CODE"))
        ).alias("IS_ENT")
    );
    return result.collectAsList();
  }

  @Benchmark
  public List<Row> property(final PathlingBenchmarkState state) {
    final String path = state.getTestDataPath().resolve("csv/conditions.csv").toString();
    final Dataset<Row> csv = state.getPathlingContext().getSpark().read().option("header", "true")
        .csv(path);

    final Dataset<Row> parents = csv.withColumn(
        "PARENTS",
        property_of(toSnomedCoding(csv.col("CODE")), "parent", "code")
    );
    final Dataset<Row> exploded_parents = parents.selectExpr(
        "CODE", "DESCRIPTION", "explode_outer(PARENTS) AS PARENT"
    );
    final Dataset<Row> result = exploded_parents.withColumn(
        "PARENT_DISPLAY", Terminology.display(
            toSnomedCoding(exploded_parents.col("PARENT")))
    );
    return result.collectAsList();
  }

  @Benchmark
  public List<Row> designation(final PathlingBenchmarkState state) {
    final String path = state.getTestDataPath().resolve("csv/conditions.csv").toString();
    final Dataset<Row> csv = state.getPathlingContext().getSpark().read().option("header", "true")
        .csv(path);

    final Dataset<Row> synonyms = csv.withColumn(
        "SYNONYMS",
        Terminology.designation(toSnomedCoding(csv.col("CODE")),
            new Coding("http://snomed.info/sct",
                "900000000000013009", null))
    );
    final Dataset<Row> result = synonyms.selectExpr(
        "CODE", "DESCRIPTION", "explode_outer(SYNONYMS) AS SYNONYM"
    );
    return result.collectAsList();
  }

}
