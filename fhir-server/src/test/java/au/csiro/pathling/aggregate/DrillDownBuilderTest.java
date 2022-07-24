/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.aggregate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.test.builders.ElementPathBuilder;
import au.csiro.pathling.test.helpers.TestHelpers;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Value;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Base64BinaryType;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.CanonicalType;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.DateTimeType;
import org.hl7.fhir.r4.model.DateType;
import org.hl7.fhir.r4.model.DecimalType;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.InstantType;
import org.hl7.fhir.r4.model.IntegerType;
import org.hl7.fhir.r4.model.MarkdownType;
import org.hl7.fhir.r4.model.OidType;
import org.hl7.fhir.r4.model.PositiveIntType;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.TimeType;
import org.hl7.fhir.r4.model.Type;
import org.hl7.fhir.r4.model.UnsignedIntType;
import org.hl7.fhir.r4.model.UriType;
import org.hl7.fhir.r4.model.UrlType;
import org.hl7.fhir.r4.model.UuidType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
@Tag("UnitTest")
class DrillDownBuilderTest {

  @Autowired
  SparkSession spark;

  @Value
  static class TestParameters {

    @Nonnull
    Type label;

    @Nonnull
    String expectedResult;

    @Override
    public String toString() {
      return label.getClass().getSimpleName();
    }

  }

  static Stream<TestParameters> parameters() {
    return Stream.of(
        new TestParameters(
            new BooleanType(true),
            "someElement"),
        new TestParameters(
            new Coding(TestHelpers.SNOMED_URL, "373067005", "No"),
            "(someElement) = http://snomed.info/sct|373067005||No"),
        new TestParameters(
            new DateType("2020-01-01"),
            "(someElement) = @2020-01-01"),
        new TestParameters(
            new DateTimeType("2018-05-19T11:03:55.123Z"),
            "(someElement) = @2018-05-19T11:03:55Z"),
        new TestParameters(
            new InstantType("2018-05-19T11:03:55.123Z"),
            "(someElement) = @2018-05-19T11:03:55Z"),
        new TestParameters(
            new DecimalType("3.4"),
            "(someElement) = 3.4"),
        new TestParameters(
            new IntegerType("3"),
            "(someElement) = 3"),
        new TestParameters(
            new UnsignedIntType("3"),
            "(someElement) = 3"),
        new TestParameters(
            new PositiveIntType("3"),
            "(someElement) = 3"),
        new TestParameters(
            new StringType("foo"),
            "(someElement) = 'foo'"),
        new TestParameters(
            new UuidType("urn:uuid:953b3678-ba7d-48ed-8a55-37835c0f0ffd"),
            "(someElement) = 'urn:uuid:953b3678-ba7d-48ed-8a55-37835c0f0ffd'"),
        new TestParameters(
            new UriType("urn:test:foo"),
            "(someElement) = 'urn:test:foo'"),
        new TestParameters(
            new UrlType("https://pathling.csiro.au"),
            "(someElement) = 'https://pathling.csiro.au'"),
        new TestParameters(
            new CodeType("foo"),
            "(someElement) = 'foo'"),
        new TestParameters(
            new OidType("1.3.6.1.4.1.343"),
            "(someElement) = '1.3.6.1.4.1.343'"),
        new TestParameters(
            new IdType("foo"),
            "(someElement) = 'foo'"),
        new TestParameters(
            new MarkdownType("foo"),
            "(someElement) = 'foo'"),
        new TestParameters(
            new Base64BinaryType("foo"),
            "(someElement) = 'foo='"),
        new TestParameters(
            new CanonicalType("urn:test:foo"),
            "(someElement) = 'urn:test:foo'"),
        new TestParameters(
            new TimeType("12:30"),
            "(someElement) = @T12:30")
    );
  }

  @ParameterizedTest
  @MethodSource("parameters")
  void labelTypes(@Nonnull final TestParameters parameters) {
    final List<Optional<Type>> labels = List.of(Optional.of(parameters.getLabel()));
    final ElementPath grouping = new ElementPathBuilder(spark)
        .expression("someElement")
        .singular(true)
        .build();
    final List<FhirPath> groupings = List.of(grouping);
    final DrillDownBuilder builder = new DrillDownBuilder(labels, groupings,
        Collections.emptyList());
    final Optional<String> drillDown = builder.build();
    assertTrue(drillDown.isPresent());
    assertEquals(parameters.getExpectedResult(), drillDown.get());
  }

}
