/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function.subsumes;

import static au.csiro.pathling.fhirpath.TerminologyUtils.isCodeableConcept;
import static au.csiro.pathling.fhirpath.TerminologyUtils.isCodingOrCodeableConcept;
import static au.csiro.pathling.fhirpath.function.NamedFunction.expressionFromInput;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;
import static org.apache.spark.sql.functions.*;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.NonLiteralPath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.encoding.SimpleCodingsDecoders;
import au.csiro.pathling.fhirpath.function.NamedFunction;
import au.csiro.pathling.fhirpath.function.NamedFunctionInput;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.sql.SqlExtensions;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.slf4j.MDC;


/**
 * A function that takes a set of Codings or CodeableConcepts as inputs and returns a set of boolean
 * values whether based upon whether each item subsumes or is subsumedBy one or more Codings or
 * CodeableConcepts in the argument set.
 *
 * @author John Grimes
 * @author Piotr Szul
 * @see <a href="https://hl7.org/fhir/R4/fhirpath.html#functions">Additional functions</a>
 */

@Slf4j
public class SubsumesFunction implements NamedFunction {

  /**
   * The column name that this function uses to represent resource ID within its working dataset.
   */
  private static final String COL_ID = "id";

  /**
   * The column name that this function uses to represent input codings within its working dataset.
   */
  private static final String COL_INPUT_CODINGS = "inputCodings";

  /**
   * The column name that this function uses to represent argument codings within its working
   * dataset.
   */
  private static final String COL_ARG_CODINGS = "argCodings";

  private static final String COL_ARG_ID = "argId";
  private static final String COL_CODING = "coding";
  private static final String FIELD_CODING = "coding";

  private boolean inverted = false;
  private String functionName = "subsumes";

  /**
   * Creates a new SubsumesFunction, with a type of {@code subsumes}.
   */
  public SubsumesFunction() {
  }

  /**
   * Creates a new SubsumesFunction, specifying whether it is inverted. "Inverted" means that the
   * type is {@code subsumedBy}, otherwise it is {@code subsumes}.
   *
   * @param inverted whether to invert the operation from {@code subsumes} to {@code subsumedBy}
   */
  public SubsumesFunction(final boolean inverted) {
    this.inverted = inverted;
    if (inverted) {
      this.functionName = "subsumedBy";
    }
  }

  @Nonnull
  @Override
  public FhirPath invoke(@Nonnull final NamedFunctionInput input) {
    validateInput(input);

    final NonLiteralPath inputFhirPath = input.getInput();
    final Dataset<Row> idAndCodingSet = createJoinedDataset(input.getInput(),
        input.getArguments().get(0));
    // Process the subsumption operation per partition, adding a result column to the dataset.

    final Column codingPairCol = struct(idAndCodingSet.col(COL_INPUT_CODINGS),
        idAndCodingSet.col(COL_ARG_CODINGS));

    @SuppressWarnings({"OptionalGetWithoutIsPresent", "TypeMayBeWeakened"})
    final SubsumptionMapperWithPreview mapper =
        new SubsumptionMapperWithPreview(MDC.get("requestId"),
            input.getContext().getTerminologyServiceFactory().get(),
            inverted);

    final Dataset<Row> resultDataset = SqlExtensions
        .mapWithPartitionPreview(idAndCodingSet, codingPairCol,
            SimpleCodingsDecoders::decodeListPair,
            mapper, StructField.apply("result", DataTypes.BooleanType, true, Metadata.empty()));
    final Column resultColumn = col("result");

    // Construct a new result expression.
    final String expression = expressionFromInput(input, functionName);
    return ElementPath
        .build(expression, resultDataset, inputFhirPath.getIdColumn(),
            inputFhirPath.getEidColumn(), resultColumn, inputFhirPath.isSingular(),
            inputFhirPath.getForeignResource(), inputFhirPath.getThisColumn(),
            FHIRDefinedType.BOOLEAN);
  }

  /**
   * Creates a dataset that preserves previous columns and adds three new ones: resource ID, input
   * codings and argument codings.
   *
   * @see #toInputDataset(FhirPath)
   * @see #toArgDataset(FhirPath)
   */
  @Nonnull
  private Dataset<Row> createJoinedDataset(@Nonnull final FhirPath inputFhirPath,
      @Nonnull final FhirPath argFhirPath) {

    final Dataset<Row> inputCodingSet = toInputDataset(inputFhirPath);
    final Dataset<Row> argCodingSet = toArgDataset(argFhirPath);

    return inputCodingSet.join(argCodingSet,
        col(COL_ID).equalTo(col(COL_ARG_ID)), "left_outer");
  }

  /**
   * Creates a {@link Dataset} with a new column, which is an array of all of the codings within the
   * values. Each CodeableConcept is converted to an array that includes all its codings. Each
   * Coding is converted to an array that only includes that coding.
   * <p>
   * Null Codings and CodeableConcepts are represented as null.
   *
   * @param fhirPath the {@link FhirPath} object to convert
   * @return the resulting Dataset
   */
  @Nonnull
  private Dataset<Row> toInputDataset(@Nonnull final FhirPath fhirPath) {
    final Column valueColumn = fhirPath.getValueColumn();

    assert (isCodingOrCodeableConcept(fhirPath));

    final Dataset<Row> expressionDataset = fhirPath.getDataset()
        .withColumn(COL_ID, fhirPath.getIdColumn());
    final Column codingArrayCol = (isCodeableConcept(fhirPath))
                                  ? valueColumn.getField(FIELD_CODING)
                                  : array(valueColumn);

    return expressionDataset.withColumn(COL_INPUT_CODINGS,
        when(valueColumn.isNotNull(), codingArrayCol).otherwise(null));
  }

  /**
   * Converts the the argument {@link FhirPath} to a Dataset with the schema: STRING id,
   * ARRAY(struct CODING) codingSet.
   * <p>
   * All codings are collected in a single `array` per resource.
   * <p>
   * Null Codings and CodeableConcepts are ignored. In the case where the resource does not have any
   * non-null elements, an empty array will be created.
   *
   * @param fhirPath to convert
   * @return input dataset
   */
  @Nonnull
  private Dataset<Row> toArgDataset(@Nonnull final FhirPath fhirPath) {
    final Column valueColumn = fhirPath.getValueColumn();

    assert (isCodingOrCodeableConcept(fhirPath));

    final Dataset<Row> expressionDataset = fhirPath.getDataset();
    final Column codingCol = (isCodeableConcept(fhirPath))
                             ? explode_outer(valueColumn.getField(FIELD_CODING))
                             : valueColumn;

    final Dataset<Row> systemAndCodeDataset = expressionDataset
        .select(fhirPath.getIdColumn().alias(COL_ARG_ID), codingCol.alias(COL_CODING));

    return systemAndCodeDataset
        .groupBy(systemAndCodeDataset.col(COL_ARG_ID))
        .agg(collect_set(systemAndCodeDataset.col(COL_CODING)).alias(COL_ARG_CODINGS));
  }

  private void validateInput(@Nonnull final NamedFunctionInput input) {

    final ParserContext context = input.getContext();
    checkUserInput(context.getTerminologyServiceFactory()
        .isPresent(), "Attempt to call terminology function " + functionName
        + " when terminology service has not been configured");

    checkUserInput(
        input.getArguments().size() == 1,
        functionName + " function accepts one argument of type Coding or CodeableConcept"
    );

    validateExpressionType(input.getInput(), "input");
    validateExpressionType(input.getArguments().get(0), "argument");
  }

  private void validateExpressionType(@Nonnull final FhirPath inputPath,
      @Nonnull final String pathRole) {
    checkUserInput(
        isCodingOrCodeableConcept(inputPath),
        functionName + " function accepts " + pathRole + " of type Coding or CodeableConcept"
    );
  }
}
