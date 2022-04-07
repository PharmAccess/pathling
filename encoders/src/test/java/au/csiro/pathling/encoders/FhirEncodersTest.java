/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright © 2018-2022, Commonwealth Scientific
 * and Industrial Research Organisation (CSIRO) ABN 41 687 119 230. Licensed
 * under the CSIRO Open Source Software Licence Agreement.
 *
 */

package au.csiro.pathling.encoders;

import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.functions;
import org.hl7.fhir.exceptions.FHIRException;
import org.hl7.fhir.r4.model.Annotation;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Condition;
import org.hl7.fhir.r4.model.DateTimeType;
import org.hl7.fhir.r4.model.Encounter;
import org.hl7.fhir.r4.model.IntegerType;
import org.hl7.fhir.r4.model.MedicationRequest;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Quantity;
import org.hl7.fhir.r4.model.Quantity.QuantityComparator;
import org.hl7.fhir.r4.model.Questionnaire;
import org.hl7.fhir.r4.model.QuestionnaireResponse;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test for FHIR encoders.
 */
public class FhirEncodersTest {

  private static final int NESTING_LEVEL_3 = 3;

  private static final FhirEncoders ENCODERS_L0 =
      FhirEncoders.forR4().getOrCreate();

  private static final FhirEncoders ENCODERS_L3 =
      FhirEncoders.forR4().withMaxNestingLevel(NESTING_LEVEL_3).getOrCreate();


  private static SparkSession spark;
  private static final Patient patient = TestData.newPatient();
  private static Dataset<Patient> patientDataset;
  private static Patient decodedPatient;

  private static final Condition condition = TestData.newCondition();
  private static Dataset<Condition> conditionsDataset;
  private static Condition decodedCondition;

  private static final Condition conditionWithVersion = TestData.conditionWithVersion();
  private static final Observation observation = TestData.newObservation();
  private static final MedicationRequest medRequest = TestData.newMedRequest();
  private static final Encounter encounter = TestData.newEncounter();
  private static final Questionnaire questionnaire = TestData.newQuestionnaire();
  private static final QuestionnaireResponse questionnaireResponse = TestData
      .newQuestionnaireResponse();

  private static Dataset<Observation> observationsDataset;
  private static Observation decodedObservation;
  private static Dataset<Condition> conditionsWithVersionDataset;
  private static Dataset<MedicationRequest> medDataset;
  private static MedicationRequest decodedMedRequest;
  private static Condition decodedConditionWithVersion;
  private static Dataset<Encounter> encounterDataset;
  private static Encounter decodedEncounter;
  private static Dataset<Questionnaire> questionnaireDataset;
  private static Questionnaire decodedQuestionnaire;
  private static Dataset<QuestionnaireResponse> questionnaireResponseDataset;
  private static QuestionnaireResponse decodedQuestionnaireResponse;

  /**
   * Set up Spark.
   */
  @BeforeClass
  public static void setUp() {
    spark = SparkSession.builder()
        .master("local[*]")
        .appName("testing")
        .config("spark.driver.bindAddress", "localhost")
        .config("spark.driver.host", "localhost")
        .getOrCreate();

    patientDataset = spark.createDataset(ImmutableList.of(patient),
        ENCODERS_L0.of(Patient.class));
    decodedPatient = patientDataset.head();

    conditionsDataset = spark.createDataset(ImmutableList.of(condition),
        ENCODERS_L0.of(Condition.class));
    decodedCondition = conditionsDataset.head();

    conditionsWithVersionDataset = spark.createDataset(ImmutableList.of(conditionWithVersion),
        ENCODERS_L0.of(Condition.class));
    decodedConditionWithVersion = conditionsWithVersionDataset.head();

    observationsDataset = spark.createDataset(ImmutableList.of(observation),
        ENCODERS_L0.of(Observation.class));
    decodedObservation = observationsDataset.head();

    // TODO: Uncomment if/when contained resources are supported.
    medDataset = spark.createDataset(ImmutableList.of(medRequest),
        ENCODERS_L0.of(MedicationRequest.class/*, Medication.class, Provenance.class*/));
    decodedMedRequest = medDataset.head();

    encounterDataset = spark
        .createDataset(ImmutableList.of(encounter), ENCODERS_L0.of(Encounter.class));
    decodedEncounter = encounterDataset.head();

    questionnaireDataset = spark
        .createDataset(ImmutableList.of(questionnaire), ENCODERS_L0.of(Questionnaire.class));
    decodedQuestionnaire = questionnaireDataset.head();

    questionnaireResponseDataset = spark
        .createDataset(ImmutableList.of(questionnaireResponse),
            ENCODERS_L0.of(QuestionnaireResponse.class));
    decodedQuestionnaireResponse = questionnaireResponseDataset.head();

  }

  /**
   * Tear down Spark.
   */
  @AfterClass
  public static void tearDown() {
    spark.stop();
  }

  @Test
  public void testResourceId() {
    Assert.assertEquals(condition.getId(),
        conditionsDataset.select("id").head().get(0));
    Assert.assertEquals(condition.getId(),
        decodedCondition.getId());
  }

  @Test
  public void testResourceWithVersionId() {
    Assert.assertEquals("with-version",
        conditionsWithVersionDataset.select("id").head().get(0));

    Assert.assertEquals(conditionWithVersion.getId(),
        conditionsWithVersionDataset.select("id_versioned").head().get(0));

    Assert.assertEquals(conditionWithVersion.getId(),
        decodedConditionWithVersion.getId());
  }

  @Test
  public void testResourceLanguage() {
    Assert.assertEquals(condition.getLanguage(),
        conditionsDataset.select("language").head().get(0));
    Assert.assertEquals(condition.getLanguage(),
        decodedCondition.getLanguage());
  }

  @Test
  public void boundCode() {

    final GenericRowWithSchema verificationStatus = (GenericRowWithSchema) conditionsDataset
        .select("verificationStatus")
        .head()
        .getStruct(0);

    final GenericRowWithSchema coding = (GenericRowWithSchema) verificationStatus
        .getList(verificationStatus.fieldIndex("coding"))
        .get(0);

    Assert.assertEquals(condition.getVerificationStatus().getCoding().size(), 1);
    Assert.assertEquals(condition.getVerificationStatus().getCodingFirstRep().getSystem(),
        coding.getString(coding.fieldIndex("system")));
    Assert.assertEquals(condition.getVerificationStatus().getCodingFirstRep().getCode(),
        coding.getString(coding.fieldIndex("code")));
  }

  @Test
  public void choiceValue() {

    // Our test condition uses the DateTime choice, so use that type and column.
    Assert.assertEquals(((DateTimeType) condition.getOnset()).getValueAsString(),
        conditionsDataset.select("onsetDateTime").head().get(0));

    Assert.assertEquals(condition.getOnset().toString(),
        decodedCondition.getOnset().toString());
  }

  @Test
  public void narrative() {

    Assert.assertEquals(condition.getText().getStatus().toCode(),
        conditionsDataset.select("text.status").head().get(0));
    Assert.assertEquals(condition.getText().getStatus(),
        decodedCondition.getText().getStatus());

    Assert.assertEquals(condition.getText().getDivAsString(),
        conditionsDataset.select("text.div").head().get(0));
    Assert.assertEquals(condition.getText().getDivAsString(),
        decodedCondition.getText().getDivAsString());
  }

  @Test
  public void coding() {

    final Coding expectedCoding = condition.getSeverity().getCodingFirstRep();
    final Coding actualCoding = decodedCondition.getSeverity().getCodingFirstRep();

    // Codings are a nested array, so we explode them into a table of the coding
    // fields, so we can easily select and compare individual fields.
    final Dataset<Row> severityCodings = conditionsDataset
        .select(functions.explode(conditionsDataset.col("severity.coding"))
            .alias("coding"))
        .select("coding.*") // Pull all fields in the coding to the top level.
        .cache();

    Assert.assertEquals(expectedCoding.getCode(),
        severityCodings.select("code").head().get(0));
    Assert.assertEquals(expectedCoding.getCode(),
        actualCoding.getCode());

    Assert.assertEquals(expectedCoding.getSystem(),
        severityCodings.select("system").head().get(0));
    Assert.assertEquals(expectedCoding.getSystem(),
        actualCoding.getSystem());

    Assert.assertEquals(expectedCoding.getUserSelected(),
        severityCodings.select("userSelected").head().get(0));
    Assert.assertEquals(expectedCoding.getUserSelected(),
        actualCoding.getUserSelected());

    Assert.assertEquals(expectedCoding.getDisplay(),
        severityCodings.select("display").head().get(0));
    Assert.assertEquals(expectedCoding.getDisplay(),
        actualCoding.getDisplay());
  }

  @Test
  public void reference() {

    Assert.assertEquals(condition.getSubject().getReference(),
        conditionsDataset.select("subject.reference").head().get(0));
    Assert.assertEquals(condition.getSubject().getReference(),
        decodedCondition.getSubject().getReference());
  }

  @Test
  public void integer() {

    Assert.assertEquals(((IntegerType) patient.getMultipleBirth()).getValue(),
        patientDataset.select("multipleBirthInteger").head().get(0));
    Assert.assertEquals(((IntegerType) patient.getMultipleBirth()).getValue(),
        ((IntegerType) decodedPatient.getMultipleBirth()).getValue());
  }

  @Test
  public void bigDecimal() {

    final BigDecimal originalDecimal = ((Quantity) observation.getValue()).getValue();
    final BigDecimal queriedDecimal = (BigDecimal) observationsDataset.select("valueQuantity.value")
        .head()
        .get(0);
    final int queriedDecimal_scale = observationsDataset.select("valueQuantity.value_scale")
        .head()
        .getInt(0);

    // we expect the values to be the same, but they may differ in scale
    Assert.assertEquals(0, originalDecimal.compareTo(queriedDecimal));
    Assert.assertEquals(originalDecimal.scale(), queriedDecimal_scale);

    final BigDecimal decodedDecimal = ((Quantity) decodedObservation.getValue()).getValue();
    // here we expect same value,  scale and precision
    Assert.assertEquals(originalDecimal, decodedDecimal);

    // Test can represent without loss 18 + 6 decimal places
    Assert.assertEquals(TestData.TEST_VERY_BIG_DECIMAL,
        decodedObservation.getReferenceRange().get(0).getHigh().getValue());

    // Test rounding of decimals with scale larger than 6
    Assert.assertEquals(TestData.TEST_VERY_SMALL_DECIMAL_SCALE_6,
        decodedObservation.getReferenceRange().get(0).getLow().getValue());
  }


  @Test
  public void choiceBigDecimalInQuestionnaire() {

    final BigDecimal originalDecimal = questionnaire.getItemFirstRep().getEnableWhenFirstRep()
        .getAnswerDecimalType().getValue();

    final BigDecimal queriedDecimal = (BigDecimal) questionnaireDataset
        .select(functions.col("item").getItem(0).getField("enableWhen").getItem(0)
            .getField("answerDecimal"))
        .head()
        .get(0);

    final int queriedDecimal_scale = questionnaireDataset
        .select(functions.col("item").getItem(0).getField("enableWhen").getItem(0)
            .getField("answerDecimal_scale"))
        .head()
        .getInt(0);

    // we expect the values to be the same, but they may differ in scale
    Assert.assertEquals(0, originalDecimal.compareTo(queriedDecimal));
    Assert.assertEquals(originalDecimal.scale(), queriedDecimal_scale);

    final BigDecimal decodedDecimal = decodedQuestionnaire.getItemFirstRep().getEnableWhenFirstRep()
        .getAnswerDecimalType().getValue();
    // here we expect same value,  scale and precision
    Assert.assertEquals(originalDecimal, decodedDecimal);

    // Test can represent without loss 18 + 6 decimal places
    Assert.assertEquals(TestData.TEST_VERY_BIG_DECIMAL,
        decodedQuestionnaire.getItemFirstRep().getInitialFirstRep().getValueDecimalType()
            .getValue());

    // Nested item should not be present.
    // Assert.assertTrue(decodedQuestionnaire.getItemFirstRep().getItem().isEmpty());
  }

  @Test
  public void choiceBigDecimalInQuestionnaireResponse() {
    final BigDecimal originalDecimal = questionnaireResponse.getItemFirstRep().getAnswerFirstRep()
        .getValueDecimalType().getValue();

    final BigDecimal queriedDecimal = (BigDecimal) questionnaireResponseDataset
        .select(functions.col("item").getItem(0).getField("answer").getItem(0)
            .getField("valueDecimal"))
        .head()
        .get(0);

    final int queriedDecimal_scale = questionnaireResponseDataset
        .select(functions.col("item").getItem(0).getField("answer").getItem(0)
            .getField("valueDecimal_scale"))
        .head()
        .getInt(0);

    Assert.assertEquals(0, originalDecimal.compareTo(queriedDecimal));
    Assert.assertEquals(originalDecimal.scale(), queriedDecimal_scale);

    final BigDecimal decodedDecimal = decodedQuestionnaireResponse.getItemFirstRep()
        .getAnswerFirstRep().getValueDecimalType().getValue();
    Assert.assertEquals(originalDecimal, decodedDecimal);

    Assert.assertEquals(TestData.TEST_VERY_SMALL_DECIMAL_SCALE_6,
        decodedQuestionnaireResponse.getItemFirstRep().getAnswerFirstRep().getValueDecimalType()
            .getValue());
    Assert.assertEquals(TestData.TEST_VERY_BIG_DECIMAL,
        decodedQuestionnaireResponse.getItemFirstRep().getAnswer().get(1).getValueDecimalType()
            .getValue());
  }

  @Test
  public void instant() {
    final Date originalInstant = TestData.TEST_DATE;

    Assert.assertEquals(originalInstant,
        observationsDataset.select("issued")
            .head()
            .get(0));

    Assert.assertEquals(originalInstant, decodedObservation.getIssued());
  }

  @Test
  public void annotation() throws FHIRException {

    final Annotation original = medRequest.getNoteFirstRep();
    final Annotation decoded = decodedMedRequest.getNoteFirstRep();

    Assert.assertEquals(original.getText(),
        medDataset.select(functions.expr("note[0].text")).head().get(0));

    Assert.assertEquals(original.getText(), decoded.getText());
    Assert.assertEquals(original.getAuthorReference().getReference(),
        decoded.getAuthorReference().getReference());

  }

  /**
   * Sanity test with a deep copy to check we didn't break internal state used by copies.
   */
  @Test
  public void testCopyDecoded() {
    Assert.assertEquals(condition.getId(), decodedCondition.copy().getId());
    Assert.assertEquals(medRequest.getId(), decodedMedRequest.copy().getId());
    Assert.assertEquals(observation.getId(), decodedObservation.copy().getId());
    Assert.assertEquals(patient.getId(), decodedPatient.copy().getId());
  }

  @Test
  public void testEmptyAttributes() {
    final Map<String, String> attributes = decodedMedRequest.getText().getDiv().getAttributes();

    Assert.assertNotNull(attributes);
    Assert.assertEquals(0, attributes.size());
  }

  @Test
  public void testFromRdd() {

    final JavaSparkContext context = new JavaSparkContext(spark.sparkContext());

    final JavaRDD<Condition> conditionRdd = context.parallelize(ImmutableList.of(condition));

    final Dataset<Condition> ds = spark.createDataset(conditionRdd.rdd(),
        ENCODERS_L0.of(Condition.class));

    final Condition convertedCondition = ds.head();

    Assert.assertEquals(condition.getId(),
        convertedCondition.getId());
  }

  @Test
  public void testFromParquet() throws IOException {

    final Path dirPath = Files.createTempDirectory("encoder_test");

    final String path = dirPath.resolve("out.parquet").toString();

    conditionsDataset.write().save(path);

    final Dataset<Condition> ds = spark.read()
        .parquet(path)
        .as(ENCODERS_L0.of(Condition.class));

    final Condition readCondition = ds.head();

    Assert.assertEquals(condition.getId(),
        readCondition.getId());
  }

  @Test
  public void testEncoderCached() {

    Assert.assertSame(ENCODERS_L0.of(Condition.class),
        ENCODERS_L0.of(Condition.class));

    Assert.assertSame(ENCODERS_L0.of(Patient.class),
        ENCODERS_L0.of(Patient.class));
  }

  @Test
  public void testPrimitiveClassDecoding() {
    Assert.assertEquals(encounter.getClass_().getCode(),
        encounterDataset.select("class.code").head().get(0));
    Assert.assertEquals(encounter.getClass_().getCode(), decodedEncounter.getClass_().getCode());
  }

  @Test
  public void testNestedQuestionnaire() {

    // Build a list of questionnaires with increasing nesting levels
    final List<Questionnaire> questionnaires = IntStream.rangeClosed(0, NESTING_LEVEL_3)
        .mapToObj(i -> TestData.newNestedQuestionnaire(i, 4))
        .collect(Collectors.toUnmodifiableList());

    final Questionnaire questionnaire_L0 = questionnaires.get(0);

    // Encode with level 0
    final Dataset<Questionnaire> questionnaireDataset_L0 = spark
        .createDataset(questionnaires,
            ENCODERS_L0.of(Questionnaire.class));
    final List<Questionnaire> decodedQuestionnaires_L0 = questionnaireDataset_L0.collectAsList();

    // all questionnaires should be pruned to be the same as level 0
    for (int i = 0; i <= NESTING_LEVEL_3; i++) {
      assertTrue(questionnaire_L0.equalsDeep(decodedQuestionnaires_L0.get(i)));
    }

    // Encode with level 3
    final Dataset<Questionnaire> questionnaireDataset_L3 = spark
        .createDataset(questionnaires,
            ENCODERS_L3.of(Questionnaire.class));
    final List<Questionnaire> decodedQuestionnaires_L3 = questionnaireDataset_L3.collectAsList();
    // All questionnaires should be fully encoded
    for (int i = 0; i <= NESTING_LEVEL_3; i++) {
      assertTrue(questionnaires.get(i).equalsDeep(decodedQuestionnaires_L3.get(i)));
    }

    Assert.assertEquals(Stream.of("Item/0", "Item/0", "Item/0", "Item/0").map(RowFactory::create)
            .collect(Collectors.toUnmodifiableList()),
        questionnaireDataset_L3.select(functions.col("item").getItem(0).getField("linkId"))
            .collectAsList());

    Assert.assertEquals(Stream.of(null, "Item/1.0", "Item/1.0", "Item/1.0").map(RowFactory::create)
            .collect(Collectors.toUnmodifiableList()),
        questionnaireDataset_L3
            .select(functions.col("item")
                .getItem(1).getField("item")
                .getItem(0).getField("linkId"))
            .collectAsList());

    Assert.assertEquals(Stream.of(null, null, "Item/2.1.0", "Item/2.1.0").map(RowFactory::create)
            .collect(Collectors.toUnmodifiableList()),
        questionnaireDataset_L3
            .select(functions.col("item")
                .getItem(2).getField("item")
                .getItem(1).getField("item")
                .getItem(0).getField("linkId"))
            .collectAsList());

    Assert.assertEquals(Stream.of(null, null, null, "Item/3.2.1.0").map(RowFactory::create)
            .collect(Collectors.toUnmodifiableList()),
        questionnaireDataset_L3
            .select(functions.col("item")
                .getItem(3).getField("item")
                .getItem(2).getField("item")
                .getItem(1).getField("item")
                .getItem(0).getField("linkId"))
            .collectAsList());
  }

  @Test
  public void testQuantityComparator() {
    final QuantityComparator originalComparator = observation.getValueQuantity().getComparator();
    final String queriedComparator = observationsDataset.select("valueQuantity.comparator").head()
        .getString(0);

    Assert.assertEquals(originalComparator.toCode(), queriedComparator);
  }
}
