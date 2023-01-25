package au.csiro.pathling.examples;

import au.csiro.pathling.query.PathlingClient;
import au.csiro.pathling.config.QueryConfiguration;
import au.csiro.pathling.extract.ExtractRequest;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.query.ExtractQuery;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import java.nio.file.Path;

public class ExtractFromJsonApp {

  public static void main(String[] args) throws Exception {

    final Path fhirData = Path.of("fhir-server/src/test/resources/test-data/fhir").toAbsolutePath();
    System.out.printf("JSON Data: %s\n", fhirData);

    final SparkSession spark = SparkSession.builder()
        .appName(ExtractFromJsonApp.class.getName())
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate();

    final Dataset<Row> patientJsonDf = spark.read()
        .text(fhirData.resolve("Patient.ndjson").toString());
    final Dataset<Row> conditionJsonDf = spark.read()
        .text(fhirData.resolve("Condition.ndjson").toString());

    final PathlingContext ptc = PathlingContext.create(spark);

    final PathlingClient pathlingClient = ptc.newClientBuilder()
        .withQueryConfiguration(QueryConfiguration.builder().explainQueries(true).build())
        .withResource(ResourceType.PATIENT, ptc.encode(patientJsonDf, "Patient").cache())
        .withResource(ResourceType.CONDITION, ptc.encode(conditionJsonDf, "Condition").cache())
        .build();

    final Dataset<Row> patientResult = pathlingClient.execute(
        ExtractRequest.builderFor(ResourceType.PATIENT)
            .withColumn("id")
            .withColumn("gender")
            .withColumn("reverseResolve(Condition.subject).code.coding.code")
            .withFilter("gender = 'male'")
            .withLimit(10)
            .build()
    );
    patientResult.show(5);

    final Dataset<Row> conditionResult =
        ExtractQuery.of(ResourceType.CONDITION)
            .withColumn("id")
            .withColumn("code.coding.code")
            .withColumn("code.coding.display")
            .withColumn("subject.resolve().ofType(Patient).gender")
            .withLimit(10)
            .execute(pathlingClient);

    conditionResult.show(5);
  }
}
