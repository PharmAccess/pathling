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

import static com.github.tomakehurst.wiremock.client.WireMock.proxyAllTo;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static org.apache.http.HttpHeaders.ACCEPT_LANGUAGE;

import au.csiro.pathling.config.EncodingConfiguration;
import au.csiro.pathling.config.TerminologyConfiguration;
import au.csiro.pathling.library.PathlingContext;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.recording.RecordSpecBuilder;
import java.nio.file.Path;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

@State(Scope.Benchmark)
@Getter
@Slf4j
public class PathlingBenchmarkState {

  public static final int WIREMOCK_PORT = 4072;
  public static final String DEFAULT_TX = "https://tx.ontoserver.csiro.au/fhir";

  @Nonnull
  private final PathlingContext pathlingContext;

  @Nonnull
  private final Path testDataPath;

  @Nonnull
  private final WireMockServer wireMockServer;

  private final boolean recordingEnabled = Boolean.parseBoolean(System.getProperty(
      "pathling.test.recording.enabled", "false"));

  @Nullable
  private final String recordingTxServerUrl = System.getProperty(
      "pathling.test.recording.terminologyServerUrl", DEFAULT_TX);

  public PathlingBenchmarkState() {
    final SparkSession spark = SparkSession.builder()
        .master("local[*]")
        .appName("pathling-benchmark")
        .getOrCreate();
    pathlingContext = PathlingContext.create(spark,
        EncodingConfiguration.builder().build(),
        TerminologyConfiguration.builder()
            .serverUrl("http://localhost:" + WIREMOCK_PORT)
            .build()
    );
    testDataPath = Path.of("src/test/resources/test-data").toAbsolutePath()
        .normalize();
    final String wiremockRecordingPath = Path.of("src/test/resources/wiremock").toAbsolutePath()
        .normalize().toString();
    wireMockServer = new WireMockServer(
        new WireMockConfiguration().port(WIREMOCK_PORT)
            .usingFilesUnderDirectory(wiremockRecordingPath));
    WireMock.configureFor("localhost", WIREMOCK_PORT);
  }

  @Setup(Level.Trial)
  public void setUpTrial() {
    log.info("Starting WireMock server");
    wireMockServer.start();
    if (recordingEnabled) {
      log.warn("Proxying all requests to: {}", recordingTxServerUrl);
      stubFor(proxyAllTo(recordingTxServerUrl));
    }
  }

  @Setup(Level.Iteration)
  public void setUpIteration() {
  }

  @TearDown(Level.Iteration)
  public void tearDownIteration() {
  }

  @TearDown(Level.Trial)
  public void tearDownTrial() {
    if (recordingEnabled) {
      log.warn("Recording snapshots to: {}", wireMockServer.getOptions().filesRoot());
      wireMockServer
          .snapshotRecord(new RecordSpecBuilder().captureHeader(ACCEPT_LANGUAGE)
              .matchRequestBodyWithEqualToJson(true, false));
    }
    log.info("Stopping WireMock server");
    wireMockServer.stop();
  }

}
