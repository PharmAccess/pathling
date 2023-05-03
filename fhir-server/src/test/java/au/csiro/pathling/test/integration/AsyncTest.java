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

package au.csiro.pathling.test.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.io.CacheableDatabase;
import au.csiro.pathling.test.helpers.TestHelpers;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Date;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.sparkproject.jetty.http.HttpStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpMethod;
import org.springframework.http.RequestEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.TestPropertySource;

/**
 * @author John Grimes
 */
@TestPropertySource(properties = {"pathling.async.enabled=true"})
@Tag("Tranche2")
@Slf4j
class AsyncTest extends IntegrationTest {

  static final int TIMEOUT = 20000;
  static final int POLL_FREQUENCY = 1000;

  @Autowired
  SparkSession spark;

  @MockBean
  CacheableDatabase database;

  @LocalServerPort
  int port;

  @Autowired
  TestRestTemplate restTemplate;

  @Test
  void asyncExtract() throws URISyntaxException, MalformedURLException, InterruptedException {
    TestHelpers.mockResource(database, spark, ResourceType.OBSERVATION);
    final String uri = "http://localhost:" + port + "/fhir/Observation/$extract?column=id&"
        + "column=code.coding&column=valueQuantity.value&column=valueQuantity.unit";
    final RequestEntity<Void> request = RequestEntity.get(new URI(uri))
        .header("Prefer", "respond-async").build();
    log.info("Sending kick-off request");
    final ResponseEntity<String> response =
        restTemplate.exchange(uri, HttpMethod.GET, request, String.class);
    assertEquals(HttpStatus.ACCEPTED_202, response.getStatusCode().value());

    assertAsyncResponse(response, HttpStatus.OK_200, true);
  }

  @Test
  void enabledNotRequested() throws URISyntaxException {
    TestHelpers.mockResource(database, spark, ResourceType.PATIENT);
    final String uri = "http://localhost:" + port + "/fhir/Patient/$aggregate?aggregation=count()";
    final ResponseEntity<String> response = restTemplate
        .exchange(uri, HttpMethod.GET, RequestEntity.get(new URI(uri)).build(), String.class);
    assertTrue(response.getStatusCode().is2xxSuccessful());
  }

  @Test
  void error() throws URISyntaxException, MalformedURLException, InterruptedException {
    TestHelpers.mockResource(database, spark, ResourceType.PATIENT);
    final String uri = "http://localhost:" + port + "/fhir/Patient/$aggregate";
    final RequestEntity<Void> request = RequestEntity.get(new URI(uri))
        .header("Prefer", "respond-async")
        .build();
    final ResponseEntity<String> response = restTemplate
        .exchange(uri, HttpMethod.GET, request, String.class);
    assertTrue(response.getStatusCode().is2xxSuccessful());

    assertAsyncResponse(response, HttpStatus.BAD_REQUEST_400, false);
  }

  @Test
  void nonExistentJob() throws URISyntaxException {
    final String uri = "http://localhost:" + port + "/fhir/job?id=foo";
    final ResponseEntity<String> response = restTemplate
        .exchange(uri, HttpMethod.GET, RequestEntity.get(new URI(uri)).build(), String.class);
    assertEquals(HttpStatus.NOT_FOUND_404, response.getStatusCode().value());
  }

  void assertAsyncResponse(@Nonnull final ResponseEntity<String> response,
      final int expectedStatus, final boolean inProgressRequired)
      throws MalformedURLException, URISyntaxException, InterruptedException {
    int statusCode;
    boolean encounteredInProgressResponse = false;
    final long startTime = new Date().getTime();
    do {
      if (new Date().getTime() - startTime > TIMEOUT) {
        throw new AssertionError("Timed out waiting for async result");
      }

      final List<String> contentLocations = response.getHeaders().get("Content-Location");
      assertNotNull(contentLocations);
      final String contentLocation = contentLocations.get(0);
      assertNotNull(contentLocation);
      final URL statusUrl = new URL(contentLocation);
      final RequestEntity<Void> statusRequest = RequestEntity.get(statusUrl.toURI()).build();
      log.info("Sending status request");
      final ResponseEntity<String> statusResponse =
          restTemplate.exchange(statusUrl.toURI(), HttpMethod.GET, statusRequest, String.class);

      statusCode = statusResponse.getStatusCodeValue();
      final List<String> eTag = statusResponse.getHeaders().get("ETag");
      final List<String> cacheControl = statusResponse.getHeaders().get("Cache-Control");
      assertNotNull(eTag);
      assertNotNull(cacheControl);
      if (statusCode != expectedStatus) {
        assertEquals(HttpStatus.ACCEPTED_202, statusCode);
        assertTrue(eTag.contains("W/\"0\""));
        assertTrue(cacheControl.contains("no-store"));
        encounteredInProgressResponse = true;
        Thread.sleep(POLL_FREQUENCY);
      } else {
        final List<String> vary = statusResponse.getHeaders().get("Vary");
        assertNotNull(vary);
        assertTrue(cacheControl.contains("must-revalidate,max-age=1"));
        assertTrue(vary.contains("Accept,Accept-Encoding,Prefer,Authorization"));
      }
    } while (statusCode != expectedStatus);

    if (inProgressRequired) {
      assertTrue(encounteredInProgressResponse);
    }
    log.info("Successful response received");
  }

}
