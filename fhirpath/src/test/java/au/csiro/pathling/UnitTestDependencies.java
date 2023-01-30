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

package au.csiro.pathling;

import au.csiro.pathling.config.SparkConfiguration;
import au.csiro.pathling.config.StorageConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.encoders.terminology.ucum.Ucum;
import au.csiro.pathling.sql.FhirpathUDFRegistrar;
import au.csiro.pathling.sql.udf.TerminologyUdfRegistrar;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.test.SharedMocks;
import au.csiro.pathling.test.stubs.TestTerminologyServiceFactory;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.sql.SparkSession;
import org.fhir.ucum.UcumException;
import org.fhir.ucum.UcumService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * @author John Grimes
 */
@Configuration
@ComponentScan
@Profile("unit-test")
public class UnitTestDependencies {

  // TODO: Why we do not need to declare it in the previous version (SpringBoot magic)???
  @Bean
  @ConditionalOnMissingBean
  @Nonnull
  static ThreadPoolTaskExecutor threadPoolTaskExecutor() {
    return new ThreadPoolTaskExecutor();
  }


  @Bean
  @ConditionalOnMissingBean
  @Nonnull
  static SparkConfiguration sparkConfiguration() {
    return SparkConfiguration.builder().build();
  }


  @Bean
  @ConditionalOnMissingBean
  @Nonnull
  static StorageConfiguration storageConfiguration() {
    return StorageConfiguration.builder().warehouseUrl("file:///some/nonexistent/path").build();
  }

  @Bean
  @ConditionalOnMissingBean
  @Nonnull
  static PathlingVersion version() {
    return new PathlingVersion();
  }

  @Bean
  @ConditionalOnMissingBean
  @Nonnull
  static SparkSession sparkSession(@Nonnull final SparkConfiguration configuration,
      @Nonnull final Environment environment,
      @Nonnull final TerminologyServiceFactory terminologyServiceFactory,
      @Nonnull final Optional<SparkListener> sparkListener) {
    // TODO: Fix 
    final SparkSession spark = SparkSession.builder()
        .master("local[1]")
        .appName("pathling-unittest")
        .getOrCreate();
    TerminologyUdfRegistrar.registerUdfs(spark, terminologyServiceFactory);
    FhirpathUDFRegistrar.registerUDFs(spark);
    return spark;
  }

  @Bean
  @ConditionalOnMissingBean
  @Nonnull
  static FhirContext fhirContext() {
    return FhirContext.forR4();
  }

  @Bean
  @ConditionalOnMissingBean
  @Nonnull
  static IParser jsonParser(@Nonnull final FhirContext fhirContext) {
    return fhirContext.newJsonParser();
  }

  @Bean
  @ConditionalOnMissingBean
  @Nonnull
  static FhirEncoders fhirEncoders() {
    return FhirEncoders.forR4().getOrCreate();
  }

  @Bean
  @ConditionalOnMissingBean
  @Nonnull
  static TerminologyService terminologyService() {
    return SharedMocks.getOrCreate(TerminologyService.class);
  }

  @Bean
  @ConditionalOnMissingBean
  @Nonnull
  static TerminologyServiceFactory terminologyClientFactory() {
    return new TestTerminologyServiceFactory();
  }

  @Bean
  @ConditionalOnMissingBean
  @Nonnull
  static UcumService ucumService() throws UcumException {
    return Ucum.service();
  }

}
