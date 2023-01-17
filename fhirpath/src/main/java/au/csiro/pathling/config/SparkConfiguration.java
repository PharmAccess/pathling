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

package au.csiro.pathling.config;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Represents configuration that controls the behaviour of Apache Spark.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SparkConfiguration {

  /**
   * The name that Pathling will be identified as within the Spark cluster.
   */
  @NotBlank
  @Builder.Default
  private String appName = "Pathling";

  /**
   * Setting this option to {@code true} will enable additional logging relating to the query plan
   * used to execute queries.
   */
  @NotNull
  @Builder.Default
  private Boolean explainQueries = false;
}
