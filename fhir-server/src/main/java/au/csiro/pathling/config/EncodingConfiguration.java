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

package au.csiro.pathling.config;

import java.util.Set;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import lombok.Data;

/**
 * Represents configuration specific to FHIR encoding.
 */
@Data
public class EncodingConfiguration {

  /**
   * The maximum nesting level for recursive data types.
   */
  @NotNull
  @Min(0)
  private Integer maxNestingLevel;

  /**
   * The list of types that are encoded within open types, such as extensions.
   */
  @NotNull
  private Set<String> openTypes;

  /**
   * Enables support for FHIR extensions.
   */
  @NotNull
  private boolean enableExtensions;

}
