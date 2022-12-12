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

package au.csiro.pathling.terminology.validatecode;

import static au.csiro.pathling.fhir.ParametersUtils.toBooleanResult;
import static au.csiro.pathling.terminology.TerminologyParameters.optional;
import static au.csiro.pathling.terminology.TerminologyParameters.required;
import static java.util.Objects.isNull;

import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.fhirpath.encoding.ImmutableCoding;
import au.csiro.pathling.terminology.TerminologyOperation;
import ca.uhn.fhir.rest.gclient.IOperationUntypedWithInput;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.UriType;

/**
 * An implementation of {@link TerminologyOperation} for the validate-code operation.
 *
 * @author John Grimes
 * @see <a
 * href="https://www.hl7.org/fhir/R4/valueset-operation-validate-code.html">ValueSet/$validate-code</a>
 */
public class ValidateCodeExecutor implements
    TerminologyOperation<Parameters, Boolean> {

  @Nonnull
  private final TerminologyClient terminologyClient;

  @Nonnull
  private final ValidateCodeParameters parameters;

  public ValidateCodeExecutor(@Nonnull final TerminologyClient terminologyClient,
      @Nonnull final ValidateCodeParameters parameters) {
    this.terminologyClient = terminologyClient;
    this.parameters = parameters;
  }

  @Override
  public Optional<Boolean> validate() {
    final ImmutableCoding coding = parameters.getCoding();
    if (isNull(coding.getSystem()) || isNull(coding.getCode())) {
      return Optional.of(false);
    } else {
      return Optional.empty();
    }
  }

  @Override
  public IOperationUntypedWithInput<Parameters> buildRequest() {
    final String codeSystemUrl = parameters.getValueSetUrl();
    final ImmutableCoding coding = parameters.getCoding();
    return terminologyClient.buildValidateCode(
        required(UriType::new, codeSystemUrl), required(UriType::new, coding.getSystem()),
        optional(StringType::new, coding.getVersion()),
        required(CodeType::new, coding.getCode())
    );
  }

  @Override
  public Boolean extractResult(@Nonnull final Parameters response) {
    return toBooleanResult(response);
  }

  @Override
  public Boolean invalidRequestFallback() {
    return false;
  }

}
