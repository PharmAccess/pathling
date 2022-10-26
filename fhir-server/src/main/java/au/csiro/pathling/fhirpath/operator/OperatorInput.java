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

package au.csiro.pathling.fhirpath.operator;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.FunctionInput;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import javax.annotation.Nonnull;
import lombok.Getter;

/**
 * Represents the inputs to a binary operator in FHIRPath.
 *
 * @author John Grimes
 */
@Getter
public class OperatorInput extends FunctionInput {

  /**
   * An expression representing the left operand.
   */
  @Nonnull
  private final FhirPath left;

  /**
   * An expression representing the right operand.
   */
  @Nonnull
  private final FhirPath right;

  /**
   * @param context The {@link ParserContext} that the operator should be executed within
   * @param left The {@link FhirPath} representing the left operand
   * @param right The {@link FhirPath} representing the right operand
   */
  public OperatorInput(@Nonnull final ParserContext context, @Nonnull final FhirPath left,
      @Nonnull final FhirPath right) {
    super(context);
    this.left = left;
    this.right = right;
  }

}
