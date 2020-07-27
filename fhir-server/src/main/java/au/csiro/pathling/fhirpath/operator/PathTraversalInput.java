/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
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
public class PathTraversalInput extends FunctionInput {

  /**
   * An expression representing the left operand.
   */
  @Nonnull
  @Getter
  private final FhirPath left;

  /**
   * An expression representing the right operand.
   */
  @Nonnull
  @Getter
  private final String right;

  /**
   * @param context The {@link ParserContext} that the operator should be executed within
   * @param left The {@link FhirPath} representing the left operand
   * @param right The FHIRPath expression on the right hand side of the operator
   */
  public PathTraversalInput(@Nonnull final ParserContext context, @Nonnull final FhirPath left,
      @Nonnull final String right) {
    super(context);
    this.left = left;
    this.right = right;
  }

}
