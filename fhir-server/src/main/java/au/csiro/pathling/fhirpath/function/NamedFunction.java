/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function;

import static au.csiro.pathling.utilities.Preconditions.checkUserInput;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.function.memberof.MemberOfFunction;
import au.csiro.pathling.fhirpath.function.subsumes.SubsumesFunction;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

/**
 * Represents a named function in FHIRPath.
 *
 * @author John Grimes
 */
public interface NamedFunction {

  /**
   * Mapping of function names to instances of those functions.
   */
  Map<String, NamedFunction> NAME_TO_INSTANCE = new ImmutableMap.Builder<String, NamedFunction>()
      .put("count", new CountFunction())
      .put("resolve", new ResolveFunction())
      .put("ofType", new OfTypeFunction())
      .put("reverseResolve", new ReverseResolveFunction())
      .put("memberOf", new MemberOfFunction())
      .put("where", new WhereFunction())
      .put("subsumes", new SubsumesFunction())
      .put("subsumedBy", new SubsumesFunction(true))
      .put("empty", new EmptyFunction())
      .put("first", new FirstFunction())
      .put("not", new NotFunction())
      .build();

  /**
   * The FHIRPath expression for the $this keyword, used to access the current item in the
   * collection in functions such as {@code where}.
   *
   * @see <a href="http://hl7.org/fhirpath/2018Sep/index.html#functions-2">Functions</a>
   */
  String THIS = "$this";

  /**
   * Invokes this function with the specified inputs.
   *
   * @param input A NamedFunctionInput object
   * @return A FhirPath object representing the resulting expression
   */
  @Nonnull
  FhirPath invoke(@Nonnull NamedFunctionInput input);

  /**
   * Retrieves an instance of the function with the specified name.
   *
   * @param name The name of the function
   * @return An instance of a NamedFunction
   */
  @Nonnull
  static NamedFunction getInstance(@Nonnull final String name) {
    final NamedFunction function = NAME_TO_INSTANCE.get(name);
    checkUserInput(function != null, "Unsupported function: " + name);
    return function;
  }

  /**
   * Check that no arguments have been passed within the supplied {@link NamedFunctionInput}.
   *
   * @param functionName The name of the function, used for error reporting purposes
   * @param input The {@link NamedFunctionInput} to check for arguments
   */
  static void checkNoArguments(@Nonnull final String functionName,
      @Nonnull final NamedFunctionInput input) {
    checkUserInput(input.getArguments().isEmpty(),
        "Arguments can not be passed to " + functionName + " function");
  }

  /**
   * @param input A {@link NamedFunctionInput}
   * @param functionName The name of the function
   * @return A FHIRPath expression for use in the output of the function
   */
  @Nonnull
  static String expressionFromInput(@Nonnull final NamedFunctionInput input,
      @Nonnull final String functionName) {
    final String inputExpression = input.getInput().getExpression();
    final String argumentsExpression = input.getArguments().stream().map(FhirPath::getExpression)
        .collect(
            Collectors.joining(", "));
    final String functionExpression = functionName + "(" + argumentsExpression + ")";

    // If the input expression is the same as the input context, the child will be the start of the
    // expression. This is to account for where we omit the expression that represents the input
    // expression, e.g. "gender" instead of "Patient.gender".
    final String inputContextExpression = input.getContext().getInputContext().getExpression();
    return inputExpression.equals(inputContextExpression)
           ? functionExpression
           : inputExpression + "." + functionExpression;
  }

}
