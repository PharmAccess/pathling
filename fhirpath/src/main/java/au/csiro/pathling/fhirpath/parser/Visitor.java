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

package au.csiro.pathling.fhirpath.parser;

import static java.util.Objects.requireNonNull;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.operator.Operator;
import au.csiro.pathling.fhirpath.operator.OperatorInput;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathBaseVisitor;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.AdditiveExpressionContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.AndExpressionContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.CombineExpressionContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.EqualityExpressionContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.ImpliesExpressionContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.IndexerExpressionContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.InequalityExpressionContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.InvocationExpressionContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.MembershipExpressionContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.MultiplicativeExpressionContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.OrExpressionContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.PolarityExpressionContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.TermExpressionContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.TypeExpressionContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.UnionExpressionContext;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.antlr.v4.runtime.tree.ParseTree;

/**
 * This class processes all types of expressions, and delegates the special handling of supported
 * types to the more specific visitor classes.
 *
 * @author John Grimes
 */
class Visitor extends FhirPathBaseVisitor<FhirPath> {

  @Nonnull
  private final ParserContext context;

  Visitor(@Nonnull final ParserContext context) {
    this.context = context;
  }

  /**
   * A term is typically a standalone literal or function invocation.
   *
   * @param ctx The {@link TermExpressionContext}
   * @return A {@link FhirPath} expression
   */
  @Override
  @Nonnull
  public FhirPath visitTermExpression(@Nullable final TermExpressionContext ctx) {
    return requireNonNull(ctx).term().accept(new TermVisitor(context));
  }

  /**
   * An invocation expression is one expression invoking another using the dot notation.
   *
   * @param ctx The {@link InvocationExpressionContext}
   * @return A {@link FhirPath} expression
   */
  @Override
  @Nonnull
  public FhirPath visitInvocationExpression(@Nullable final InvocationExpressionContext ctx) {
    final FhirPath expressionResult = new Visitor(context).visit(
        requireNonNull(ctx).expression());
    // The input context is passed through to the invocation visitor as the invoker.
    return ctx.invocation().accept(new InvocationVisitor(context, expressionResult));
  }

  @Nonnull
  private FhirPath visitBinaryOperator(@Nullable final ParseTree leftContext,
      @Nullable final ParseTree rightContext, @Nullable final String operatorName) {
    requireNonNull(operatorName);

    // Parse the left and right expressions.
    final FhirPath left = new Visitor(context).visit(leftContext);
    // If the root nesting level has been erased by the parsing of the left expression (i.e. there has been aggregation or use of where)
    final ParserContext rightParserContext = context.getNesting().isRootErased()
                                             ? context.disaggregate(left)
                                             : context.withContextDataset(left.getDataset());
    final FhirPath right = new Visitor(rightParserContext)
        .visit(rightContext);

    // Retrieve an Operator instance based upon the operator string.
    final Operator operator = Operator.getInstance(operatorName);

    final OperatorInput operatorInput = new OperatorInput(context, left, right);
    return operator.invoke(operatorInput);
  }

  @Override
  @Nonnull
  public FhirPath visitEqualityExpression(@Nullable final EqualityExpressionContext ctx) {
    return visitBinaryOperator(requireNonNull(ctx).expression(0), ctx.expression(1),
        ctx.children.get(1).toString());
  }

  @Override
  public FhirPath visitInequalityExpression(@Nullable final InequalityExpressionContext ctx) {
    return visitBinaryOperator(requireNonNull(ctx).expression(0), ctx.expression(1),
        ctx.children.get(1).toString());
  }

  @Override
  @Nonnull
  public FhirPath visitAndExpression(@Nullable final AndExpressionContext ctx) {
    return visitBinaryOperator(requireNonNull(ctx).expression(0), ctx.expression(1),
        ctx.children.get(1).toString());
  }

  @Override
  @Nonnull
  public FhirPath visitOrExpression(@Nullable final OrExpressionContext ctx) {
    return visitBinaryOperator(requireNonNull(ctx).expression(0), ctx.expression(1),
        ctx.children.get(1).toString());
  }

  @Override
  @Nonnull
  public FhirPath visitImpliesExpression(@Nullable final ImpliesExpressionContext ctx) {
    return visitBinaryOperator(requireNonNull(ctx).expression(0), ctx.expression(1),
        ctx.children.get(1).toString());
  }

  @Override
  @Nonnull
  public FhirPath visitMembershipExpression(@Nullable final MembershipExpressionContext ctx) {
    return visitBinaryOperator(requireNonNull(ctx).expression(0), ctx.expression(1),
        ctx.children.get(1).toString());
  }

  @Override
  @Nonnull
  public FhirPath visitMultiplicativeExpression(
      @Nullable final MultiplicativeExpressionContext ctx) {
    return visitBinaryOperator(requireNonNull(ctx).expression(0), ctx.expression(1),
        ctx.children.get(1).toString());
  }

  @Override
  @Nonnull
  public FhirPath visitAdditiveExpression(@Nullable final AdditiveExpressionContext ctx) {
    return visitBinaryOperator(requireNonNull(ctx).expression(0), ctx.expression(1),
        ctx.children.get(1).toString());
  }

  @Override
  public FhirPath visitCombineExpression(@Nullable final CombineExpressionContext ctx) {
    return visitBinaryOperator(requireNonNull(ctx).expression(0), ctx.expression(1),
        ctx.children.get(1).toString());
  }

  // All other FHIRPath constructs are currently unsupported.

  @Override
  @Nonnull
  public FhirPath visitIndexerExpression(final IndexerExpressionContext ctx) {
    throw new InvalidUserInputError("Indexer operation is not supported");
  }

  @Override
  @Nonnull
  public FhirPath visitPolarityExpression(final PolarityExpressionContext ctx) {
    throw new InvalidUserInputError("Polarity operator is not supported");
  }

  @Override
  @Nonnull
  public FhirPath visitUnionExpression(final UnionExpressionContext ctx) {
    throw new InvalidUserInputError("Union expressions are not supported");
  }

  @Override
  @Nonnull
  public FhirPath visitTypeExpression(final TypeExpressionContext ctx) {
    throw new InvalidUserInputError("Type expressions are not supported");
  }

}
