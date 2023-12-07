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
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.DateCollection;
import au.csiro.pathling.fhirpath.collection.DateTimeCollection;
import au.csiro.pathling.fhirpath.collection.DecimalCollection;
import au.csiro.pathling.fhirpath.collection.IntegerCollection;
import au.csiro.pathling.fhirpath.collection.TimeCollection;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathBaseVisitor;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.BooleanLiteralContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.CodingLiteralContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.DateLiteralContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.DateTimeLiteralContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.NullLiteralContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.NumberLiteralContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.QuantityLiteralContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.StringLiteralContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.TimeLiteralContext;
import au.csiro.pathling.fhirpath.path.Literals.BooleanLiteral;
import au.csiro.pathling.fhirpath.path.Literals.CalendarDurationLiteral;
import au.csiro.pathling.fhirpath.path.Literals.CodingLiteral;
import au.csiro.pathling.fhirpath.path.Literals.StringLiteral;
import au.csiro.pathling.fhirpath.path.Literals.UcumQuantityLiteral;
import java.text.ParseException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import au.csiro.pathling.fhirpath.path.Paths.StringLiteral;
import org.antlr.v4.runtime.tree.TerminalNode;

/**
 * This class deals with terms that are literal expressions.
 *
 * @author John Grimes
 */
class LiteralTermVisitor extends FhirPathBaseVisitor<FhirPath<Collection>> {

  @Override
  @Nonnull
  public FhirPath<Collection> visitStringLiteral(
      @Nullable final StringLiteralContext ctx) {
    @Nullable final String fhirPath = requireNonNull(ctx).getText();
    requireNonNull(fhirPath);
    return new StringLiteral(fhirPath);
  }

  @Override
  public FhirPath<Collection> visitDateLiteral(@Nullable final DateLiteralContext ctx) {
    @Nullable final String fhirPath = requireNonNull(ctx).getText();
    requireNonNull(fhirPath);

    return (input, context) -> {
      try {
        return DateCollection.fromLiteral(fhirPath);
      } catch (final ParseException e) {
        throw new InvalidUserInputError("Unable to parse date format: " + fhirPath);
      }
    };
  }

  @Override
  @Nonnull
  public FhirPath<Collection> visitDateTimeLiteral(
      @Nullable final DateTimeLiteralContext ctx) {
    @Nullable final String fhirPath = requireNonNull(ctx).getText();
    requireNonNull(fhirPath);

    return (input, context) -> {
      try {
        return DateTimeCollection.fromLiteral(fhirPath);
      } catch (final ParseException e) {
        throw new InvalidUserInputError("Unable to parse date/time format: " + fhirPath);
      }
    };
  }

  @Override
  @Nonnull
  public FhirPath<Collection> visitTimeLiteral(@Nullable final TimeLiteralContext ctx) {
    @Nullable final String fhirPath = requireNonNull(ctx).getText();
    requireNonNull(fhirPath);

    return (input, context) -> TimeCollection.fromLiteral(fhirPath);
  }

  @Override
  @Nonnull
  public FhirPath<Collection> visitNumberLiteral(
      @Nullable final NumberLiteralContext ctx) {
    @Nullable final String fhirPath = requireNonNull(ctx).getText();
    requireNonNull(fhirPath);

    return (input, context) -> {
      // The FHIRPath grammar lumps these two types together, so we tease them apart by trying to 
      // parse them.
      try {
        return IntegerCollection.fromLiteral(fhirPath);
      } catch (final NumberFormatException e) {
        try {
          return DecimalCollection.fromLiteral(fhirPath);
        } catch (final NumberFormatException ex) {
          throw new InvalidUserInputError("Invalid date format: " + fhirPath);
        }
      }
    };
  }

  @Override
  @Nonnull
  public FhirPath<Collection> visitBooleanLiteral(
      @Nullable final BooleanLiteralContext ctx) {
    return new BooleanLiteral(requireNonNull(requireNonNull(ctx).getText()));
  }

  @Override
  @Nonnull
  public FhirPath<Collection> visitNullLiteral(@Nullable final NullLiteralContext ctx) {
    return (input, context) -> Collection.nullCollection();
  }

  @Override
  @Nonnull
  public FhirPath<Collection> visitQuantityLiteral(
      @Nullable final QuantityLiteralContext ctx) {
    requireNonNull(ctx);
    @Nullable final String number = ctx.quantity().NUMBER().getText();
    requireNonNull(number);
    @Nullable final TerminalNode ucumUnit = ctx.quantity().unit().STRING();
    return (ucumUnit == null)
           ? new CalendarDurationLiteral(
        String.format("%s %s", number, ctx.quantity().unit().getText()))
           : new UcumQuantityLiteral(String.format("%s %s", number, ucumUnit.getText()));
  }

  @Override
  @Nonnull
  public FhirPath<Collection> visitCodingLiteral(
      @Nullable final CodingLiteralContext ctx) {
    return new CodingLiteral(requireNonNull(requireNonNull(ctx).getText()));
  }

}
