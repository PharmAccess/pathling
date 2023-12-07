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

package au.csiro.pathling.fhirpath.function;

import static au.csiro.pathling.utilities.Preconditions.checkArgument;

import au.csiro.pathling.fhirpath.TypeSpecifier;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import au.csiro.pathling.fhirpath.collection.StringCollection;
import au.csiro.pathling.fhirpath.column.ColumnCtx;
import au.csiro.pathling.fhirpath.validation.FhirpathFunction;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.spark.sql.Column;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Implementation of FHIRPath functions defined for FHIRViews.
 */
@SuppressWarnings("unused")
public class FhirViewFunctions {

  public static final String REFERENCE_ELEMENT_NAME = "reference";

  @FhirpathFunction
  public static StringCollection getResourceKey(@Nonnull final ResourceCollection input) {
    return StringCollection.build(input.getKeyColumn());
  }

  @FhirpathFunction
  // TODO: This needs to be somehow constrained to the collections of References
  public static Collection getReferenceKey(@Nonnull final Collection input,
      @Nullable final TypeSpecifier typeSpecifier) {
    checkArgument(input.getFhirType().map(FHIRDefinedType.REFERENCE::equals).orElse(false),
        "getReferenceKey can only be applied to a REFERENCE collection");
    // TODO: How to deal with exceptions here?
    // TODO: add filtering on 'type' but that requies changes in the Encoder (as 'type' is not encoded)
    // TODO: add support for other types of references
    return Optional.ofNullable(typeSpecifier)
        .map(ts -> ts.toFhirType().toCode() + "/.+")
        .<Function<ColumnCtx, ColumnCtx>>map(
            regex -> (c -> c.getField(REFERENCE_ELEMENT_NAME).rlike(regex)))
        .map(input::filter).orElse(input)
        .traverse(REFERENCE_ELEMENT_NAME).orElse(Collection.nullCollection());
  }
}
