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

package au.csiro.pathling.fhirpath.definition;

import ca.uhn.fhir.context.BaseRuntimeChildDefinition;
import ca.uhn.fhir.context.BaseRuntimeElementDefinition;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nonnull;

/**
 * Encapsulates the FHIR definitions for a child resolved to specific element.
 *
 * @author John Grimes
 */
public class ElementChildDefinition extends
    BaseNodeDefinition<BaseRuntimeElementDefinition<?>> implements
    ElementDefinition {

  @Nonnull
  protected final BaseRuntimeChildDefinition childDefinition;

  @Nonnull
  protected final String elementName;

  protected ElementChildDefinition(
      @Nonnull final BaseRuntimeElementDefinition<?> elementDefinition,
      @Nonnull final BaseRuntimeChildDefinition childDefinition,
      @Nonnull final String elementName) {
    super(elementDefinition);
    this.childDefinition = childDefinition;
    this.elementName = elementName;
  }

  protected ElementChildDefinition(@Nonnull final BaseRuntimeChildDefinition childDefinition,
      @Nonnull final String elementName) {
    this(Objects.requireNonNull(childDefinition.getChildByName(elementName)), childDefinition,
        elementName);
  }

  protected ElementChildDefinition(
      @Nonnull final BaseRuntimeChildDefinition childDefinition) {
    this(childDefinition, childDefinition.getElementName());
  }

  @Nonnull
  @Override
  public String getElementName() {
    return elementName;
  }

  @Nonnull
  @Override
  public String getName() {
    return childDefinition.getElementName();
  }

  @Override
  public Optional<Integer> getMaxCardinality() {
    return Optional.of(childDefinition.getMax());
  }
}
