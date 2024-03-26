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

package au.csiro.pathling.view;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toUnmodifiableList;

import java.util.List;
import javax.annotation.Nonnull;
import lombok.Value;

@Value
public class GroupingSelection implements ProjectionClause {

  @Nonnull
  List<ProjectionClause> components;

  @Override
  @Nonnull
  public ProjectionResult evaluate(@Nonnull final ProjectionContext context) {
    // evaluate and cross join the subcomponents
    final List<ProjectionResult> subResults = components.stream().map(c -> c.evaluate(context))
        .collect(toUnmodifiableList());
    return ProjectionResult.combine(subResults);
  }

  @Override
  public String toString() {
    return "GroupingSelection{" +
        "components=[" + components.stream()
        .map(ProjectionClause::toString)
        .collect(joining(", ")) +
        "]}";
  }

}
