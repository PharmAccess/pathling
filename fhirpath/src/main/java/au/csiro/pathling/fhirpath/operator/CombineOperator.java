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

package au.csiro.pathling.fhirpath.operator;

import static au.csiro.pathling.QueryHelpers.createColumn;

import au.csiro.pathling.QueryHelpers.DatasetWithColumn;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.NonLiteralPath;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Merges the left and right operands into a single collection.
 *
 * @author John Grimes
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/operators.html#combine">combine</a>
 */
public class CombineOperator implements Operator {

  private static final String NAME = "combine";

  @Nonnull
  @Override
  public FhirPath invoke(@Nonnull final OperatorInput input) {
    final String expression = Operator.buildExpression(input, NAME);
    final FhirPath left = input.getLeft();
    final FhirPath right = input.getRight();

    final Dataset<Row> leftTrimmed = left.getUnionableDataset(right);
    final Dataset<Row> rightTrimmed = right.getUnionableDataset(left);

    final Dataset<Row> dataset = leftTrimmed.union(rightTrimmed);
    final DatasetWithColumn datasetWithColumn = createColumn(dataset, left.getValueColumn());
    final Optional<Column> thisColumn = left instanceof NonLiteralPath
                                        ? ((NonLiteralPath) left).getThisColumn()
                                        : Optional.empty();
    return left.combineWith(right, datasetWithColumn.getDataset(), expression, left.getIdColumn(),
        datasetWithColumn.getColumn(), false, thisColumn);
  }

}
