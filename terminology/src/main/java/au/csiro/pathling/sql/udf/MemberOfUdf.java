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

package au.csiro.pathling.sql.udf;

import static au.csiro.pathling.sql.udf.TerminologyUdfHelpers.decodeOneOrMany;
import static au.csiro.pathling.sql.udf.TerminologyUdfHelpers.validCodings;

import au.csiro.pathling.terminology.TerminologyService2;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Coding;

/**
 * The implementation of the 'member_of' UDF.
 */
@Slf4j
public class MemberOfUdf implements SqlFunction,
    SqlFunction2<Object, String, Boolean> {

  private static final long serialVersionUID = 7605853352299165569L;

  public static final String FUNCTION_NAME = "member_of";

  @Nonnull
  private final TerminologyServiceFactory terminologyServiceFactory;

  MemberOfUdf(@Nonnull final TerminologyServiceFactory terminologyServiceFactory) {
    this.terminologyServiceFactory = terminologyServiceFactory;
  }

  @Override
  public DataType getReturnType() {
    return DataTypes.BooleanType;
  }

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Nullable
  @Override
  public Boolean call(@Nullable final Object codingRowOrArray, @Nullable final String url) {
    return doCall(decodeOneOrMany(codingRowOrArray), url);
  }

  @Nullable
  protected Boolean doCall(@Nullable final Stream<Coding> codings, @Nullable final String url) {
    if (url == null || codings == null) {
      return null;
    }
    final TerminologyService2 terminologyService = terminologyServiceFactory.buildService2();
    return validCodings(codings)
        .anyMatch(coding -> terminologyService.validateCode(url, coding));
  }
}
