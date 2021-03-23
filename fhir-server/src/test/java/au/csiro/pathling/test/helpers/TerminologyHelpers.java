/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.helpers;

import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.ConceptMapEquivalence;
import org.hl7.fhir.r4.model.ValueSet;

public final class TerminologyHelpers {

  private TerminologyHelpers() {
  }

  private static final String SNOMED_URI = "http://snomed.info/sct";

  public static final String CM_HIST_ASSOCIATIONS = "http://snomed.info/sct?fhir_cm=900000000000526001";

  public static final List<ConceptMapEquivalence> ALL_EQUIVALENCES = Arrays
      .asList(ConceptMapEquivalence.values());

  public static Coding CD_SNOMED_720471000168102 = snomedCoding("720471000168102",
      "Duodopa intestinal gel");
  public static Coding CD_SNOMED_72940011000036107 = snomedCoding("72940011000036107",
      "Duodopa gel: intestinal");


  public static Coding CD_SNOMED_403190006 = snomedCoding("403190006",
      "Epidermal burn of skin");
  public static Coding CD_SNOMED_284551006 = snomedCoding("284551006",
      "Laceration of foot");


  @Nonnull
  public static SimpleCoding snomedSimple(@Nonnull final String code) {
    return new SimpleCoding(SNOMED_URI, code);
  }

  @Nonnull
  public static SimpleCoding testSimple(@Nonnull final String code) {
    return new SimpleCoding(SNOMED_URI, code);
  }

  @Nonnull
  public static SimpleCoding simpleOf(@Nonnull final Coding code) {
    return new SimpleCoding(code);
  }

  @Nonnull
  public static Set<SimpleCoding> setOfSimpleFrom(@Nonnull final Coding... codings) {
    return Arrays.stream(codings).map(TerminologyHelpers::simpleOf)
        .collect(Collectors.toUnmodifiableSet());
  }

  @Nonnull
  public static Set<SimpleCoding> setOfSimpleFrom(@Nonnull final CodeableConcept... validMembers) {
    return Arrays.stream(validMembers)
        .flatMap(codeableConcept -> codeableConcept.getCoding().stream())
        .map(TerminologyHelpers::simpleOf)
        .collect(Collectors.toUnmodifiableSet());
  }

  @Nonnull
  public static Set<SimpleCoding> setOfSimpleFrom(@Nonnull final ValueSet valueSet) {
    // TODO: move to MemeberOfMapping
    return valueSet.getExpansion().getContains().stream()
        .map(contains -> new SimpleCoding(contains.getSystem(), contains.getCode(),
            contains.getVersion()))
        .collect(Collectors.toSet());
  }

  @Nonnull
  private static Coding snomedCoding(@Nonnull final String code, @Nonnull final String dislayName) {
    return new Coding(SNOMED_URI, code, dislayName);
  }

}
