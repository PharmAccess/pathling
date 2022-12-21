package au.csiro.pathling.sql.udf;

import static au.csiro.pathling.sql.udf.TerminologyUdfHelpers.decodeOneOrMany;
import static au.csiro.pathling.sql.udf.TerminologyUdfHelpers.encodeMany;
import static au.csiro.pathling.sql.udf.TerminologyUdfHelpers.validCodings;
import static java.util.Objects.nonNull;
import static java.util.function.Predicate.not;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.encoding.CodingEncoding;
import au.csiro.pathling.fhirpath.encoding.ImmutableCoding;
import au.csiro.pathling.terminology.TerminologyService2;
import au.csiro.pathling.terminology.TerminologyService2.Translation;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import com.google.common.collect.ImmutableSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.ConceptMapEquivalence;
import scala.collection.mutable.WrappedArray;
import scala.reflect.ClassTag;

/**
 * The implementation of the 'translate()' udf.
 */
@Slf4j
public class TranslateUdf implements SqlFunction,
    SqlFunction5<Object, String, Boolean, WrappedArray<String>, String, Row[]> {

  private static final long serialVersionUID = 7605853352299165569L;

  public static final Set<String> VALID_EQUIVALENCE_CODES = Stream.of(
          ConceptMapEquivalence.values())
      .map(ConceptMapEquivalence::toCode)
      .filter(Objects::nonNull)
      .collect(Collectors.toUnmodifiableSet());


  public static final Set<String> DEFAULT_EQUIVALENCES = ImmutableSet.of(
      ConceptMapEquivalence.EQUIVALENT.toCode());

  public static final String FUNCTION_NAME = "translate";
  public static final DataType RETURN_TYPE = DataTypes.createArrayType(CodingEncoding.DATA_TYPE);
  public static final boolean PARAM_REVERSE_DEFAULT = false;

  @Nonnull
  private final TerminologyServiceFactory terminologyServiceFactory;

  TranslateUdf(@Nonnull final TerminologyServiceFactory terminologyServiceFactory) {
    this.terminologyServiceFactory = terminologyServiceFactory;
  }

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public DataType getReturnType() {
    return RETURN_TYPE;
  }


  @Nonnull
  public static String checkValidEquivalenceCode(@Nonnull final String code) {
    if (!VALID_EQUIVALENCE_CODES.contains(code)) {
      throw new InvalidUserInputError(
          String.format("Unknown ConceptMapEquivalence code '%s'", code));
    } else {
      return code;
    }
  }

  @Nullable
  protected Stream<Coding> doCall(@Nullable final Stream<Coding> codings,
      @Nullable final String conceptMapUri, @Nullable Boolean reverse,
      @Nullable final String[] equivalences,
      @Nullable final String target) {
    if (codings == null || conceptMapUri == null) {
      return null;
    }

    final boolean resolvedReverse = reverse != null
                                    ? reverse
                                    : PARAM_REVERSE_DEFAULT;

    final Set<String> includeEquivalences = (equivalences == null)
                                            ? DEFAULT_EQUIVALENCES
                                            : toValidSetOfEquivalenceCodes(equivalences);

    if (includeEquivalences.isEmpty()) {
      return Stream.empty();
    }

    final TerminologyService2 terminologyService = terminologyServiceFactory.buildService2();
    return validCodings(codings)
        .flatMap(coding ->
            terminologyService.translate(coding, conceptMapUri, resolvedReverse, target).stream())
        .filter(entry -> includeEquivalences.contains(entry.getEquivalence().toCode()))
        .map(Translation::getConcept)
        .map(ImmutableCoding::of)
        .distinct()
        .map(ImmutableCoding::toCoding);
  }

  @Nullable
  @Override
  public Row[] call(@Nullable final Object codingRowOrArray, @Nullable final String conceptMapUri,
      @Nullable final Boolean reverse, @Nullable final WrappedArray<String> equivalences,
      @Nullable final String target) {

    //noinspection RedundantCast
    return encodeMany(
        doCall(decodeOneOrMany(codingRowOrArray), conceptMapUri, reverse,
            nonNull(equivalences)
            ? (String[]) equivalences.toArray(ClassTag.apply(String.class))
            : null,
            target));
  }

  @Nonnull
  private Set<String> toValidSetOfEquivalenceCodes(@Nonnull String[] equivalences) {
    return Stream.of(equivalences)
        .filter(Objects::nonNull)
        .filter(not(String::isEmpty))
        .map(TranslateUdf::checkValidEquivalenceCode)
        .collect(Collectors.toUnmodifiableSet());
  }
}
