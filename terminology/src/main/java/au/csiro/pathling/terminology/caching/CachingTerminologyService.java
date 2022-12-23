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

package au.csiro.pathling.terminology.caching;

import static au.csiro.pathling.utilities.Preconditions.checkPresent;

import au.csiro.pathling.config.HttpClientCachingConfiguration;
import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.fhirpath.encoding.ImmutableCoding;
import au.csiro.pathling.terminology.BaseTerminologyService;
import au.csiro.pathling.terminology.TerminologyOperation;
import au.csiro.pathling.terminology.TerminologyParameters;
import au.csiro.pathling.terminology.TerminologyResult;
import au.csiro.pathling.terminology.lookup.LookupExecutor;
import au.csiro.pathling.terminology.lookup.LookupParameters;
import au.csiro.pathling.terminology.subsumes.SubsumesExecutor;
import au.csiro.pathling.terminology.subsumes.SubsumesParameters;
import au.csiro.pathling.terminology.translate.TranslateExecutor;
import au.csiro.pathling.terminology.translate.TranslateParameters;
import au.csiro.pathling.terminology.validatecode.ValidateCodeExecutor;
import au.csiro.pathling.terminology.validatecode.ValidateCodeParameters;
import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.gclient.IOperationUntypedWithInput;
import ca.uhn.fhir.rest.server.exceptions.BaseServerResponseException;
import ca.uhn.fhir.rest.server.exceptions.NotModifiedException;
import java.io.Closeable;
import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.ws.rs.core.CacheControl;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.codesystems.ConceptSubsumptionOutcome;
import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * A terminology service that uses embedded Infinispan to cache the results of the underlying
 * terminology service operations.
 *
 * @author John Grimes
 */
public abstract class CachingTerminologyService extends BaseTerminologyService {

  public static final String VALIDATE_CODE_CACHE_NAME = "validate-code";
  public static final String SUBSUMES_CACHE_NAME = "subsumes";
  public static final String TRANSLATE_CACHE_NAME = "translate";
  public static final String LOOKUP_CACHE_NAME = "lookup";
  public static final String ETAG_HEADER_NAME = "etag";
  public static final String IF_NONE_MATCH_HEADER_NAME = "if-none-match";
  public static final String CACHE_CONTROL_HEADER_NAME = "cache-control";

  @Nonnull
  protected final HttpClientCachingConfiguration configuration;

  @Nonnull
  protected final EmbeddedCacheManager cacheManager;

  @Nonnull
  protected final Cache<Integer, TerminologyResult<Boolean>> validateCodeCache;

  @Nonnull
  protected final Cache<Integer, TerminologyResult<ConceptSubsumptionOutcome>> subsumesCache;

  @Nonnull
  protected final Cache<Integer, TerminologyResult<ArrayList<Translation>>> translateCache;

  @Nonnull
  protected final Cache<Integer, TerminologyResult<ArrayList<PropertyOrDesignation>>> lookupCache;

  @SuppressWarnings("unchecked")
  public CachingTerminologyService(@Nonnull final TerminologyClient terminologyClient,
      @Nullable final Closeable toClose,
      @Nonnull final HttpClientCachingConfiguration configuration) {
    super(terminologyClient, toClose);
    this.configuration = configuration;
    cacheManager = buildCacheManager();
    validateCodeCache = (Cache<Integer, TerminologyResult<Boolean>>) buildCache(cacheManager,
        VALIDATE_CODE_CACHE_NAME);
    subsumesCache = (Cache<Integer, TerminologyResult<ConceptSubsumptionOutcome>>) buildCache(
        cacheManager, SUBSUMES_CACHE_NAME);
    translateCache = (Cache<Integer, TerminologyResult<ArrayList<Translation>>>) buildCache(
        cacheManager, TRANSLATE_CACHE_NAME);
    lookupCache = (Cache<Integer, TerminologyResult<ArrayList<PropertyOrDesignation>>>) buildCache(
        cacheManager, LOOKUP_CACHE_NAME);
  }

  @Override
  public boolean validateCode(@Nonnull final String valueSetUrl, @Nonnull final Coding coding) {
    final ValidateCodeParameters parameters = new ValidateCodeParameters(valueSetUrl,
        ImmutableCoding.of(coding));
    final ValidateCodeExecutor executor = new ValidateCodeExecutor(terminologyClient, parameters);
    return getFromCache(validateCodeCache, parameters, executor);
  }

  @Nonnull
  @Override
  public List<Translation> translate(@Nonnull final Coding coding,
      @Nonnull final String conceptMapUrl,
      final boolean reverse, @Nullable final String target) {
    final TranslateParameters parameters = new TranslateParameters(ImmutableCoding.of(coding),
        conceptMapUrl, reverse, target);
    final TranslateExecutor executor = new TranslateExecutor(terminologyClient, parameters);
    return getFromCache(translateCache, parameters, executor);
  }

  @Nonnull
  @Override
  public ConceptSubsumptionOutcome subsumes(@Nonnull final Coding codingA,
      @Nonnull final Coding codingB) {
    final SubsumesParameters parameters = new SubsumesParameters(
        ImmutableCoding.of(codingA), ImmutableCoding.of(codingB));
    final SubsumesExecutor executor = new SubsumesExecutor(terminologyClient, parameters);
    return getFromCache(subsumesCache, parameters, executor);
  }

  @Nonnull
  @Override
  public List<PropertyOrDesignation> lookup(@Nonnull final Coding coding,
      @Nullable final String property) {
    final LookupParameters parameters = new LookupParameters(ImmutableCoding.of(coding), property);
    final LookupExecutor executor = new LookupExecutor(terminologyClient, parameters);
    return getFromCache(lookupCache, parameters, executor);
  }

  /**
   * Gets the result of an operation from the cache, or fetches a new result if the cache is empty
   * or expired.
   *
   * @param cache The cache being used for this operation
   * @param parameters The parameters for the operation
   * @param operation A {@link TerminologyOperation} that provides the behavior specific to the type
   * of operation
   * @param <ParametersType> The type of the parameters that are input to the operation
   * @param <ResponseType> The type of the response returned by the terminology client
   * @param <ResultType> The type of the final result that is extracted from the response
   * @return The operation result
   */
  private <ParametersType extends TerminologyParameters, ResponseType, ResultType extends Serializable> ResultType getFromCache(
      @Nonnull final Cache<Integer, TerminologyResult<ResultType>> cache,
      @Nonnull final ParametersType parameters,
      @Nonnull final TerminologyOperation<ResponseType, ResultType> operation
  ) {
    final int key = parameters.hashCode();
    final TerminologyResult<ResultType> cached = cache.get(key);

    if (cached == null) {
      // Cache miss.
      final TerminologyResult<ResultType> result = fetch(operation, Optional.empty());
      cache.put(key, result);
      return result.getData();
    } else {
      final boolean expired =
          cached.getExpires() != null && System.currentTimeMillis() > cached.getExpires();
      return cache.compute(key,
          (k, v) -> expired
                    // Cache hit, but the entry is expired and needs to be revalidated.
                    ? fetch(operation, Optional.ofNullable(v))
                    // Cache hit, and the entry is still valid.
                    : v).getData();
    }
  }

  /**
   * Fetches an operation result, or revalidates it if the cached result is still valid.
   *
   * @param operation A {@link TerminologyOperation} that provides the behavior specific to the type
   * of operation
   * @param cached A previously cached value
   * @param <ResponseType> The type of the response returned by the terminology client
   * @param <ResultType> The type of the final result that is extracted from the response
   * @return The operation result
   */
  private <ResponseType, ResultType extends Serializable> TerminologyResult<ResultType> fetch(
      @Nonnull final TerminologyOperation<ResponseType, ResultType> operation,
      @Nonnull final Optional<TerminologyResult<ResultType>> cached) {
    final Optional<ResultType> invalidResult = operation.validate();
    if (invalidResult.isPresent()) {
      // If the parameters fail validation, cache the invalid result forever.
      return new TerminologyResult<>(invalidResult.get(), null, null, false);
    }

    final IOperationUntypedWithInput<ResponseType> request = operation.buildRequest();

    // Add an If-None-Match header if the cached result is accompanied by an ETag.
    cached.flatMap(c -> Optional.ofNullable(c.getETag()))
        .ifPresent(eTag -> request.withAdditionalHeader(IF_NONE_MATCH_HEADER_NAME, eTag));

    // We use a default expiry if the server does not provide one.
    final long defaultExpires = secondsFromNow(configuration.getDefaultExpiry());

    try {
      final MethodOutcome outcome = request.returnMethodOutcome().execute();

      // If the response was 200 OK, use the data from the fresh response.
      @SuppressWarnings("unchecked") final ResultType result = operation.extractResult(
          (ResponseType) outcome.getResource());
      final Optional<String> newETag = getSingularHeader(outcome, ETAG_HEADER_NAME);
      final Optional<Long> expires = getExpires(outcome);
      return new TerminologyResult<>(
          result, newETag.orElse(null), expires.orElse(defaultExpires), false);

    } catch (final NotModifiedException e) {
      // If the response was 304 Not Modified, use the data from the cached response and update 
      // the ETag and expiry.
      final Optional<String> newETag = getSingularHeader(e.getResponseHeaders(), ETAG_HEADER_NAME);
      final Optional<Long> expires = getExpires(e.getResponseHeaders());
      final TerminologyResult<ResultType> previous = checkPresent(cached);
      return new TerminologyResult<>(previous.getData(), newETag.orElse(previous.getETag()),
          expires.orElse(previous.getExpires()), false);

    } catch (final BaseServerResponseException e) {
      // If the terminology server rejects the request as invalid, cache the invalid result for the 
      // amount of time instructed by the server. If there is no such instruction, cache it for the 
      // configured default expiry.
      final long expires = getExpires(e.getResponseHeaders()).orElse(
          defaultExpires);
      final TerminologyResult<ResultType> fallback = new TerminologyResult<>(
          operation.invalidRequestFallback(), null, expires, false);
      return handleError(e, fallback);
    }
  }

  /**
   * @return a new {@link EmbeddedCacheManager} instance appropriate for the specific implementation
   */
  protected abstract EmbeddedCacheManager buildCacheManager();

  /**
   * @param cacheManager the {@link EmbeddedCacheManager} to use to construct the cache
   * @param cacheName a name for the cache
   * @return a new {@link Cache} instance appropriate for the specific implementation
   */
  protected abstract Cache<Integer, ?> buildCache(@Nonnull final EmbeddedCacheManager cacheManager,
      @Nonnull final String cacheName);

  @SuppressWarnings("SameParameterValue")
  @Nonnull
  private static Optional<String> getSingularHeader(@Nonnull final MethodOutcome outcome,
      @Nonnull final String headerName) {
    return getSingularHeader(outcome.getResponseHeaders(), headerName);
  }

  @Nonnull
  private static Optional<String> getSingularHeader(
      @Nullable final Map<String, List<String>> headers,
      @Nonnull final String headerName) {
    return Optional.ofNullable(headers)
        .map(rh -> rh.get(headerName))
        .flatMap(h -> h.stream().findFirst());
  }

  @Nonnull
  private static Optional<Long> getExpires(@Nonnull final MethodOutcome outcome) {
    return getExpires(outcome.getResponseHeaders());
  }

  @Nonnull
  private static Optional<Long> getExpires(@Nullable final Map<String, List<String>> headers) {
    final Optional<Integer> maxAge = getSingularHeader(headers, CACHE_CONTROL_HEADER_NAME)
        .map(CacheControl::valueOf)
        .map(CacheControl::getMaxAge);
    return maxAge.map(CachingTerminologyService::secondsFromNow);
  }

  private static long secondsFromNow(final int seconds) {
    return Instant.now().plusSeconds(seconds).toEpochMilli();
  }

}
