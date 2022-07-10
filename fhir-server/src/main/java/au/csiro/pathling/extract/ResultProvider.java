/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.extract;

import static au.csiro.pathling.security.SecurityAspect.getCurrentUserId;
import static au.csiro.pathling.utilities.Preconditions.checkNotNull;

import au.csiro.pathling.errors.AccessDeniedError;
import au.csiro.pathling.errors.ResourceNotFoundError;
import au.csiro.pathling.io.ResultReader;
import au.csiro.pathling.security.OperationAccess;
import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.annotation.OperationParam;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.springframework.context.annotation.Profile;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

/**
 * @author John Grimes
 */
@Component
@Profile("server")
@Slf4j
public class ResultProvider {

  private static final Pattern ID_PATTERN = Pattern.compile("^\\w{1,50}$");

  @Nonnull
  private final ResultRegistry resultRegistry;

  @Nonnull
  private final ResultReader resultReader;

  /**
   * @param resultRegistry a {@link ResultRegistry} from which to retrieve result URLs
   * @param resultReader a {@link ResultReader} for reading in result files
   */
  public ResultProvider(@Nonnull final ResultRegistry resultRegistry,
      @Nonnull final ResultReader resultReader) {
    this.resultRegistry = resultRegistry;
    this.resultReader = resultReader;
  }

  /**
   * Enables the download of the result of an extract operation.
   *
   * @param id the ID of the extract request
   * @param response the {@link HttpServletResponse} for updating the response
   * @param requestDetails the {@link RequestDetails}, for preparing the request for downstream
   * processing
   */
  @SuppressWarnings({"unused", "TypeMayBeWeakened"})
  @OperationAccess("extract")
  @Operation(name = "$result", idempotent = true, manualResponse = true)
  public void result(@Nullable @OperationParam(name = "id") final String id,
      @Nullable final HttpServletResponse response, @Nullable final RequestDetails requestDetails) {
    checkNotNull(response);
    checkNotNull(requestDetails);

    // Validate that the ID looks reasonable.
    if (id == null || !ID_PATTERN.matcher(id).matches()) {
      throw new ResourceNotFoundError("Result ID not found");
    }

    log.info("Retrieving extract result: {}", id);
    final Result result = resultRegistry.get(id);
    // Check that the result exists.
    if (result == null) {
      throw new ResourceNotFoundError("Result ID not found");
    }

    // Check that the user requesting the result is the same user that started the job.
    final Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
    final Optional<String> currentUserId = getCurrentUserId(authentication);
    if (!result.getOwnerId().equals(currentUserId)) {
      throw new AccessDeniedError("The requested result is not owned by the current user");
    }

    // Open an input stream to read the result.
    final InputStream inputStream = resultReader.read(result);

    // Set the appropriate response headers.
    response.setHeader("Content-Type", "text/csv");

    // Opt out of processing the request using the ResponseHighlighterInterceptor.
    requestDetails.setAttribute("ResponseHighlighterInterceptorHandled", Boolean.TRUE);

    // Copy all data from the result to the HTTP response.
    try {
      IOUtils.copyLarge(inputStream, response.getOutputStream());
    } catch (final IOException e) {
      throw new RuntimeException("Problem writing result data to response: " + result, e);
    }
  }

}
