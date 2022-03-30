/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.update;

import static au.csiro.pathling.fhir.FhirServer.resourceTypeFromClass;
import static au.csiro.pathling.io.Database.prepareResourceForUpdate;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;

import au.csiro.pathling.io.Database;
import au.csiro.pathling.security.OperationAccess;
import ca.uhn.fhir.rest.annotation.IdParam;
import ca.uhn.fhir.rest.annotation.ResourceParam;
import ca.uhn.fhir.rest.annotation.Update;
import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.server.IResourceProvider;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.IdType;
import org.springframework.context.annotation.Profile;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

/**
 * HAPI resource provider that provides update-related operations, such as create and update.
 *
 * @author John Grimes
 * @author Sean Fong
 */
@Component
@Scope("prototype")
@Profile("server")
public class UpdateProvider implements IResourceProvider {

  @Nonnull
  private final Database database;

  @Nonnull
  private final Class<? extends IBaseResource> resourceClass;

  @Nonnull
  private final ResourceType resourceType;

  public UpdateProvider(@Nonnull final Database database,
      @Nonnull final Class<? extends IBaseResource> resourceClass) {
    this.database = database;
    this.resourceClass = resourceClass;
    resourceType = resourceTypeFromClass(resourceClass);
  }

  @Override
  public Class<? extends IBaseResource> getResourceType() {
    return resourceClass;
  }

  /**
   * Implements the update operation.
   *
   * @param id the id of the resource to be updated
   * @param resource the resource to be updated
   * @return a {@link MethodOutcome} describing the result of the operation
   * @see <a href="https://hl7.org/fhir/R4/http.html#update">update</a>
   */
  @Update
  @OperationAccess("update")
  @SuppressWarnings("UnusedReturnValue")
  public MethodOutcome update(@Nullable @IdParam final IdType id,
      @Nullable @ResourceParam final IBaseResource resource) {
    checkUserInput(id != null && !id.isEmpty(), "ID must be supplied");
    checkUserInput(resource != null, "Resource must be supplied");

    final String resourceId = id.getIdPart();
    final IBaseResource preparedResource = prepareResourceForUpdate(resource,
        resourceId);
    database.merge(resourceType, preparedResource);

    final MethodOutcome outcome = new MethodOutcome();
    outcome.setId(resource.getIdElement());
    outcome.setResource(preparedResource);
    return outcome;
  }

}
