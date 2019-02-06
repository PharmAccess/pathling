/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight;

import ca.uhn.fhir.rest.annotation.Elements;
import ca.uhn.fhir.rest.annotation.IdParam;
import ca.uhn.fhir.rest.annotation.Read;
import ca.uhn.fhir.rest.annotation.Search;
import ca.uhn.fhir.rest.client.api.IBasicClient;
import java.util.List;
import java.util.Set;
import org.hl7.fhir.dstu3.model.IdType;
import org.hl7.fhir.dstu3.model.StructureDefinition;

/**
 * @author John Grimes
 */
public interface TerminologyClient extends IBasicClient {

  @Search
  List<StructureDefinition> getAllStructureDefinitions(@Elements Set<String> theElements);

  @Read
  StructureDefinition getStructureDefinitionById(@IdParam IdType theId);

}
