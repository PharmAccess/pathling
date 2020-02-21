/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright © 2018-2020, Commonwealth Scientific
 * and Industrial Research Organisation (CSIRO) ABN 41 687 119 230. Licensed
 * under the CSIRO Open Source Software Licence Agreement.
 */

package au.csiro.pathling.encoders.codes.broadcast;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

/**
 * A collection of BroadcastableConceptMaps that is usable in Spark transformations or UDFs.
 */
public class BroadcastableMappings implements Serializable {

  /**
   * Map of concept map URIs to broadcastable maps.
   */
  private Map<String, BroadcastableConceptMap> conceptMaps;

  public BroadcastableMappings(Map<String, BroadcastableConceptMap> conceptMaps) {
    this.conceptMaps = conceptMaps;
  }

  /**
   * Returns the broadcastable concept map for the concept map with the given URI.
   *
   * @param conceptMapUri URI of the concept map
   * @return the broadcastable concept map.
   */
  public BroadcastableConceptMap getBroadcastConceptMap(String conceptMapUri) {

    BroadcastableConceptMap map = conceptMaps.get(conceptMapUri);

    if (map == null) {
      BroadcastableConceptMap emptyMap = new BroadcastableConceptMap(
          conceptMapUri,
          Collections.emptyList());

      conceptMaps.put(conceptMapUri, emptyMap);

      map = emptyMap;
    }

    return map;
  }
}
