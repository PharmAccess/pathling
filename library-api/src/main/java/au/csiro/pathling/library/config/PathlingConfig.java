package au.csiro.pathling.library.config;

import javax.annotation.Nullable;
import java.util.List;

public class PathlingConfig {

  @Nullable
  String fhirVersion;

  @Nullable
  Integer maxNestingLevel;

  @Nullable
  Boolean extensionsEnabled;

  @Nullable
  List<String> openTypesEnabled;

  @Nullable
  String terminologyServerUrl;

  @Nullable
  AuthConfig authConfig;
}
