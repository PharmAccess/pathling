package au.csiro.pathling.library.config;

import lombok.Builder;
import lombok.Data;
import javax.annotation.Nullable;


@Data
@Builder
public class AuthConfig {

  @Nullable
  String tokenEndpoint;

  @Nullable
  String clientId;

  @Nullable
  String clientSecret;

  @Nullable
  String scope;

  @Nullable
  Integer tokenExpiryTolerance;
}
