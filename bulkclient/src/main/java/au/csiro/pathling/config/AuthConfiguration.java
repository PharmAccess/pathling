/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.config;

import au.csiro.pathling.config.AuthConfiguration.ValidAuthConfiguration;
import java.io.Serializable;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import javax.annotation.Nullable;
import javax.validation.Constraint;
import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import javax.validation.Payload;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import lombok.Builder;
import lombok.Data;
import lombok.ToString;
import org.hibernate.validator.constraints.URL;

/**
 * Configuration relating to authentication of requests to the terminology service.
 *
 * @author John Grimes
 */
@Data
@Builder
@ValidAuthConfiguration
public class AuthConfiguration implements Serializable {

  private static final long serialVersionUID = 6321330066417583745L;

  /**
   * Enables authentication of requests to the terminology server.
   */
  @NotNull
  @Builder.Default
  private boolean enabled = false;

  /**
   * An OAuth2 token endpoint for use with the client credentials grant.
   */
  @Nullable
  @URL
  private String tokenEndpoint;

  /**
   * A client ID for use with the client credentials grant.
   */
  @Nullable
  private String clientId;

  /**
   * A client secret for use with the symmetric client authentication.
   */
  @Nullable
  @ToString.Exclude
  private String clientSecret;

  /**
   * A private key in JWK format to use with the asymmetric client authentication.
   */
  @Nullable
  @ToString.Exclude
  private String privateKeyJWK;


  /**
   * Send the basic authentication credentials in the form body instead of the Authorization
   * header.
   */
  @Builder.Default
  private boolean useFormForBasicAuth = false;

  /**
   * A scope value for use with the client credentials grant.
   */
  @Nullable
  private String scope;

  /**
   * The minimum number of seconds that a token should have before expiry when deciding whether to
   * send it with a terminology request.
   */
  @NotNull
  @Min(0)
  @Builder.Default
  private long tokenExpiryTolerance = 120;

  @Target({ElementType.TYPE, ElementType.ANNOTATION_TYPE})
  @Retention(RetentionPolicy.RUNTIME)
  @Constraint(validatedBy = AuthConfigValidator.class)
  @Documented
  public @interface ValidAuthConfiguration {

    String message() default "If terminology authentication is enabled, token endpoint, "
        + "client ID and client secret must be supplied.";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};

  }

  public static class AuthConfigValidator implements
      ConstraintValidator<ValidAuthConfiguration, AuthConfiguration> {

    @Override
    public boolean isValid(final AuthConfiguration value,
        final ConstraintValidatorContext context) {
      if (value.isEnabled()) {
        return value.getTokenEndpoint() != null && value.getClientId() != null
            && value.getClientSecret() != null;
      }
      return true;
    }
  }
}
