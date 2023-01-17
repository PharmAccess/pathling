package au.csiro.pathling.io;

import au.csiro.pathling.config.SparkConfiguration;
import au.csiro.pathling.config.StorageConfiguration;
import lombok.Value;
import lombok.experimental.NonFinal;
import org.springframework.validation.annotation.Validated;
import javax.validation.Valid;

@Value
@NonFinal
@Validated
public class DatabaseConfiguration {

  @Valid
  SparkConfiguration spark;

  @Valid
  StorageConfiguration storage;
}
