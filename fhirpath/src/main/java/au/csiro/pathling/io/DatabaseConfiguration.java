package au.csiro.pathling.io;

import au.csiro.pathling.config.SparkConfiguration;
import au.csiro.pathling.config.StorageConfiguration;
import lombok.Value;
import org.springframework.validation.annotation.Validated;
import javax.validation.Valid;

@Value
@Validated
public class DatabaseConfiguration {

  @Valid
  SparkConfiguration spark;

  @Valid
  StorageConfiguration storage;
}
