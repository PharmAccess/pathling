package au.csiro.pathling.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import javax.validation.constraints.NotNull;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class QueryConfiguration {

  /**
   * Setting this option to {@code true} will enable additional logging relating to the query plan
   * used to execute queries.
   */
  @NotNull
  @Builder.Default
  private Boolean explainQueries = false;

  @NotNull
  @Builder.Default
  private Boolean cacheDatasets = true;
}
