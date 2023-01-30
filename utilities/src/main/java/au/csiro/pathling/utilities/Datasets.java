package au.csiro.pathling.utilities;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

import static java.util.Objects.requireNonNull;

@Slf4j
public abstract class Datasets {

  /**
   * Writes a result to the configured result storage area.
   *
   * @param result the {@link Dataset} containing the result
   * @param fileUrl a name to use as the filename
   * @param saveMode the {@link SaveMode} to use
   * @return the URL of the result
   */
  public static String writeCsv(@Nonnull final Dataset<?> result, @Nonnull final String fileUrl,
      @Nonnull final SaveMode saveMode) {
    final SparkSession spark = result.sparkSession();
    
    // Get a handle for the Hadoop FileSystem representing the result location, and check that it
    // is accessible.
    @Nullable final org.apache.hadoop.conf.Configuration hadoopConfiguration = spark.sparkContext()
        .hadoopConfiguration();
    requireNonNull(hadoopConfiguration);
    @Nullable final FileSystem warehouseLocation;
    try {
      warehouseLocation = FileSystem.get(new URI(fileUrl), hadoopConfiguration);
    } catch (final IOException e) {
      throw new RuntimeException("Problem accessing result location: " + fileUrl, e);
    } catch (final URISyntaxException e) {
      throw new RuntimeException("Problem parsing result URL: " + fileUrl, e);
    }
    requireNonNull(warehouseLocation);

    // Write result dataset to result location.
    final String resultFileUrl = fileUrl;
    log.info("Writing result: " + resultFileUrl);
    try {
      result.coalesce(1)
          .write()
          .mode(saveMode)
          .csv(resultFileUrl);
    } catch (final Exception e) {
      throw new RuntimeException("Problem writing to file: " + resultFileUrl, e);
    }

    // Find the single file and copy it into the final location.
    final String targetUrl = resultFileUrl + ".csv";
    try {
      final Path resultPath = new Path(resultFileUrl);
      final FileStatus[] partitionFiles = warehouseLocation.listStatus(resultPath);
      final String targetFile = Arrays.stream(partitionFiles)
          .map(f -> f.getPath().toString())
          .filter(f -> f.endsWith(".csv"))
          .findFirst()
          .orElseThrow(() -> new IOException("Partition file not found"));
      log.info("Renaming result to: " + targetUrl);
      warehouseLocation.rename(new Path(targetFile), new Path(targetUrl));
      log.info("Cleaning up: " + resultFileUrl);
      warehouseLocation.delete(resultPath, true);
    } catch (final IOException e) {
      throw new RuntimeException("Problem copying partition file", e);
    }

    return targetUrl;
  }
  
}
