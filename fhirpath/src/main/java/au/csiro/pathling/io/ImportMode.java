package au.csiro.pathling.io;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;

/**
 * Supported import modes for Pathling persistence schemes.
 *
 * @author John Grimes
 */
public enum ImportMode {
  /**
   * Results in all existing resources of the specified type to be deleted and replaced with the
   * contents of the source file.
   */
  OVERWRITE("overwrite"),

  /**
   * Matches existing resources with updated resources in the source file based on their ID, and
   * either update the existing resources or add new resources as appropriate.
   */
  MERGE("merge");

  @Nonnull
  @Getter
  private final String code;

  ImportMode(@Nonnull final String code) {
    this.code = code;
  }

  @Nullable
  public static ImportMode fromCode(@Nonnull final String code) {
    for (final ImportMode mode : values()) {
      if (mode.code.equals(code)) {
        return mode;
      }
    }
    return null;
  }

}
