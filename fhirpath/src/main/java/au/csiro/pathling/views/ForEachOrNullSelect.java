package au.csiro.pathling.views;

import java.util.Collections;
import java.util.List;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import com.google.gson.annotations.SerializedName;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * Same as forEach, but produces a single row with a null value if the collection is empty.
 *
 * @author John Grimes
 * @see <a
 * href="https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.select.forEachOrNull">ViewDefinition.select.forEachOrNull</a>
 */
@Data
@EqualsAndHashCode(callSuper = false)
public class ForEachOrNullSelect extends SelectClause {

  /**
   * Same as forEach, but produces a single row with a null value if the collection is empty.
   *
   * @see <a
   * href="https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.select.forEachOrNull">ViewDefinition.select.forEachOrNull</a>
   */
  @NotNull
  @SerializedName("forEachOrNull")
  String path;

  @NotNull
  List<ColumnElement> column =  Collections.emptyList();
  
  /**
   * Nested select relative to the {@link #path}.
   *
   * @see <a
   * href="https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.select.select">ViewDefinition.select.select</a>
   */
  @NotNull
  @Size()
  List<SelectClause> select = Collections.emptyList();


  @NotNull
  @Size()
  List<SelectClause> unionAll = Collections.emptyList();

}
