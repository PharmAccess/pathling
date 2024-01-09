package au.csiro.pathling.views;

import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Defines the content of a column within the view.
 *
 * @author John Grimes
 * @see <a
 * href="https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.select">ViewDefinition.select</a>
 */
public abstract class SelectClause implements SelectionElement {

  @Nullable
  abstract String getPath();

  @Nonnull
  abstract List<ColumnElement> getColumn();

  @Nonnull
  abstract List<SelectClause> getSelect();

  @Nonnull
  abstract List<SelectClause> getUnionAll();
}
