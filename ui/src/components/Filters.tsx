/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import { Icon, Tag } from "@blueprintjs/core";
import React, { ReactElement } from "react";
import { connect } from "react-redux";
import { GlobalState } from "../store";
import * as elementTreeActions from "../store/ElementTreeActions";
import * as queryActions from "../store/QueryActions";
import { ExpressionWithIdentity } from "../store/QueryReducer";
import ExpressionEditor from "./ExpressionEditor";
import "./style/Filters.scss";

interface Props {
  aggregations: ExpressionWithIdentity[];
  groupings: ExpressionWithIdentity[];
  filters: ExpressionWithIdentity[];
  removeFilter: (id: string) => any;
  updateFilter: (filter: ExpressionWithIdentity) => any;
  clearElementTreeFocus: () => any;
}

/**
 * Renders a control which can be used to represent a set of selected
 * filters, when composing a query.
 *
 * @author John Grimes
 */
function Filters(props: Props) {
  const {
    aggregations,
    groupings,
    filters,
    removeFilter,
    updateFilter,
    clearElementTreeFocus
  } = props;

  const handleRemove = (event: any, filter: ExpressionWithIdentity) => {
    // This is required to stop the click event from opening the expression
    // editor for other filters.
    event.stopPropagation();
    if (aggregations.length + groupings.length + filters.length === 1) {
      clearElementTreeFocus();
    }
    removeFilter(filter.id);
  };

  const renderBlankCanvas = (): ReactElement => (
    <div className="filters__blank">Filters</div>
  );

  const renderFilters = (): ReactElement[] =>
    filters.map((filter, i) => (
      <ExpressionEditor key={i} expression={filter} onChange={updateFilter}>
        <Tag
          className={
            filter.disabled
              ? "filters__expression filters__expression--disabled"
              : "filters__expression"
          }
          round={true}
          large={true}
          onRemove={event => handleRemove(event, filter)}
          title="Edit this expression"
        >
          {filter.label}
        </Tag>
      </ExpressionEditor>
    ));

  return (
    <div className="filters">
      <Icon className="filters__identity" icon="filter" />
      {filters.length === 0 ? renderBlankCanvas() : renderFilters()}
    </div>
  );
}

const mapStateToProps = (state: GlobalState) => ({ ...state.query.query });

const actions = { ...queryActions, ...elementTreeActions };

export default connect(
  mapStateToProps,
  actions
)(Filters);
