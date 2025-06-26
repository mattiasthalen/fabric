from __future__ import annotations
from sqlglot import exp
from sqlmesh.core.macros import macro

@macro()
def record_windows(evaluator, source: exp.Expression, partition_by: exp.Expression, order_by: exp.Expression, min_ts: exp.Expression, max_ts: exp.Expression) -> exp.Expression:
    """
    Applies window functions to a source CTE to generate record versions and validity periods.

    Args:
        source: The source CTE to select from.
        partition_by: The column to partition the window functions by.
        order_by: The column to order the window functions by.
        min_ts: The minimum timestamp value.
        max_ts: The maximum timestamp value.

    Returns:
        A sqlglot expression representing the SELECT statement.
    """
    
    row_number_window = exp.Window(
        this=exp.RowNumber(),
        partition_by=[partition_by.copy()],
        order=exp.Order(expressions=[order_by.copy()])
    )

    lead_window = exp.Window(
        this=exp.Lead(this=order_by.copy()),
        partition_by=[partition_by.copy()],
        order=exp.Order(expressions=[order_by.copy()])
    )

    coalesced_lead = exp.Coalesce(
        this=lead_window,
        expressions=[exp.Cast(this=max_ts.copy(), to=exp.DataType(this=exp.DataType.Type.TIMESTAMP))]
    )

    return exp.select(
        "*",
        exp.Alias(this=row_number_window.copy(), alias=exp.Identifier(this="record_version")),
        exp.Alias(
            this=exp.Case()
            .when(exp.EQ(this=row_number_window.copy(), expression=exp.Literal.number(1)), then=exp.Cast(this=min_ts.copy(), to=exp.DataType(this=exp.DataType.Type.TIMESTAMP)))
            .else_(order_by.copy()),
            alias=exp.Identifier(this="record_valid_from")
        ),
        exp.Alias(
            this=coalesced_lead.copy(),
            alias=exp.Identifier(this="record_valid_to")
        ),
        exp.Alias(
            this=exp.Case()
            .when(exp.EQ(this=coalesced_lead.copy(), expression=exp.Cast(this=max_ts.copy(), to=exp.DataType(this=exp.DataType.Type.TIMESTAMP))), then=exp.Literal.number(1))
            .else_(exp.Literal.number(0)),
            alias=exp.Identifier(this="is_current_record")
        ),
        exp.Alias(
            this=exp.Case()
            .when(exp.EQ(this=coalesced_lead.copy(), expression=exp.Cast(this=max_ts.copy(), to=exp.DataType(this=exp.DataType.Type.TIMESTAMP))), then=order_by.copy())
            .else_(coalesced_lead.copy()),
            alias=exp.Identifier(this="record_updated_at")
        )
    ).from_(source)
