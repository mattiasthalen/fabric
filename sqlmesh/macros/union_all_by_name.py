from __future__ import annotations
import typing as t
from sqlglot import exp
from sqlmesh.core.macros import macro

@macro()
def union_all_by_name(
    evaluator,
    *tables: t.Union[exp.Table, exp.Expression]
) -> exp.Union:
    """
    Returns a UNION ALL statement combining the input tables by column name.
    Columns missing in a table are filled with NULL AS <column>.
    Args:
        evaluator: The macro evaluator (provided by SQLMesh).
        *tables: Table expressions as separate arguments or a single array/tuple.
    Returns:
        exp.Union: The UNION ALL SQL expression.
    """
    # Support both @UNION_ALL_BY_NAME(tbl1, tbl2, ...) and @UNION_ALL_BY_NAME([tbl1, tbl2, ...])
    if len(tables) == 1 and isinstance(tables[0], (exp.Array, exp.Tuple)):
        table_exprs = tables[0].expressions
    else:
        table_exprs = list(tables)

    all_columns = set()
    table_columns = []
    for table in table_exprs:
        cols = list(evaluator.columns_to_types(table).keys())
        table_columns.append(cols)
        all_columns.update(cols)
    all_columns = sorted(all_columns)

    selects = []
    for table, cols in zip(table_exprs, table_columns):
        col_set = set(cols)
        select_exprs = []
        for col in all_columns:
            if col in col_set:
                select_exprs.append(exp.column(col))
            else:
                select_exprs.append(
                    exp.alias_(exp.null(), col)
                )
        selects.append(
            exp.select(*select_exprs).from_(table)
        )

    union = selects[0]
    for sel in selects[1:]:
        union = exp.union(union, sel, distinct=False)
    return union