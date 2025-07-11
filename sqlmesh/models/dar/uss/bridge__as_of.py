import os
import yaml
from typing import Any, Dict, List

from sqlglot import exp
from sqlmesh.core.model import model
from sqlmesh.core.macros import MacroEvaluator

def get_frames_path() -> str:
    """Returns the absolute path to the frames.yml file."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # Correct path: from sqlmesh/models/dar/uss/bridge__as_of__blueprint.py to sqlmesh/models/dab/hook/frames.yml
    return os.path.abspath(os.path.join(script_dir, '../../dab/hook/frames.yml'))

def load_frames(path: str) -> List[Dict[str, Any]]:
    """Loads frames from a YAML file."""
    with open(path, 'r') as f:
        return yaml.safe_load(f)

def get_table_names(frames: List[Dict[str, Any]]) -> List[str]:
    """Generates a list of table names from frames."""
    return ["dar.uss__staging._bridge__" + frame["name"] for frame in frames]

def get_all_columns(evaluator: MacroEvaluator, tables: List[str]) -> List[str]:
    """Gets a sorted list of unique column names from all tables."""
    all_columns = {
        col
        for table in tables
        for col in evaluator.columns_to_types(table).keys()
        if not col.startswith('_hook__')
    }

    def sort_key(col: str):
        """Defines the custom sorting logic for columns."""

        record_field_order = [
            '_record__updated_at',
            '_record__valid_from',
            '_record__valid_to',
            '_record__is_current'
        ]
        
        if col == 'peripheral':
            return (0, col)
        if col.startswith('_pit_hook__'):
            return (1, col)
        if col in record_field_order:
            return (2, record_field_order.index(col))
        return (3, col)

    return sorted(list(all_columns), key=sort_key)

def create_select_expression_for_table(
    evaluator: MacroEvaluator, all_columns: List[str], table: str
) -> exp.Select:
    """Creates a SELECT expression for a single table."""
    table_cols = set(evaluator.columns_to_types(table).keys())
    select_expressions = [
        exp.column(col) if col in table_cols else exp.alias_(exp.null(), col)
        for col in all_columns
    ]
    return exp.select(*select_expressions).from_(table)

def union_selects(select_expressions: List[exp.Select]) -> exp.Expression:
    """Unions a list of SELECT expressions."""
    if not select_expressions:
        return exp.select(exp.Literal.number(1)).where(exp.false())
    
    union = select_expressions[0]
    for sel in select_expressions[1:]:
        union = exp.union(union, sel, distinct=False)
    return union

@model(
    "dar.uss._bridge__as_of",
    enabled=True,
    is_sql=True,
    kind="VIEW"
)
def entrypoint(evaluator: MacroEvaluator) -> exp.Expression:
    """The entrypoint function for the SQLMesh model."""
    frames = load_frames(get_frames_path())
    tables = get_table_names(frames)
    
    if not tables:
        # Return a SELECT statement with no results if there are no tables.
        return exp.select(exp.Literal.number(1)).where(exp.false())

    all_columns = get_all_columns(evaluator, tables)
    
    select_expressions = [
        create_select_expression_for_table(evaluator, all_columns, table)
        for table in tables
    ]
    
    return union_selects(select_expressions)
