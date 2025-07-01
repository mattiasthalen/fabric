import os
import yaml
from typing import Any, Dict, List, Optional

from sqlglot import exp
from sqlmesh.core.model import model
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model.kind import ModelKindName

# --- File and Frame Utilities ---
def get_frames_path() -> str:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # Correct path: from sqlmesh/models/dar/uss/bridge_blueprint.py to sqlmesh/models/dab/hook/frames.yml
    return os.path.abspath(os.path.join(script_dir, '../../dab/hook/frames.yml'))

def load_frames(path: str) -> List[Dict[str, Any]]:
    with open(path, 'r') as f:
        return yaml.safe_load(f)
    
def load_foreign_hooks(path: str) -> List[Dict[str, Any]]:
    with open(path, 'r') as f:
        frames = yaml.safe_load(f)
        for frame in frames:
            frame['foreign_hooks'] = frame.get('foreign_hooks', [])
        return frames

def add_foreign_hooks(frames: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    for frame in frames:
        foreign_hooks = []
        for hook in frame.get("hooks", []):
            if not hook.get("primary", False):
                foreign_hooks.append(hook)
        for hook in frame.get("composite_hooks", []) or []:
            if not hook.get("primary", False):
                foreign_hooks.append(hook)
        frame["foreign_hooks"] = foreign_hooks
    return frames

# --- Primary Hook Finder ---
def find_primary_hook(frame: Dict[str, Any]) -> Optional[str]:
    hooks = frame.get("hooks", [])
    for hook in hooks:
        if hook.get("primary", False):
            return hook.get("name")
    for hook in frame.get("composite_hooks", []) or []:
        if hook.get("primary", False):
            return hook.get("name")
    return None

# --- Main Entrypoint ---
frames_path = get_frames_path()
frames = add_foreign_hooks(load_frames(frames_path))

@model(
    "dar.uss__staging._bridge__@{name}",
    is_sql=True,
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_TIME_RANGE,
        time_column="_record__updated_at",
    ),
    blueprints=frames,
)
def entrypoint(evaluator: MacroEvaluator) -> str | exp.Expression:
    name = evaluator.blueprint_var("name")
    hooks = evaluator.blueprint_var("hooks") or []
    composite_hooks = evaluator.blueprint_var("composite_hooks") or []

    frame = {"hooks": hooks, "composite_hooks": composite_hooks}

    primary_hook = find_primary_hook(frame)
    if not primary_hook:
        raise ValueError(f"No primary hook found for frame {name}")

    # --- Enhanced Foreign Hook Filtering ---
    def is_primary_in_frame(hook_name: str) -> bool:
        for f in frames:
            for h in f.get("hooks", []) + (f.get("composite_hooks", []) or []):
                if h.get("name") == hook_name and h.get("primary", False):
                    return True
        return False

    # Helper to find the frame name for a given hook name where it's primary
    def get_foreign_frame_name(hook_name: str) -> str | None:
        for f in frames:
            for h in f.get("hooks", []) + (f.get("composite_hooks", []) or []):
                if h.get("name") == hook_name and h.get("primary", False):
                    return f.get("name")
        return None

    # Only keep foreign hooks that are primary in their own frame
    foreign_hooks = [hook for hook in (*hooks, *composite_hooks) if hook["name"] != primary_hook and is_primary_in_frame(hook["name"])]

    # Add left joins for each foreign hook, using the foreign frame name
    left_table = f"frame__{name}"
    joins = []
    foreign_hook_columns = []

    record__updated_at = [exp.column("_record__updated_at", table=left_table)]
    record__valid_from = [exp.column("_record__valid_from", table=left_table)]
    record__valid_to = [exp.column("_record__valid_to", table=left_table)]
    record__is_current = [exp.column("_record__is_current", table=left_table)]

    for hook in foreign_hooks:
        fk = hook["name"]
        foreign_frame_name = get_foreign_frame_name(fk)

        if not foreign_frame_name:
            continue

        right_table_name = f"_bridge__{foreign_frame_name}"
        right_table = f"dar.uss__staging.{right_table_name}"
        right_columns = evaluator.columns_to_types(right_table).keys()

        foreign_pit_hooks = [exp.column(column, table=right_table_name) for column in right_columns if column.startswith("_pit")]
        foreign_hook_columns.extend(foreign_pit_hooks)

        record__updated_at.append(exp.column("_record__updated_at", table=right_table_name))
        record__valid_from.append(exp.column("_record__valid_from", table=right_table_name))
        record__valid_to.append(exp.column("_record__valid_to", table=right_table_name))
        record__is_current.append(exp.column("_record__is_current", table=right_table_name))

        join = exp.Join(
            this=right_table,
            on=exp.and_(
                exp.EQ(
                    this=exp.column(fk, table=left_table),
                    expression=exp.column(fk, table=right_table_name)
                ),
                exp.LT(
                    this=exp.column("_record__valid_from", table=left_table),
                    expression=exp.column("_record__valid_to", table=right_table_name)
                ),
                exp.GT(
                    this=exp.column("_record__valid_to", table=left_table),
                    expression=exp.column("_record__valid_from", table=right_table_name)
                )
            ),
            kind="LEFT"
        )

        joins.append(join)
    
    validity_expression = lambda x, y: exp.func(y, *x) if len(x) > 1 else x[0]
    record__updated_at = validity_expression(record__updated_at, "GREATEST")
    record__valid_from = validity_expression(record__valid_from, "GREATEST")
    record__valid_to = validity_expression(record__valid_to, "LEAST")
    record__is_current = validity_expression(record__is_current, "LEAST")

    sql = (
        exp.select(
            exp.cast(exp.Literal.string(name), exp.DataType.build("text")).as_("peripheral"),
            exp.column(f"_pit{primary_hook}", table=left_table),
            exp.column(primary_hook, table=left_table),
            *foreign_hook_columns,
            record__updated_at.as_("_record__updated_at"),
            record__valid_from.as_("_record__valid_from"),
            record__valid_to.as_("_record__valid_to"),
            record__is_current.as_("_record__is_current")
        )
        .from_(f"dab.hook.frame__{name}")
    )

    for join in joins:
        sql = sql.join(join)
    
    sql = sql.where(
        record__updated_at.between(
            low=evaluator.locals["start_ts"],
            high=evaluator.locals["end_ts"]
        )
    )

    return sql
