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

    foreign_hooks = [exp.column(hook["name"]) for hook in (*hooks, *composite_hooks) if hook["name"] != primary_hook]

    source_table = f"dab.hook.frame__{name}"
    sql = exp.select(
        exp.cast(exp.Literal.string(name), exp.DataType.build("text")).as_("peripheral"),
        exp.column(f"_pit{primary_hook}"),
        exp.column(primary_hook),
        *foreign_hooks,
        exp.column("_record__updated_at"),
        exp.column("_record__valid_from"),
        exp.column("_record__valid_to"),
        exp.column("_record__is_current"),
    ).from_(
        source_table
    ).where(
        exp.column("_record__updated_at").between(
            low=evaluator.locals["start_ts"],
            high=evaluator.locals["end_ts"]
        )
    )
    return sql
