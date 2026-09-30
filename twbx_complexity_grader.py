# Databricks notebook source
# MAGIC %md
# MAGIC # Tableau Workbook Complexity Scorer
# MAGIC
# MAGIC Parses a workbook's `.twb` XML and produces a complexity report card.
# MAGIC Two ways to run it:
# MAGIC
# MAGIC 1. **Chained after the ingestion notebook** (normal job usage): pass the
# MAGIC    same `extract_dir` the ingestion notebook's `extract_hyper_files()`
# MAGIC    already unzipped the `.twbx` into. No second unzip happens -- this
# MAGIC    notebook just finds the `.twb` that's already sitting there.
# MAGIC 2. **Standalone**: pass `twbx_path` pointing at a `.twbx` file directly
# MAGIC    (e.g. ad hoc scoring, or testing a workbook without running the
# MAGIC    ingestion notebook first). If no `.twb` is found under `extract_dir`
# MAGIC    yet, this notebook unzips `twbx_path` into `extract_dir` itself.
# MAGIC
# MAGIC **Scope / assumptions (POC, extract-only workbooks)**
# MAGIC - Assumes every workbook is extract-backed (no live-connection handling).
# MAGIC - Tableau's on-disk `.twb` schema has drifted across versions. The
# MAGIC   element/attribute names below reflect common recent-version structure,
# MAGIC   but you should validate against a handful of real pilot workbooks
# MAGIC   before trusting the numbers — especially the action/parameter linkage
# MAGIC   and table-calc detection, which are the least standardized parts of
# MAGIC   the schema.
# MAGIC - Filter/context-filter detection and table-calc detection here are
# MAGIC   heuristic (regex/structure based), not exhaustive. Treat as a first
# MAGIC   pass to calibrate, not ground truth.

# COMMAND ----------

import re
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path

try:
    from tableauhyperapi import HyperProcess, Telemetry, Connection
    HYPER_AVAILABLE = True
except ImportError:
    HYPER_AVAILABLE = False

# COMMAND ----------

dbutils.widgets.text("extract_dir", "/tmp/tableau_extract")
dbutils.widgets.text("twbx_path", "")  # optional: a .twbx to unzip into extract_dir if no .twb is there yet (standalone use)
dbutils.widgets.text("workbook_name", "")  # optional label; defaults to the .twb filename
dbutils.widgets.text("target_catalog", "main")
dbutils.widgets.text("target_schema", "tableau_extracts")
dbutils.widgets.text("scores_table", "workbook_complexity_scores")

extract_dir = dbutils.widgets.get("extract_dir")
twbx_path_param = dbutils.widgets.get("twbx_path")
workbook_name_param = dbutils.widgets.get("workbook_name")
target_catalog = dbutils.widgets.get("target_catalog")
target_schema = dbutils.widgets.get("target_schema")
scores_table = dbutils.widgets.get("scores_table")

# COMMAND ----------

LOD_PATTERN = re.compile(r"\{\s*(FIXED|INCLUDE|EXCLUDE)\b", re.IGNORECASE)
TABLE_CALC_FUNCS = re.compile(
    r"\b(WINDOW_(SUM|AVG|MAX|MIN|MEDIAN|COUNT|STDEV|STDEVP|VAR|VARP)|"
    r"RUNNING_(SUM|AVG|MAX|MIN|COUNT)|RANK_?(DENSE|MODIFIED|UNIQUE)?|"
    r"INDEX|FIRST|LAST|LOOKUP|TOTAL|PREVIOUS_VALUE)\s*\(",
    re.IGNORECASE,
)
FIELD_REF_PATTERN = re.compile(r"\[([^\]]+)\]")


def find_twb_file(extract_dir: str, twbx_path: str = "") -> Path:
    """Return the .twb file to score.

    Normal chained-job usage: the ingestion notebook's extract_hyper_files()
    already unzipped the .twbx into extract_dir before this notebook runs,
    so this just finds the .twb that's already there -- no unzip happens.

    Standalone usage: if no .twb is found under extract_dir yet and
    twbx_path is given, unzip it into extract_dir ourselves (same logic the
    ingestion notebook uses) so this notebook can run on its own, e.g. for
    ad hoc scoring or testing a workbook without running that notebook first.
    """
    twb_files = list(Path(extract_dir).rglob("*.twb"))
    if twb_files:
        return twb_files[0]

    if not twbx_path:
        raise FileNotFoundError(
            f"No .twb file found under {extract_dir}, and no twbx_path was "
            "given to extract one from. Either run the ingestion notebook "
            "first (which unzips the .twbx into extract_dir), or set the "
            "twbx_path widget to a .twbx file for this notebook to unzip "
            "directly."
        )
    if not Path(twbx_path).exists():
        raise FileNotFoundError(f"twbx_path not found: {twbx_path}")

    Path(extract_dir).mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(twbx_path, "r") as z:
        z.extractall(extract_dir)

    twb_files = list(Path(extract_dir).rglob("*.twb"))
    if not twb_files:
        raise FileNotFoundError(
            f"Unzipped {twbx_path} into {extract_dir} but found no .twb "
            "file inside it. This workbook may not be a valid .twbx."
        )
    return twb_files[0]


def sanitize_name(name: str) -> str:
    """Mirrors the ingestion notebook's sanitize_name so the table names we
    report here match what was actually written to Unity Catalog."""
    clean = "".join(c if c.isalnum() or c == "_" else "_" for c in name)
    clean = clean.strip("_").lower()
    return clean or "unnamed_table"


def list_uc_table_paths(extract_dir: str, catalog: str, schema: str) -> list:
    """Enumerate the actual table names inside the .hyper file(s) already
    sitting in extract_dir, and return the fully-qualified UC paths the
    ingestion notebook would have written them to. Requires tableauhyperapi
    (same dependency the ingestion notebook already installs) -- falls back
    to a schema-level reference if it's not available in this environment.
    """
    hyper_files = list(Path(extract_dir).rglob("*.hyper"))
    if not hyper_files or not HYPER_AVAILABLE:
        return [f"`{catalog}`.`{schema}`.*  -- (exact table names unavailable: "
                f"no .hyper file found or tableauhyperapi not installed)"]

    table_paths = []
    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        for hyper_file in hyper_files:
            with Connection(endpoint=hyper.endpoint, database=str(hyper_file)) as connection:
                for schema_name in connection.catalog.get_schema_names():
                    for table in connection.catalog.get_table_names(schema=schema_name):
                        table_name = sanitize_name(table.name.unescaped)
                        table_paths.append(f"`{catalog}`.`{schema}`.`{table_name}`")
    return sorted(set(table_paths))


def build_genie_prompt_line(workbook_name, tier, uc_table_paths, calc_fields, structure, interactivity, no_equiv):
    """Compose a single copy/pasteable line of context for an AI dashboard
    authoring agent. This is a first-cut format, not a validated template --
    adjust the wording once you see how your specific agent responds to it.
    """
    calc_summary = "; ".join(calc_fields.keys()) if calc_fields else "none"
    gaps_summary = no_equiv if no_equiv else "none"
    return (
        f"Rebuild the Tableau dashboard '{workbook_name}' (conversion complexity: {tier}) "
        f"as a Databricks AI/BI dashboard using data from: {', '.join(uc_table_paths)}. "
        f"Source workbook has {structure['num_sheets']} sheet(s) and {structure['num_dashboards']} "
        f"dashboard(s) with {interactivity['num_filters']} filter(s) and {interactivity['num_parameters']} "
        f"parameter(s). Calculated fields to reimplement as SQL: {calc_summary}. "
        f"Tableau features with no direct Databricks equivalent (will need a workaround or feature drop): {gaps_summary}."
    )


def collect_calc_fields(root):
    """Return {field_name: formula} across all non-parameter datasources.

    Uses "./datasources/datasource" (direct children of the <workbook> root),
    NOT ".//datasources/datasource". The latter also matches the lightweight
    per-worksheet datasource-reference stanzas at
    workbook/worksheets/worksheet/table/view/datasources/datasource, which
    Tableau writes once per sheet that uses the data source -- these are
    dependency pointers, not separate data sources, and in the one real
    workbook tested (2026-09-29) they carried no calculation columns, but
    relying on that would be an assumption, not a guarantee.
    """
    calc_fields = {}
    for ds in root.findall("./datasources/datasource"):
        if ds.get("name") == "Parameters":
            continue
        for column in ds.findall("column"):
            calc = column.find("calculation")
            if calc is not None and calc.get("formula"):
                # Key by the internal name, not the caption -- formulas reference
                # other fields via their internal [Name] token (e.g. [Calculation_2]),
                # not the human-readable caption, so keying by caption made every
                # nested reference fail to match and silently capped nesting depth at 1.
                name = column.get("name", "").strip("[]")
                calc_fields[name] = calc.get("formula")
    return calc_fields


def parse_data_model(root):
    # "./datasources/datasource" (direct children of <workbook>), not ".//" --
    # see collect_calc_fields for why. This is the bug that inflated
    # num_datasources to 5 on a workbook that really has 1 (2026-09-29): the
    # broad search also counted each worksheet's per-view datasource-reference
    # stanza as if it were its own data source.
    datasources = [
        ds for ds in root.findall("./datasources/datasource")
        if ds.get("name") != "Parameters"
    ]
    num_joins = 0
    num_custom_sql = 0
    for ds in datasources:
        for relation in ds.findall(".//relation"):
            rel_type = relation.get("type")
            if rel_type == "join":
                num_joins += 1
            elif rel_type == "text":
                num_custom_sql += 1
    return {
        "num_datasources": len(datasources),
        "num_joins": num_joins,
        "num_custom_sql": num_custom_sql,
    }


def parse_calculations(calc_fields: dict):
    num_lod = sum(1 for f in calc_fields.values() if LOD_PATTERN.search(f))
    num_table_calc = sum(1 for f in calc_fields.values() if TABLE_CALC_FUNCS.search(f))

    graph = {
        name: {r for r in FIELD_REF_PATTERN.findall(formula) if r in calc_fields and r != name}
        for name, formula in calc_fields.items()
    }

    def depth(node, seen):
        if node in seen:
            return 0  # circular reference guard
        deps = graph.get(node, set())
        if not deps:
            return 1
        return 1 + max(depth(d, seen | {node}) for d in deps)

    max_nesting_depth = max((depth(n, set()) for n in graph), default=0)

    return {
        "num_calc_fields": len(calc_fields),
        "num_lod_expressions": num_lod,
        "num_table_calcs": num_table_calc,
        "max_calc_nesting_depth": max_nesting_depth,
    }


def _zone_depth(zone, current=1):
    children = zone.findall("zone")
    if not children:
        return current
    return max(_zone_depth(z, current + 1) for z in children)


def parse_structure(root):
    num_sheets = len(root.findall(".//worksheets/worksheet"))
    dashboards = root.findall(".//dashboards/dashboard")

    max_container_depth = 0
    total_objects = 0
    for dash in dashboards:
        zones_root = dash.find("zones")
        if zones_root is None:
            continue
        for z in zones_root.findall("zone"):
            max_container_depth = max(max_container_depth, _zone_depth(z))
        total_objects += sum(1 for z in dash.iter("zone") if not z.findall("zone"))

    num_dashboards = len(dashboards)
    return {
        "num_sheets": num_sheets,
        "num_dashboards": num_dashboards,
        "max_container_nesting_depth": max_container_depth,
        "avg_objects_per_dashboard": round(total_objects / num_dashboards, 1) if num_dashboards else 0,
    }


def parse_interactivity(root, calc_fields: dict):
    # Loop per-worksheet rather than one combined "//worksheet//filter" path --
    # filters are usually nested several levels deep (worksheet/table/view/filter),
    # so each worksheet needs its own recursive descendant search.
    num_filters = sum(
        len(ws.findall(".//filter")) for ws in root.findall(".//worksheets/worksheet")
    )

    # "./datasources/datasource" (direct children of <workbook>), not ".//" --
    # see collect_calc_fields for why the broad form over-matches.
    param_names = set()
    for ds in root.findall("./datasources/datasource"):
        if ds.get("name") == "Parameters":
            for column in ds.findall("column"):
                param_names.add((column.get("caption") or column.get("name", "")).strip("[]"))
    num_sets = sum(
        1 for ds in root.findall("./datasources/datasource")
        for column in ds.findall("column")
        if column.get("type") == "set" or column.find("groupfilter") is not None
    )

    action_counts = {"filter": 0, "highlight": 0, "url": 0, "parameter": 0, "other": 0}
    num_param_actions_feeding_calc = 0

    for action in root.findall(".//actions/action"):
        children = list(action)
        child = children[0] if children else None
        tag = child.tag if child is not None else "other"
        action_counts[tag] = action_counts.get(tag, 0) + 1

        if tag == "parameter" and child is not None:
            target = (child.get("parameter") or "").strip("[]")
            if target and any(f"[{target}]" in formula for formula in calc_fields.values()):
                num_param_actions_feeding_calc += 1

    return {
        "num_filters": num_filters,
        "num_parameters": len(param_names),
        "num_sets": num_sets,
        "num_actions_filter": action_counts["filter"],
        "num_actions_highlight": action_counts["highlight"],
        "num_actions_url": action_counts["url"],
        "num_actions_parameter": action_counts["parameter"],
        "num_param_actions_feeding_calc": num_param_actions_feeding_calc,
    }


# COMMAND ----------

# ---- Scoring: weights and overrides ----
# Starting weights only — recalibrate once you have actual conversion-hours
# data from a handful of completed migrations (see note in chat).
CATEGORY_WEIGHTS = {"data_model": 0.15, "calculations": 0.40, "structure": 0.20, "interactivity": 0.25}

OVERRIDE_RULES = [
    ("max_calc_nesting_depth", 5, "Very Complex", "calc nesting depth > 5"),
    ("num_param_actions_feeding_calc", 0, "Complex", "parameter action feeds a calculated field"),
]


def detect_no_equivalent_features(root):
    """Best-effort detection of Tableau features with no direct equivalent in
    Databricks AI/BI dashboards. These aren't just "more SQL to write" --
    they typically mean dropping the feature, faking it with a workaround,
    or building a custom Databricks App instead of a plain dashboard.
    Not exhaustive -- see the per-check notes below for confidence level.

    Known gaps NOT auto-detected here (schema too inconsistent to trust a
    heuristic from documentation alone): Viz in Tooltip, Ask Data / Explain
    Data objects. Check for these manually during the pilot.

    Dynamic Zone Visibility (DZV) is INTENTIONALLY NOT auto-detected here.
    An earlier version of this check flagged any <zone> with a `param`
    attribute, on the theory that DZV sets a param/visual-context binding on
    the zone. Confirmed false positive on a real workbook (SuperStore Test
    Mid, 2026-09-29): `param="vert"`/`"horz"` on a zone is Tableau's own
    layout-flow *stacking direction* attribute, present on ~every flow
    container, not a DZV marker. Tableau doesn't publish the .twb schema and
    no verified DZV-specific XML signature has been confirmed yet, so rather
    than ship another guessed heuristic, this is left as a manual check
    during the pilot until we can diff a .twb file from a workbook confirmed
    to use DZV against one that doesn't, and identify the real signal.
    """
    flags = []

    if root.findall(".//storyboards/storyboard"):
        flags.append("Story Points")

    if any(z.get("type") == "extension" for z in root.iter("zone")):
        flags.append("Dashboard Extensions (custom JS objects)")

    for action in root.findall(".//actions/action"):
        children = list(action)
        if children and children[0].tag == "set":
            flags.append("Set Actions")
            break

    return flags


# Per-category linear normalization ceilings: the raw (weighted) count at
# which a category is considered fully "maxed out" (100/100). These are
# still reasoned placeholders, not calibrated against real conversion-hours
# data -- but they replace a log-curve normalize() that had a worse problem
# than "not calibrated": it actively misbehaved at low counts.
#
# Bug found 2026-09-29 running this against a real, genuinely simple
# workbook (SuperStore Test Mid: 1 data source, 2 calc fields, 4 sheets,
# 0 filters): the old normalize(value, scale=10.0) used
# 100 * log1p(value) / log1p(scale), a log curve, which is steepest right
# at zero. That inflates small counts instead of suppressing them --
# 1 occurrence of anything already scored 28.9/100, 2 occurrences 45.8/100,
# and a nesting depth of 4 (ordinary, auto-generated Tableau layout wrapping
# for a basic 4-worksheet dashboard, not a sign of real complexity) combined
# with the category's other terms to blow straight through the scale and
# cap at 100. The composite ended up "Complex" for a workbook that's
# actually about as simple as they come.
#
# A plain linear ratio against a per-category ceiling doesn't have that
# failure mode: a handful of anything stays proportionally low, and only
# a raw count that's actually large approaches 100.
CATEGORY_CEILINGS = {
    "data_model": 15,      # e.g. ~5 data sources with a couple joins/custom SQL each
    "calculations": 60,    # e.g. ~20 calc fields, several LOD/table calcs (each weighted x3)
    "structure": 40,       # e.g. ~15 sheets across a few dashboards with deep container nesting
    "interactivity": 30,   # e.g. ~15 filters/parameters plus a few parameter-actions-feeding-calc (weighted x3)
}


def normalize(value, category):
    """Linear scale: value / ceiling, capped at 100. See CATEGORY_CEILINGS
    docstring above for why this replaced a log-curve version. Recalibrate
    these ceilings once you have actual conversion-hours data from a
    handful of completed migrations."""
    ceiling = CATEGORY_CEILINGS[category]
    return min(100.0, 100.0 * value / ceiling)


def score_workbook(twb_path: Path, workbook_name: str, extract_dir: str, catalog: str, schema: str) -> dict:
    tree = ET.parse(twb_path)
    root = tree.getroot()

    calc_fields = collect_calc_fields(root)
    data_model = parse_data_model(root)
    calculations = parse_calculations(calc_fields)
    structure = parse_structure(root)
    interactivity = parse_interactivity(root, calc_fields)

    category_scores = {
        "data_model": normalize(
            data_model["num_datasources"] + data_model["num_joins"] + data_model["num_custom_sql"],
            "data_model",
        ),
        "calculations": normalize(
            calculations["num_calc_fields"] + 3 * calculations["num_lod_expressions"] + 3 * calculations["num_table_calcs"],
            "calculations",
        ),
        "structure": normalize(
            structure["num_sheets"] + structure["num_dashboards"] + 2 * structure["max_container_nesting_depth"],
            "structure",
        ),
        "interactivity": normalize(
            interactivity["num_filters"] + interactivity["num_parameters"] + 3 * interactivity["num_param_actions_feeding_calc"],
            "interactivity",
        ),
    }

    composite = sum(category_scores[cat] * weight for cat, weight in CATEGORY_WEIGHTS.items())

    metrics = {**data_model, **calculations, **structure, **interactivity}
    tier = "Simple" if composite < 25 else "Moderate" if composite < 50 else "Complex" if composite < 75 else "Very Complex"

    no_equiv = detect_no_equivalent_features(root)

    triggered_overrides = []
    for metric_name, threshold, forced_tier, label in OVERRIDE_RULES:
        if metrics.get(metric_name, 0) > threshold:
            triggered_overrides.append(label)
            if _tier_rank(forced_tier) > _tier_rank(tier):
                tier = forced_tier

    uc_table_paths = list_uc_table_paths(extract_dir, catalog, schema)
    genie_prompt_line = build_genie_prompt_line(
        workbook_name, tier, uc_table_paths, calc_fields, structure, interactivity,
        ", ".join(no_equiv) if no_equiv else "",
    )

    override_flags_str = ", ".join(triggered_overrides) if triggered_overrides else ""
    no_equiv_str = ", ".join(no_equiv) if no_equiv else ""
    proceed, proceed_reason = compute_proceed_decision(tier, override_flags_str, no_equiv_str)

    return {
        "twb_file": twb_path.name,
        "workbook_name": workbook_name,
        "composite_score": round(composite, 1),
        "complexity_tier": tier,
        # "" rather than None -- a single-row Spark DataFrame with a None value
        # in a column can't have its type inferred and will fail to write.
        "override_flags": override_flags_str,
        "no_equivalent_features": no_equiv_str,
        "proceed": proceed,
        "proceed_reason": proceed_reason,
        "uc_table_paths": ", ".join(uc_table_paths),
        "genie_authoring_prompt": genie_prompt_line,
        **{f"score_{cat}": round(score, 1) for cat, score in category_scores.items()},
        **metrics,
    }


def _tier_rank(tier):
    return ["Simple", "Moderate", "Complex", "Very Complex"].index(tier)


def compute_proceed_decision(tier: str, override_flags: str, no_equivalent_features: str):
    """Go/no-go for the POC conversion.

    Don't proceed ("proceed" = False) if ANY of:
      - no_equivalent_features is non-empty (a Tableau feature with no
        Databricks AI/BI dashboard equivalent was detected)
      - override_flags is non-empty (any override rule was triggered)
      - the final tier is above "Complex" (i.e. "Very Complex")
    "Complex" on its own, with no overrides and no no-equivalent features,
    still proceeds -- only "Very Complex" trips the tier condition.
    """
    reasons = []
    if no_equivalent_features:
        reasons.append(f"no-equivalent feature(s) detected: {no_equivalent_features}")
    if override_flags:
        reasons.append(f"override flag(s) triggered: {override_flags}")
    if _tier_rank(tier) > _tier_rank("Complex"):
        reasons.append(f"complexity tier is {tier}")

    proceed = len(reasons) == 0
    reason_text = "; ".join(reasons) if reasons else "no overrides, no no-equivalent features, tier <= Complex"
    return proceed, reason_text


# COMMAND ----------

def main():
    twb_path = find_twb_file(extract_dir, twbx_path_param)
    workbook_name = workbook_name_param or twb_path.stem
    result = score_workbook(twb_path, workbook_name, extract_dir, target_catalog, target_schema)

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{target_catalog}`.`{target_schema}`")
    result_df = spark.createDataFrame([result])
    full_table_name = f"`{target_catalog}`.`{target_schema}`.`{scores_table}`"
    result_df.write.mode("append").option("mergeSchema", "true").saveAsTable(full_table_name)

    decision_label = "PROCEED" if result["proceed"] else "DO NOT PROCEED"
    print(f"Scored {result['workbook_name']}: {result['composite_score']} ({result['complexity_tier']}) -- {decision_label}")
    print(f"  Reason: {result['proceed_reason']}")
    if result["override_flags"]:
        print(f"  Overrides triggered: {result['override_flags']}")
    if result["no_equivalent_features"]:
        print(f"  No-equivalent features: {result['no_equivalent_features']}")
    print("\n--- Paste into your dashboard authoring agent: ---")
    print(result["genie_authoring_prompt"])
    print("--- end ---\n")

    dbutils.jobs.taskValues.set(key="decision_label", value=decision_label)
    dbutils.jobs.taskValues.set(key="proceed", value=result["proceed"])
    dbutils.jobs.taskValues.set(key="complexity_tier", value=result["complexity_tier"])


main()