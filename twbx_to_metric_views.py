# Databricks notebook source
# MAGIC %md
# MAGIC # Tableau Packaged Workbook (.twbx) -> Unity Catalog Metric Views
# MAGIC
# MAGIC Creates one metric view per calculated field.
# MAGIC
# MAGIC **Pipeline** (each step is a self-contained function so it can be registered
# MAGIC as a Unity Catalog Python function; imports and helpers live inside each function):
# MAGIC
# MAGIC 1. `parse_twbx_calculations` -> JSON of calcs (name, id, formula), deduplicated
# MAGIC 2. `build_metric_views` -> one metric view definition (YAML + CREATE VIEW SQL) per calc.
# MAGIC    Accepts the list of tables materialized by the upstream extract task and, per calc,
# MAGIC    picks the table that holds the referenced fields (joining extra tables when needed).
# MAGIC 3. `push_metric_views` -> execute the SQL via the Databricks Statement Execution API
# MAGIC
# MAGIC **Upstream dependency:** the `Extract_Data_Source_from_Tableau_Workbook` task, which sets
# MAGIC the task values `created_tables` (comma-separated catalog.schema.table), `twbx_path` and `uc_schema`.
# MAGIC
# MAGIC **Requires:** pandas, databricks-sdk. Optional: unitycatalog-ai (for `register_as_uc_functions`).
# MAGIC Metric view spec 1.1 -> SQL warehouse / DBR 17.3+ (wildcard fields need 18.2+).

# COMMAND ----------

# ---- Job parameters (settable via Databricks widgets / job params) ----
dbutils.widgets.text("upstream_task_key", "Extract_Data_Source_from_Tableau_Workbook")
dbutils.widgets.text("twbx_path", "/Volumes/cache_money/default/tableauworkbooks/SuperStore Test Mid.twbx")
# Optional override: comma-separated catalog.schema.table list, or a catalog.schema to scan.
# Leave blank to use the tables created by the upstream task.
dbutils.widgets.text("source_tables", "")
dbutils.widgets.text("metric_view_schema", "")  # blank = same schema as the source tables
dbutils.widgets.text("target_warehouse", "Serverless Starter Warehouse")  # warehouse name or ID
dbutils.widgets.dropdown("conversion_mode", "rule", ["rule", "llm"])
dbutils.widgets.text("llm_endpoint", "")
dbutils.widgets.dropdown("dry_run", "true", ["true", "false"])

upstream_task_key = dbutils.widgets.get("upstream_task_key")


def _task_value(key: str, fallback: str) -> str:
    """Read a task value from the upstream task; fall back to `fallback` when run interactively."""
    try:
        value = dbutils.jobs.taskValues.get(
            taskKey=upstream_task_key, key=key, default=fallback, debugValue=fallback)
        return value if value is not None else fallback
    except Exception:
        return fallback


twbx_path = _task_value("twbx_path", dbutils.widgets.get("twbx_path"))
source_tables = (
    dbutils.widgets.get("source_tables").strip()
    or _task_value("created_tables", "")
    or _task_value("uc_schema", "")  # fall back to scanning the whole workbook schema
)
if not source_tables:
    raise ValueError(
        "No source tables: run this after the upstream extract task, or set the "
        "'source_tables' widget to a table list or catalog.schema.")
metric_view_schema = dbutils.widgets.get("metric_view_schema").strip()
target_warehouse = dbutils.widgets.get("target_warehouse")
conversion_mode = dbutils.widgets.get("conversion_mode")
llm_endpoint = dbutils.widgets.get("llm_endpoint")
dry_run = dbutils.widgets.get("dry_run").lower() == "true"


def parse_twbx_calculations(twbx_path: str) -> str:
    """
    Parse calculated fields from a Tableau packaged workbook and deduplicate them.

    Args:
        twbx_path: Path to the .twbx file (local path or /Volumes/... path).

    Returns:
        JSON array of unique calculations with keys: name, calc_id, formula,
        formula_inlined, datatype, tableau_role, datasources. `formula_inlined` has
        comments removed and references to other calculated fields expanded so each
        calculation is self-contained.
    """
    import re
    import zipfile
    import xml.etree.ElementTree as ET

    import pandas as pd

    def strip_comments(f):
        f = re.sub(r"/\*.*?\*/", "", f, flags=re.S)
        out = []
        for line in f.splitlines():
            in_s = in_d = False
            cut = len(line)
            for i, ch in enumerate(line):
                if ch == "'" and not in_d:
                    in_s = not in_s
                elif ch == '"' and not in_s:
                    in_d = not in_d
                elif line[i:i + 2] == "//" and not in_s and not in_d:
                    cut = i
                    break
            out.append(line[:cut])
        return "\n".join(out).strip()

    with zipfile.ZipFile(twbx_path) as zf:
        twb_names = [n for n in zf.namelist() if n.lower().endswith(".twb")]
        if not twb_names:
            raise ValueError("No .twb file found inside the .twbx archive")
        root = ET.fromstring(zf.read(twb_names[0]))

    rows = []
    for ds in root.iter("datasource"):
        if ds.get("name") == "Parameters":
            continue
        ds_name = ds.get("caption") or ds.get("name") or ""
        for col in ds.iter("column"):
            calc = col.find("calculation")
            if calc is None or calc.get("class") != "tableau" or not calc.get("formula"):
                continue
            calc_id = (col.get("name") or "").strip("[]")
            rows.append({
                "datasource": ds_name,
                "calc_id": calc_id,
                "name": col.get("caption") or calc_id,
                "formula": calc.get("formula"),
                "datatype": col.get("datatype"),
                "tableau_role": col.get("role"),
            })

    columns = ["datasource", "calc_id", "name", "formula", "datatype", "tableau_role"]
    df = pd.DataFrame(rows, columns=columns)
    if df.empty:
        return "[]"

    # Expand references to other calculated fields (per datasource), recursively.
    def inline(formula, ds_map, seen=()):
        def repl(m):
            key = m.group(1)
            if key in ds_map and key not in seen:
                return "(" + inline(ds_map[key], ds_map, seen + (key,)) + ")"
            return m.group(0)
        return re.sub(r"\[([^\]]+)\]", repl, formula)

    inlined = []
    for ds_name, grp in df.groupby("datasource", sort=False):
        ds_map = {}
        for r in grp.itertuples(index=False):
            clean = strip_comments(r.formula)
            ds_map[r.calc_id] = clean
            ds_map[r.name] = clean
        for idx, r in zip(grp.index, grp.itertuples(index=False)):
            inlined.append((idx, inline(strip_comments(r.formula), ds_map, (r.calc_id, r.name))))
    df["formula_inlined"] = pd.Series(dict(inlined))

    # Deduplicate: same name + same formula (whitespace-insensitive) across datasources/copies.
    df["_key"] = df["formula_inlined"].map(lambda s: re.sub(r"\s+", " ", s).strip())
    df = df.groupby(["name", "_key"], sort=False, as_index=False).agg(
        calc_id=("calc_id", "first"),
        formula=("formula", "first"),
        formula_inlined=("formula_inlined", "first"),
        datatype=("datatype", "first"),
        tableau_role=("tableau_role", "first"),
        datasources=("datasource", lambda s: sorted(set(s))),
    )
    df = df.drop(columns="_key")
    return df.to_json(orient="records")


def build_metric_views(
    calcs_json: str,
    source_tables: str,
    conversion_mode: str = "rule",
    llm_endpoint: str = "",
    view_prefix: str = "mv_",
    target_schema: str = "",
    include_source_fields: bool = False,
) -> str:
    """
    Build one Unity Catalog metric view definition per Tableau calculated field.

    For every calculation the fields it references are matched against the columns of the
    candidate source tables. The table covering the most references becomes the metric
    view's `source`; any remaining references that live in another table are brought in
    through a metric view `joins:` entry, keyed on the columns the two tables share.

    Calculations containing an aggregate (SUM, COUNTD, ...) become a measure. All others
    become a field (dimension) plus a default record_count measure so the view is queryable.
    Unsupported calcs (LOD expressions, table calcs, parameters, unjoinable tables) are
    reported, not built.

    Args:
        calcs_json: JSON output of parse_twbx_calculations.
        source_tables: Candidate UC tables, as a comma-separated string or JSON array. Each entry
            is catalog.schema.table, or catalog.schema to use every table in that schema.
        conversion_mode: 'rule' for rule-based Tableau->Databricks SQL, or 'llm' to use a serving endpoint.
        llm_endpoint: Databricks Model Serving chat endpoint name (required when conversion_mode='llm').
        view_prefix: Prefix for metric view names.
        target_schema: catalog.schema for the metric views. Defaults to the chosen source table's schema.
        include_source_fields: If true, also expose all source columns as fields (source.*, needs DBR 18.2+).

    Returns:
        JSON array with one record per calc: name, calc_id, view_name, kind, status
        (ok / needs_review / unsupported), converted_by, source_table, joins, notes,
        tableau_formula, expr, sql.
    """
    import json
    import re
    from io import StringIO

    import pandas as pd

    if conversion_mode not in ("rule", "llm"):
        raise ValueError("conversion_mode must be 'rule' or 'llm'")
    if conversion_mode == "llm" and not llm_endpoint:
        raise ValueError("llm_endpoint is required when conversion_mode='llm'")

    def norm(s):
        return re.sub(r"[^0-9a-z]+", "_", str(s).lower()).strip("_")

    def qcol(c):
        return c if re.fullmatch(r"[A-Za-z_]\w*", c) else "`" + c.replace("`", "``") + "`"

    def short(t):
        return t.rsplit(".", 1)[-1]

    # ---- Workspace client ----
    w = None
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
    except Exception:
        if conversion_mode == "llm":
            raise

    # ---- Resolve the candidate source tables ----
    raw = (source_tables or "").strip()
    entries = json.loads(raw) if raw.startswith("[") else raw.split(",")
    tables = []
    for e in (str(x).strip().replace("`", "") for x in entries):
        if not e:
            continue
        parts = e.split(".")
        if len(parts) == 3:
            tables.append(e)
        elif len(parts) == 2:
            if w is None:
                raise ValueError(f"Listing tables in schema '{e}' requires databricks-sdk")
            for t in w.tables.list(catalog_name=parts[0], schema_name=parts[1]):
                # Skip views (including metric views created by earlier runs of this script).
                if "VIEW" in str(t.table_type or "").upper() or short(t.full_name).startswith(view_prefix):
                    continue
                tables.append(t.full_name)
        else:
            raise ValueError(f"Expected catalog.schema.table or catalog.schema, got '{e}'")
    tables = list(dict.fromkeys(tables))
    if not tables:
        raise ValueError("No source tables found")

    # ---- Columns per table (used for table selection, name matching and LLM context) ----
    table_cols = {}
    for t in tables:
        cols = {}
        if w is not None:
            try:
                cols = {c.name: c.type_text for c in (w.tables.get(t).columns or [])}
            except Exception:
                cols = {}
        table_cols[t] = cols
    table_norm = {t: {norm(c): c for c in cols} for t, cols in table_cols.items()}
    have_columns = any(table_cols.values())

    def find_col(t, ref):
        if ref in table_cols[t]:
            return ref
        return table_norm[t].get(norm(ref))

    def ws(s):
        return re.sub(r"\s+", " ", s).strip()

    STR_LIT = r"'(?:[^'\\]|\\.)*'|\"(?:[^\"\\]|\\.)*\""

    # ---- Pick the source table (and joins) for one calculation ----
    def plan_sources(formula, hints):
        plain = re.sub(STR_LIT, "''", formula)
        refs = list(dict.fromkeys(ws(x) for x in re.findall(r"\[([^\]]+)\]", plain)))

        hits = {}
        for ref in refs:
            h = [(t, c) for t in tables for c in [find_col(t, ref)] if c]
            if not h:
                # Tableau disambiguates duplicate names from related tables as "Field (Table)".
                m = re.fullmatch(r"(.+?)\s*\(([^()]+)\)", ref)
                if m:
                    base_ref, tbl = m.group(1), norm(m.group(2))
                    for t in tables:
                        st = norm(short(t))
                        if tbl and (st == tbl or tbl in st or st in tbl):
                            c = find_col(t, base_ref)
                            if c:
                                h.append((t, c))
            hits[ref] = h

        hint_norms = [n for n in (norm(x) for x in hints) if n]

        def score(t):
            st = norm(short(t))
            cover = sum(1 for h in hits.values() if any(x[0] == t for x in h))
            hinted = any(hn in st or st in hn for hn in hint_norms)
            return (cover, hinted, len(table_cols[t]))

        base = max(tables, key=score)
        colmap, joins, notes, used_aliases = {}, {}, [], {"source"}
        for ref, h in hits.items():
            on_base = [c for t, c in h if t == base]
            if on_base:
                colmap[ref] = qcol(on_base[0])
                continue
            if not h:
                if have_columns:
                    notes.append(f"column [{ref}] not found in any source table")
                colmap[ref] = qcol(ref)
                continue
            t, c = next(((t, c) for t, c in h if t in joins), h[0])
            if t not in joins:
                shared = sorted(set(table_norm[base]) & set(table_norm[t]))
                keys = [k for k in shared if re.search(r"(^|_)(id|key)$", k)] or shared
                if not keys:
                    return base, [], {}, notes, (
                        f"[{ref}] is in {t}, which shares no column with {base} to join on")
                alias = norm(short(t)) or "t"
                if alias[0].isdigit():
                    alias = "_" + alias
                while alias in used_aliases:
                    alias += "_j"
                used_aliases.add(alias)
                on = " AND ".join(
                    f"source.{qcol(table_norm[base][k])} = {alias}.{qcol(table_norm[t][k])}" for k in keys)
                joins[t] = {"name": alias, "source": t, "on": on}
                notes.append(f"joined {t} as '{alias}' on {', '.join(keys)} "
                             "(inferred from shared column names; verify it is many-to-one)")
            colmap[ref] = f"{joins[t]['name']}.{qcol(c)}"
        return base, list(joins.values()), colmap, notes, None

    # ---- Small parsing helpers ----
    def split_args(s):
        args, depth, cur, quote = [], 0, [], None
        for ch in s:
            if quote:
                cur.append(ch)
                if ch == quote:
                    quote = None
            elif ch in "'\"`":
                quote = ch
                cur.append(ch)
            elif ch in "([{":
                depth += 1
                cur.append(ch)
            elif ch in ")]}":
                depth -= 1
                cur.append(ch)
            elif ch == "," and depth == 0:
                args.append("".join(cur).strip())
                cur = []
            else:
                cur.append(ch)
        last = "".join(cur).strip()
        if last or args:
            args.append(last)
        return args

    def rewrite_call(expr, name, builder):
        pat = re.compile(r"\b" + name + r"\s*\(", re.I)
        out, i = [], 0
        while True:
            m = pat.search(expr, i)
            if not m:
                out.append(expr[i:])
                break
            out.append(expr[i:m.start()])
            j = k = m.end()
            depth, quote = 1, None
            while k < len(expr) and depth:
                ch = expr[k]
                if quote:
                    if ch == quote:
                        quote = None
                elif ch in "'\"`":
                    quote = ch
                elif ch == "(":
                    depth += 1
                elif ch == ")":
                    depth -= 1
                k += 1
            args = [rewrite_call(a, name, builder) for a in split_args(expr[j:k - 1])]
            out.append(builder(args))
            i = k
        return "".join(out)

    def map_outside(s, fn):
        parts = re.split(r"('(?:[^'\\]|\\.)*'|`[^`]*`)", s)
        return "".join(p if i % 2 else fn(p) for i, p in enumerate(parts))

    UNSUPPORTED = [
        (r"\{", "LOD expression (FIXED/INCLUDE/EXCLUDE)"),
        (r"\b(WINDOW_\w+|RUNNING_\w+|LOOKUP|PREVIOUS_VALUE|INDEX|SIZE|FIRST|LAST|TOTAL|RANK\w*)\s*\(", "table calculation"),
        (r"\b(SCRIPT_\w+|RAWSQL\w*|MODEL_\w+)\s*\(", "external / raw SQL function"),
        (r"\[Parameters\]\s*\.", "Tableau parameter reference"),
        (r"\bDATEPARSE\s*\(", "DATEPARSE (format strings differ)"),
    ]
    AGG = (r"\b(SUM|AVG|COUNT|MIN|MAX|MEDIAN|STDDEV_SAMP|STDDEV_POP|VAR_SAMP|VAR_POP|PERCENTILE"
           r"|COLLECT_LIST|COLLECT_SET|CORR|COVAR_\w+|ANY_VALUE|APPROX_\w+|MEASURE)\s*\(")
    UNITS = {"YEAR", "QUARTER", "MONTH", "WEEK", "DAY", "HOUR", "MINUTE", "SECOND"}

    # ---- Rule-based conversion ----
    def rule_convert(f, colmap):
        notes = []
        plain = re.sub(STR_LIT, "''", f)
        for pat, label in UNSUPPORTED:
            if re.search(pat, plain, re.I):
                return None, ["unsupported: " + label], True

        def unit(a):
            u = a.strip().strip("'\"").upper()
            if u not in UNITS:
                raise ValueError("unsupported date part " + u)
            return u

        def attr(a):
            notes.append("ATTR() approximated with MIN()")
            return f"MIN({a[0]})"

        calls = {
            "ZN": lambda a: f"COALESCE({a[0]}, 0)",
            "IFNULL": lambda a: f"COALESCE({a[0]}, {a[1]})",
            "ISNULL": lambda a: f"({a[0]} IS NULL)",
            "IIF": lambda a: f"IF({', '.join(a[:3])})",
            "COUNTD": lambda a: f"COUNT(DISTINCT {a[0]})",
            "ATTR": attr,
            "STDEVP": lambda a: f"STDDEV_POP({a[0]})",
            "STDEV": lambda a: f"STDDEV_SAMP({a[0]})",
            "VARP": lambda a: f"VAR_POP({a[0]})",
            "VAR": lambda a: f"VAR_SAMP({a[0]})",
            "LEN": lambda a: f"LENGTH({a[0]})",
            "MID": lambda a: f"SUBSTRING({', '.join(a)})",
            "FIND": lambda a: f"INSTR({a[0]}, {a[1]})",
            "STR": lambda a: f"CAST({a[0]} AS STRING)",
            "FLOAT": lambda a: f"CAST({a[0]} AS DOUBLE)",
            "INT": lambda a: f"CAST({a[0]} AS INT)",
            "DATETRUNC": lambda a: f"DATE_TRUNC('{unit(a[0])}', {a[1]})",
            "DATEDIFF": lambda a: f"DATEDIFF({unit(a[0])}, {a[1]}, {a[2]})",
            "DATEADD": lambda a: f"DATEADD({unit(a[0])}, {a[1]}, {a[2]})",
            "DATEPART": lambda a: f"EXTRACT({unit(a[0])} FROM {a[1]})",
            "TODAY": lambda a: "CURRENT_DATE()",
            "NOW": lambda a: "CURRENT_TIMESTAMP()",
            "MAX": lambda a: f"GREATEST({', '.join(a)})" if len(a) > 1 else f"MAX({a[0]})",
            "MIN": lambda a: f"LEAST({', '.join(a)})" if len(a) > 1 else f"MIN({a[0]})",
        }

        def col_sub(m):
            ref = ws(m.group(1))
            return colmap.get(ref, qcol(ref))

        def kw(p):
            p = p.replace("==", "=")
            p = re.sub(r"\bELSEIF\b", "WHEN", p, flags=re.I)
            return re.sub(r"\bIF\b", "CASE WHEN", p, flags=re.I)

        s = re.sub(r"\s+", " ", f).strip()
        s = re.sub(r"\[([^\]]+)\]", col_sub, s)
        s = re.sub(r'"((?:[^"\\]|\\.)*)"', lambda m: "'" + m.group(1).replace("'", "\\'") + "'", s)
        s = map_outside(s, kw)
        try:
            for fn, build in calls.items():
                s = rewrite_call(s, fn, build)
        except Exception as e:  # bad arg counts, unsupported date parts, ...
            return None, [f"unsupported: could not translate function call ({e})"], True

        if re.search(r"'\s*\+|\+\s*'", s):
            notes.append("string concatenation with '+' - consider CONCAT()")
        if "^" in plain:
            notes.append("'^' is XOR in Databricks SQL - use POWER() if exponent was intended")
        return s, notes, False

    # ---- LLM-based conversion ----
    def llm_convert(name, f, base, joins, colmap):
        from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

        system = (
            "You translate one Tableau calculated-field formula into a single Databricks SQL expression "
            "for a Unity Catalog metric view. Replace every Tableau [Field] reference with the exact SQL "
            "given for it in column_mapping (joined-table columns are already prefixed with their join "
            "alias); for references missing from column_mapping, use the closest column of the source "
            "table. Do not invent table names or joins. Expressions with aggregates (SUM, COUNT, ...) are "
            "measures. If the formula uses LOD expressions, table calculations, parameters, or anything "
            "not expressible, set unsupported to true. "
            'Reply with ONLY JSON: {"expr": string or null, "unsupported": boolean, "notes": string}.'
        )
        payload = {
            "calculation_name": name,
            "tableau_formula": f,
            "source_table": {"name": base, "columns": table_cols.get(base, {})},
            "joined_tables": [{"alias": j["name"], "table": j["source"],
                               "columns": table_cols.get(j["source"], {})} for j in joins],
            "column_mapping": {f"[{k}]": v for k, v in colmap.items()},
        }
        resp = w.serving_endpoints.query(
            name=llm_endpoint,
            messages=[ChatMessage(role=ChatMessageRole.SYSTEM, content=system),
                      ChatMessage(role=ChatMessageRole.USER, content=json.dumps(payload))],
            max_tokens=1024,
            temperature=0.0,
        )
        text = resp.choices[0].message.content.strip()
        text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text).strip()
        data = json.loads(text)
        if data.get("unsupported") or not data.get("expr"):
            return None, ["unsupported: " + (data.get("notes") or "LLM could not translate")], True
        return data["expr"].strip(), ([data["notes"]] if data.get("notes") else []), False

    # ---- YAML / SQL ----
    def q(v):  # JSON strings are valid YAML double-quoted scalars
        return json.dumps(v, ensure_ascii=False)

    def build_yaml(slug, name, kind, expr, comment, base, joins):
        lines = ["version: 1.1", f"comment: {q(comment)}", f"source: {q(base)}"]
        if joins:
            lines.append("joins:")
            for j in joins:
                lines += [f"  - name: {q(j['name'])}", f"    source: {q(j['source'])}", f"    on: {q(j['on'])}"]
        if kind == "dimension" or include_source_fields:
            lines.append("fields:")
            if include_source_fields:
                lines.append('  - expr: "source.*"')
            if kind == "dimension":
                lines += [f"  - name: {q(slug)}", f"    expr: {q(expr)}", f"    display_name: {q(name)}"]
        lines.append("measures:")
        if kind == "measure":
            lines += [f"  - name: {q(slug)}", f"    expr: {q(expr)}", f"    display_name: {q(name)}"]
        else:
            lines += ['  - name: "record_count"', '    expr: "COUNT(1)"', '    display_name: "Record Count"']
        return "\n".join(lines)

    df = pd.read_json(StringIO(calcs_json), orient="records", dtype=False, convert_dates=False)
    used, results = set(), []
    for r in df.to_dict("records"):
        slug = re.sub(r"[^0-9a-z]+", "_", str(r["name"]).lower()).strip("_") or "calc"
        if slug[0].isdigit():
            slug = "_" + slug
        if slug in used:
            slug += "_" + re.sub(r"[^0-9a-z]+", "", str(r["calc_id"]).lower())[-6:]
        used.add(slug)

        formula = r["formula_inlined"]
        hints = r.get("datasources") or []
        if isinstance(hints, str):
            hints = [hints]
        base, joins, colmap, plan_notes, fatal = plan_sources(formula, hints)

        schema_path = target_schema or base.rsplit(".", 1)[0]
        view_name = f"{schema_path}.{view_prefix}{slug}"

        converted_by = conversion_mode
        if fatal:
            expr, notes, bad = None, ["unsupported: " + fatal], True
        else:
            try:
                if conversion_mode == "llm":
                    expr, notes, bad = llm_convert(r["name"], formula, base, joins, colmap)
                else:
                    expr, notes, bad = rule_convert(formula, colmap)
            except Exception as e:
                expr, notes, bad = rule_convert(formula, colmap)
                notes = [f"LLM conversion failed ({type(e).__name__}); used rule-based fallback"] + notes
                converted_by = "rule (llm fallback)"
        notes = plan_notes + notes

        rec = {"name": r["name"], "calc_id": r["calc_id"], "view_name": view_name,
               "kind": None, "status": "unsupported", "converted_by": converted_by,
               "source_table": base, "joins": [j["source"] for j in joins],
               "notes": notes, "tableau_formula": r["formula"], "expr": expr, "sql": None}
        if not bad:
            plain_out = re.sub(r"'(?:[^']|'')*'", "''", expr)
            kind = "measure" if re.search(AGG, plain_out, re.I) else "dimension"
            comment = f"Tableau calculated field '{r['name']}' ({r['calc_id']}). Original formula: {r['formula']}"
            yaml_text = build_yaml(slug, r["name"], kind, expr, comment, base, joins)
            qualified = ".".join("`" + p.replace("`", "``") + "`" for p in view_name.split("."))
            rec.update({
                "kind": kind,
                "status": "needs_review" if notes else "ok",
                "sql": f"CREATE OR REPLACE VIEW {qualified} WITH METRICS LANGUAGE YAML AS\n$$\n{yaml_text}\n$$",
            })
        results.append(rec)
    return json.dumps(results)


def push_metric_views(
    views_json: str,
    warehouse_id: str,
    skip_unsupported: bool = True,
    dry_run: bool = False,
) -> str:
    """
    Create the metric views in Databricks using the Statement Execution API.

    Args:
        views_json: JSON output of build_metric_views.
        warehouse_id: ID of the SQL warehouse (DBR 17.3+) that runs the CREATE VIEW statements.
        skip_unsupported: Skip calculations whose status is 'unsupported'.
        dry_run: If true, do not execute anything; just report what would be created.

    Returns:
        JSON array with one record per view: view_name, status (created / failed / skipped / dry_run), error.
    """
    import json
    import time

    results = []
    w = None
    if not dry_run:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.sql import StatementState
        w = WorkspaceClient()

    for v in json.loads(views_json):
        out = {"view_name": v["view_name"], "status": None, "error": None}
        if not v.get("sql") or (skip_unsupported and v["status"] == "unsupported"):
            out.update(status="skipped", error="; ".join(v.get("notes") or []))
        elif dry_run:
            out["status"] = "dry_run"
        else:
            try:
                resp = w.statement_execution.execute_statement(
                    statement=v["sql"], warehouse_id=warehouse_id, wait_timeout="30s")
                waited = 0
                while resp.status.state in (StatementState.PENDING, StatementState.RUNNING) and waited < 300:
                    time.sleep(2)
                    waited += 2
                    resp = w.statement_execution.get_statement(resp.statement_id)
                if resp.status.state == StatementState.SUCCEEDED:
                    out["status"] = "created"
                else:
                    out["status"] = "failed"
                    out["error"] = resp.status.error.message if resp.status.error else str(resp.status.state)
            except Exception as e:
                out.update(status="failed", error=str(e))
        results.append(out)
    return json.dumps(results)


def resolve_warehouse_id(name_or_id: str) -> str:
    """Accept a SQL warehouse name or ID and return the ID."""
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    for wh in w.warehouses.list():
        if name_or_id in (wh.id, wh.name):
            return wh.id
    raise ValueError(f"SQL warehouse '{name_or_id}' not found")


def twbx_to_metric_views(
    twbx_path: str,
    source_tables: str,
    warehouse: str,
    conversion_mode: str = "rule",
    llm_endpoint: str = "",
    target_schema: str = "",
    dry_run: bool = True,
):
    """Local orchestrator (not registered in UC). Runs parse -> build -> push and returns DataFrames."""
    import json
    from io import StringIO

    import pandas as pd

    calcs_json = parse_twbx_calculations(twbx_path)
    calcs_df = pd.read_json(StringIO(calcs_json), orient="records", dtype=False, convert_dates=False)
    views_json = build_metric_views(calcs_json, source_tables, conversion_mode, llm_endpoint,
                                    target_schema=target_schema)
    views_df = pd.DataFrame(json.loads(views_json))
    warehouse_id = warehouse if dry_run else resolve_warehouse_id(warehouse)
    push_df = pd.DataFrame(json.loads(push_metric_views(views_json, warehouse_id, dry_run=dry_run)))
    return calcs_df, views_df, push_df


def register_as_uc_functions(catalog: str, schema: str):
    """Register the three tool functions as Unity Catalog Python functions (needs unitycatalog-ai)."""
    from unitycatalog.ai.core.databricks import DatabricksFunctionClient

    client = DatabricksFunctionClient()
    for fn in (parse_twbx_calculations, build_metric_views, push_metric_views):
        client.create_python_function(func=fn, catalog=catalog, schema=schema, replace=True)


if __name__ == "__main__":
    print(f"Workbook: {twbx_path}")
    print(f"Candidate source tables: {source_tables}")
    calcs, views, pushed = twbx_to_metric_views(
        twbx_path=twbx_path,
        source_tables=source_tables,
        warehouse=target_warehouse,
        conversion_mode=conversion_mode,
        llm_endpoint=llm_endpoint,
        target_schema=metric_view_schema,
        dry_run=dry_run,
    )
    print(calcs[["name", "calc_id", "formula"]])
    if not views.empty:
        print(views[["name", "view_name", "source_table", "joins", "kind", "status", "notes"]])
    print(pushed)