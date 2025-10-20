# gradSERU_College_Graph_App.py
# ------------------------------------------------------------
# One-file app that combines:
#  • Tkinter GUI (with scrolling log, responsive while running)
#  • Graph and heatmap generation (formerly College_Graph_Generation)
#
# Requirements: pandas, openpyxl, matplotlib
# Optional: Pillow (for the banner image)
#   pip install pandas openpyxl matplotlib pillow
# ------------------------------------------------------------

import sys
import os
import re
import threading
from contextlib import redirect_stdout, redirect_stderr
from io import StringIO
from pathlib import Path

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")           # use non-GUI backend for background threads
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap

import tkinter as tk
from tkinter import ttk, filedialog, messagebox

try:
    from PIL import Image, ImageTk
    PIL_OK = True
except Exception:
    PIL_OK = False

# =========================
# USER CONFIG (defaults used if GUI inputs are empty)
# =========================
MAP_PATH = Path(r"D:/RA/Code Base/Graph Generation/gradSERU_MAP.xlsx")
COLLEGES_ROOT = Path(r"D:/RA/Data/UMN_College/")
OUTPUT_DIR = Path(r"D:/RA/Data/UMN_College_Output/")

# Only build a heatmap if we have at least this many departments/programs
HEATMAP_MIN_UNITS = 4  # "more than 3"

# Derived dirs — created lazily
CATEGORIES_DIR = OUTPUT_DIR / "Summary Scores (Excel)"
DETAILS_DIR    = OUTPUT_DIR / "Detailed Category Results"
GRAPH_DIR      = OUTPUT_DIR / "Summary Scores (Graph)"

# Colleges subtree — created lazily
COLLEGES_BASE    = OUTPUT_DIR / "Colleges"
COLLEGES_CATS    = COLLEGES_BASE / "Summary Scores (Excel)"
COLLEGES_DETAILS = COLLEGES_BASE / "Detailed Category Results"
COLLEGES_GRAPHS  = COLLEGES_BASE / "Summary Scores (Graph)"   # singular as requested
COLLEGES_HEATMAP = COLLEGES_BASE / "Summary Scores (Heatmap)"

# Departments subtree (used in mixed case)
DEPTS_BASE         = OUTPUT_DIR / "Departments"
DEPTS_BASE_CATS    = DEPTS_BASE / "Summary Scores (Excel)"
DEPTS_BASE_DETAILS = DEPTS_BASE / "Detailed Category Results"
DEPTS_BASE_GRAPHS  = DEPTS_BASE / "Summary Scores (Graph)"
DEPTS_BASE_HEATMAP = DEPTS_BASE / "Summary Scores (Heatmap)"


def _init_output_dirs():
    """Recompute global paths whenever OUTPUT_DIR changes."""
    global CATEGORIES_DIR, DETAILS_DIR, GRAPH_DIR
    global COLLEGES_BASE, COLLEGES_CATS, COLLEGES_DETAILS, COLLEGES_GRAPHS, COLLEGES_HEATMAP
    global DEPTS_BASE, DEPTS_BASE_CATS, DEPTS_BASE_DETAILS, DEPTS_BASE_GRAPHS, DEPTS_BASE_HEATMAP

    CATEGORIES_DIR = OUTPUT_DIR / "Summary Scores (Excel)"
    DETAILS_DIR    = OUTPUT_DIR / "Detailed Category Results"
    GRAPH_DIR      = OUTPUT_DIR / "Summary Scores (Graph)"

    COLLEGES_BASE    = OUTPUT_DIR / "Colleges"
    COLLEGES_CATS    = COLLEGES_BASE / "Summary Scores (Excel)"
    COLLEGES_DETAILS = COLLEGES_BASE / "Detailed Category Results"
    COLLEGES_GRAPHS  = COLLEGES_BASE / "Summary Scores (Graph)"
    COLLEGES_HEATMAP = COLLEGES_BASE / "Summary Scores (Heatmap)"

    DEPTS_BASE         = OUTPUT_DIR / "Departments"
    DEPTS_BASE_CATS    = DEPTS_BASE / "Summary Scores (Excel)"
    DEPTS_BASE_DETAILS = DEPTS_BASE / "Detailed Category Results"
    DEPTS_BASE_GRAPHS  = DEPTS_BASE / "Summary Scores (Graph)"
    DEPTS_BASE_HEATMAP = DEPTS_BASE / "Summary Scores (Heatmap)"

# color for heatmap


# Initialize once
_init_output_dirs()

# =========================
# Generation constants & helpers
# =========================

POSSIBLE_PCT_COLS = [
    "% of Total # of Responses along Data Values - Dimension, Data Labels",
    "% Helpful, Very Helpful, or Extremely Helpful",
    "% Agree or Strongly agree",
    "% Moderate, Large, or Very Large extent",
    "% Well, Very Well, or Extremely Well",
]
POSSIBLE_WORDING_COLS = ["Wording", "Data Labels"]
POSSIBLE_RESP_COLS = ["# of Responses"]

# Special file marker (case-insensitive substring match)
OBSTACLES_MARKER = "ob_ obstacles to completion.xlsx"

# Fixed category order (treats '&' ~ 'and')
CATEGORY_ORDER = [
    "Maintaining Effective Communication",
    "Aligning Expectations",
    "Assessing Understanding",
    "Addressing Equity and Inclusion",
    "Fostering Independence",
    "Promoting Professional Development",
    "Cultivating Ethical Behavior",
    "Satisfaction with Advising",
    "Quality of Advising",
]

def _norm_cat(s: str) -> str:
    s = str(s).lower().replace("&", "and")
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()

_ORDER_MAP = { _norm_cat(c): i for i, c in enumerate(CATEGORY_ORDER) }

def _apply_category_order(df: pd.DataFrame) -> pd.DataFrame:
    if "Category" not in df.columns:
        return df
    out = df.copy()
    out["_cat_key"] = out["Category"].apply(_norm_cat)
    out["_order"] = out["_cat_key"].map(_ORDER_MAP).fillna(len(_ORDER_MAP))
    out = out.sort_values(["_order", "Category"], kind="stable").drop(columns=["_cat_key", "_order"])
    return out


def _find_first_present(columns, candidates):
    cols = set(columns)
    for name in candidates:
        if name in cols:
            return name
    return None

def _read_map(map_path: Path) -> pd.DataFrame:
    """Read and validate the MAP file; add _question_norm and _valid flags."""
    if map_path.suffix.lower() in (".xlsx", ".xls"):
        df = pd.read_excel(map_path)
    else:
        df = pd.read_csv(map_path)

    required = {"Question", "MAP Category"}
    missing = required - set(df.columns)
    if missing:
        print(
            f"[ERROR] MAP file is missing required column(s): {sorted(missing)}\n"
            'Make sure the column names are "Question" and "MAP Category".',
            flush=True,
        )
        raise SystemExit(2)

    df["_question_norm"] = (
        df["Question"].astype(str).fillna("")
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
        .str.lower()
    )

    def _is_nonempty(x):
        return (pd.notna(x)) and (str(x).strip() != "")

    df["_valid"] = df.apply(
        lambda r: _is_nonempty(r["Question"]) and _is_nonempty(r["MAP Category"]),
        axis=1,
    )
    return df

def _coerce_percent(val):
    if pd.isna(val):
        return None
    s = str(val).strip()
    if s == "":
        return None
    s = re.sub(r"[^0-9.\-]", "", s)
    try:
        return float(s)
    except ValueError:
        return None

def _read_one_excel(file_path: Path) -> pd.DataFrame:
    """Read one Excel and return question-level rows with Pct and Respondents."""
    try:
        df = pd.read_excel(file_path)
    except Exception as e:
        raise RuntimeError(f"Failed to read {file_path.name}: {e}")

    pct_col = _find_first_present(df.columns, POSSIBLE_PCT_COLS)
    if pct_col is None:
        raise ValueError(
            f"{file_path.name} is missing a % column. Expected one of: {POSSIBLE_PCT_COLS}"
        )

    wording_col = _find_first_present(df.columns, POSSIBLE_WORDING_COLS)
    if wording_col is None:
        raise ValueError(
            f"{file_path.name} is missing a wording column. Expected one of: {POSSIBLE_WORDING_COLS}"
        )

    resp_col = _find_first_present(df.columns, POSSIBLE_RESP_COLS)

    keep = df[[wording_col, pct_col]].copy()
    keep.rename(columns={wording_col: "QuestionText", pct_col: "PctRaw"}, inplace=True)

    keep["_wording_norm"] = (
        keep["QuestionText"]
        .astype(str)
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
        .str.lower()
    )
    keep["_pct"] = keep["PctRaw"].apply(_coerce_percent)  # expected 0..1

    if resp_col is not None:
        keep["Respondents"] = pd.to_numeric(df[resp_col], errors="coerce")
    else:
        keep["Respondents"] = pd.NA
        print(f"[WARN] '{file_path.name}' has no '# of Responses' column; Avg Respondents will be blank.", flush=True)

    return keep

# Depth-limited file iterators
def _iter_excels_here(dir_path: Path):
    for p in dir_path.glob("*"):
        if p.is_file() and p.suffix.lower() in (".xlsx", ".xls") and not p.name.startswith("~$"):
            yield p

def _iter_excels_deeper(dir_path: Path):
    for p in dir_path.glob("**/*"):
        if p.is_file() and p.suffix.lower() in (".xlsx", ".xls") and not p.name.startswith("~$"):
            if p.parent != dir_path:
                yield p

def _read_specific_files(files) -> pd.DataFrame:
    files = list(files)
    if not files:
        raise FileNotFoundError("No Excel files provided to read.")
    dfs, warns = [], []
    unit_name = Path(files[0]).parent.name if files else ""
    for f in files:
        try:
            df = _read_one_excel(f)
            df["_source_file"] = Path(f).name
            dfs.append(df)
        except Exception as e:
            warns.append(f"[WARN] {unit_name}: skipping '{Path(f).name}' -> {e}")
    for w in warns:
        print(w, flush=True)
    if not dfs:
        raise RuntimeError("All files failed. See warnings above.")

    all_df = pd.concat(dfs, ignore_index=True)
    agg = (
        all_df.groupby(["_wording_norm"], as_index=False)
        .agg(
            _pct=("_pct", "mean"),
            QuestionText=("QuestionText", "first"),
            PctRaw=("PctRaw", "first"),
            Respondents=("Respondents", "mean"),
            _source_file=("_source_file", lambda x: ", ".join(sorted(set(map(str, x))))),
        )
    )
    return agg

# folder-level "too deep" logger
_DEEP_OFFENDERS: set[Path] = set()

def _note_deep_folder(folder: Path):
    if folder not in _DEEP_OFFENDERS:
        print(f'[WARN] "{folder}" contains sub-folders inside it. Please keep only the Excel files directly inside each Program folder (no extra layers of folders).', flush=True)
        _DEEP_OFFENDERS.add(folder)

# presence scanner respecting depth rules
def _collect_all_present_norms_depth_limited(root: Path) -> set:
    present = set()

    for child in sorted(root.iterdir()):
        if not child.is_dir():
            continue

        files_here = list(_iter_excels_here(child))
        deeper     = list(_iter_excels_deeper(child))

        if files_here:
            if deeper:
                _note_deep_folder(child)
            try:
                df = _read_specific_files(files_here)
                present.update(df["_wording_norm"].unique())
            except Exception as e:
                print(f"[WARN] Presence scan failed for '{child.name}': {e}", flush=True)
            continue

        subdirs = [d for d in child.iterdir() if d.is_dir()]
        for dep in sorted(subdirs):
            dep_here   = list(_iter_excels_here(dep))
            dep_deeper = list(_iter_excels_deeper(dep))
            if dep_here:
                if dep_deeper:
                    _note_deep_folder(dep)
                try:
                    df = _read_specific_files(dep_here)
                    present.update(df["_wording_norm"].unique())
                except Exception as e:
                    print(f"[WARN] Presence scan failed for '{dep.name}': {e}", flush=True)
            else:
                if dep_deeper:
                    _note_deep_folder(dep)

    return present

def _safe_sheet_name(name: str, used: set) -> str:
    bad = r'[\[\]\:\*\?\/\\]'
    cleaned = re.sub(bad, "_", str(name)) or "Sheet"
    if len(cleaned) > 31:
        cleaned = cleaned[:31]
    base = cleaned
    i = 1
    while cleaned in used:
        suffix = f"_{i}"
        trimmed = base[: 31 - len(suffix)]
        cleaned = trimmed + suffix
        i += 1
    used.add(cleaned)
    return cleaned

def _safe_filename(name: str) -> str:
    return re.sub(r'[^\w\-. ]', "_", name)

def _apply_category_order_inplace(ax, title):
    ax.set_xlim(0.0, 1.0)
    ax.set_xlabel("Priority")
    ax.set_title(title)

def _plot_college_bar(unit_name: str, cat_scores: pd.DataFrame, out_dir: Path):
    if cat_scores.empty:
        return
    plot_df = _apply_category_order(cat_scores.copy())
    if "Avg Respondents" in plot_df.columns:
        labels = [
            f"{c} ({int(r) if pd.notna(r) else 0})"
            for c, r in zip(plot_df["Category"], plot_df["Avg Respondents"])
        ]
    else:
        labels = list(plot_df["Category"])

    fig, ax = plt.subplots(figsize=(9, max(3, 0.5 * len(plot_df))))
    ax.barh(labels, plot_df["Priority Score"], color="#ffa800")
    ax.invert_yaxis()
    _apply_category_order_inplace(ax, unit_name)

    for y, v in enumerate(plot_df["Priority Score"].values):
        ax.text(v + 0.01 if v <= 0.95 else 0.99, y, f"{v:.2f}", va="center",
                ha="left" if v <= 0.95 else "right")
    fig.tight_layout()
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{_safe_filename(unit_name)}_priority.png"
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)

# Heatmap helpers
def _build_heatmap_matrix(scores_by_unit: dict[str, pd.DataFrame]) -> pd.DataFrame:
    if not scores_by_unit:
        return pd.DataFrame()

    all_cats = set()
    for df in scores_by_unit.values():
        if "Category" in df.columns:
            all_cats.update(df["Category"].dropna().astype(str))
    if not all_cats:
        return pd.DataFrame()

    ordered_cols, seen = [], set()
    for c in CATEGORY_ORDER:
        if c in all_cats and c not in seen:
            ordered_cols.append(c); seen.add(c)
    for c in sorted(all_cats):
        if c not in seen:
            ordered_cols.append(c); seen.add(c)

    units = sorted(scores_by_unit.keys())
    mat = pd.DataFrame(index=units, columns=ordered_cols, dtype=float)
    for unit, df in scores_by_unit.items():
        if {"Category", "Priority Score"} <= set(df.columns):
            s = df.set_index("Category")["Priority Score"]
            for cat, val in s.items():
                if cat in mat.columns:
                    mat.loc[unit, cat] = float(val)

    mat = mat.dropna(axis=1, how="all")
    return mat

def _save_heatmap_excel_and_png(matrix_df: pd.DataFrame, title: str, out_dir: Path, base_name: str):
    """
    Save the heatmap matrix as Excel and a polished PNG.

    Features:
    - White text annotations for better contrast
    - White grid lines between squares
    - Custom blue→orange color scale (#2b5c8a → #e88204)
    - Auto-scaled layout for any number of programs/categories
    """
    if matrix_df.empty or matrix_df.shape[1] == 0:
        return

    out_dir.mkdir(parents=True, exist_ok=True)

    # 1️⃣ Excel export
    excel_path = out_dir / f"{_safe_filename(base_name)}_heatmap.xlsx"
    matrix_df.reset_index().rename(columns={"index": "Unit"}).to_excel(
        excel_path, sheet_name="Heatmap", index=False
    )

    # 2️⃣ Figure setup
    n_rows, n_cols = matrix_df.shape
    fig_w = max(8.0, 0.9 * n_cols + 3.0)
    fig_h = max(4.5, 0.6 * n_rows + 1.5)
    fig, ax = plt.subplots(figsize=(fig_w, fig_h), dpi=300)

    # 3️⃣ Prepare data and colormap
    data = matrix_df.to_numpy(dtype=float)
    masked = np.ma.masked_invalid(data)

    # Blue → Orange gradient (0→1)
    cmap = LinearSegmentedColormap.from_list("blue_orange", ["#2b5c8a", "#e88204"])
    cmap.set_bad("#e0e0e0")  # gray for missing values

    # 4️⃣ Draw with white gridlines
    mesh = ax.pcolormesh(
        masked,
        cmap=cmap,
        edgecolors="white",
        linewidth=1.0,
        shading="flat",
        vmin=0.0,
        vmax=1.0,
    )

    # 5️⃣ Axis labels
    ax.set_xticks(np.arange(n_cols) + 0.5)
    ax.set_yticks(np.arange(n_rows) + 0.5)
    ax.set_xticklabels(matrix_df.columns, rotation=35, ha="right", va="top")
    ax.set_yticklabels(matrix_df.index)
    ax.invert_yaxis()

    # 6️⃣ Colorbar
    cbar = plt.colorbar(mesh, ax=ax, fraction=0.046, pad=0.04)
    cbar.set_label("Priority Score (0 = low, 1 = high)", rotation=90)

    # 7️⃣ Annotate each cell (white text)
    ann_fs = 10 if max(n_rows, n_cols) <= 12 else 8
    for i in range(n_rows):
        for j in range(n_cols):
            val = data[i, j]
            if not np.isnan(val):
                ax.text(
                    j + 0.5, i + 0.5, f"{val:.2f}",
                    ha="center", va="center",
                    fontsize=ann_fs, color="white", fontweight="bold"
                )

    # 8️⃣ Title and layout
    ax.set_title(title, fontsize=16, pad=10, color="black")
    ax.set_xlim(0, n_cols)
    ax.set_ylim(0, n_rows)
    fig.tight_layout()
    fig.subplots_adjust(bottom=0.25)

    # 9️⃣ Save PNG
    png_path = out_dir / f"{_safe_filename(base_name)}_heatmap.png"
    plt.savefig(png_path, bbox_inches="tight")
    plt.close(fig)

# Core (unit = department)
def compute_unit(dir_path: Path, map_df: pd.DataFrame, out_cats: Path, out_details: Path, out_graphs: Path):
    files_here = list(_iter_excels_here(dir_path))
    if not files_here:
        raise FileNotFoundError(f"No Excel files directly under {dir_path}")

    unit_name = dir_path.name
    unit_df = _read_specific_files(files_here)

    present_norms = set(unit_df["_wording_norm"].unique())

    merged = unit_df.merge(
        map_df[["_question_norm", "MAP Category"]],
        left_on="_wording_norm",
        right_on="_question_norm",
        how="inner",
        validate="m:m",
    ).copy()

    merged = merged.dropna(subset=["MAP Category"]).copy()
    merged["CategoryList"] = merged["MAP Category"].astype(str).str.split(",")
    merged["CategoryList"] = merged["CategoryList"].apply(
        lambda lst: [c.strip() for c in lst if c and c.strip()]
    )
    merged = merged.explode("CategoryList").rename(columns={"CategoryList": "Category"}).reset_index(drop=True)

    def _priority_row(row):
        p = row["_pct"]
        if p is None:
            return None
        src = str(row.get("_source_file", "")).lower()
        if OBSTACLES_MARKER in src:
            return 1 if p > 0.20 else 0
        return 1 if p < 0.80 else 0

    merged["priority"] = merged.apply(_priority_row, axis=1)
    merged = merged.dropna(subset=["priority"])

    cat_scores = (
        merged.groupby("Category", as_index=False)
        .agg(
            **{
                "Priority Score": ("priority", "mean"),
                "Avg Respondents": ("Respondents", "mean"),
            }
        )
    )

    cat_scores["Priority Score"] = cat_scores["Priority Score"].round(3)
    cat_scores["Avg Respondents"] = cat_scores["Avg Respondents"].round(0).astype("Int64")

    cat_scores.insert(0, "Unit", unit_name)
    cat_scores = _apply_category_order(cat_scores)

    # Detailed workbook
    out_details.mkdir(parents=True, exist_ok=True)
    detail_path = out_details / f"{_safe_filename(unit_name)}_category_details.xlsx"
    used_names = set()
    with pd.ExcelWriter(detail_path, engine="openpyxl") as writer:
        _apply_category_order(cat_scores)[["Category", "Priority Score", "Avg Respondents"]].to_excel(
            writer, sheet_name=_safe_sheet_name("Summary", used_names), index=False
        )
        seen = set()
        for pref in CATEGORY_ORDER:
            key = _norm_cat(pref)
            for cat in merged["Category"].unique():
                if _norm_cat(cat) == key and cat not in seen:
                    seen.add(cat)
                    out = merged[merged["Category"] == cat][
                        ["QuestionText", "PctRaw", "_pct", "Respondents", "priority", "_source_file"]
                    ].copy()
                    out = out.sort_values(by=["priority", "_pct", "QuestionText"], ascending=[False, True, True])
                    out.rename(columns={"PctRaw": "Original", "_pct": "Numeric (0-1)"}, inplace=True)
                    out.to_excel(writer, sheet_name=_safe_sheet_name(cat, used_names), index=False)
        for cat in merged["Category"].unique():
            if cat in seen:
                continue
            out = merged[merged["Category"] == cat][
                ["QuestionText", "PctRaw", "_pct", "Respondents", "priority", "_source_file"]
            ].copy()
            out = out.sort_values(by=["priority", "_pct", "QuestionText"], ascending=[False, True, True])
            out.rename(columns={"PctRaw": "Original", "_pct": "Numeric (0-1)"}, inplace=True)
            out.to_excel(writer, sheet_name=_safe_sheet_name(cat, used_names), index=False)

    # Per-unit summary CSV
    out_cats.mkdir(parents=True, exist_ok=True)
    out_csv = out_cats / f"{_safe_filename(unit_name)}_category_priority.csv"
    _apply_category_order(cat_scores)[["Category", "Priority Score", "Avg Respondents"]].to_csv(out_csv, index=False)

    # Graph
    _plot_college_bar(unit_name, cat_scores[["Category", "Priority Score", "Avg Respondents"]], out_graphs)

    return cat_scores, present_norms

def _write_questions_check_per_unit(map_df: pd.DataFrame, present_norms: set, unit_name: str, out_folder: Path):
    out_folder.mkdir(parents=True, exist_ok=True)
    qdf = map_df[["Question", "_question_norm", "_valid"]].drop_duplicates().reset_index(drop=True)
    vals = []
    for _, row in qdf.iterrows():
        if not row["_valid"]:
            vals.append("N/A")
        else:
            vals.append("Y" if row["_question_norm"] in present_norms else "N")
    df = pd.DataFrame(
        {
            "Question": qdf["Question"].where(qdf["Question"].astype(str).str.strip() != "", "(blank)"),
            "Present": vals,
        }
    )
    out_path = out_folder / f"{_safe_filename(unit_name)}_Questions_Check.xlsx"
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Presence", index=False)

def _write_questions_check_matrix(map_df: pd.DataFrame, presence_by_unit: dict, unit_order: list, out_path: Path):
    qdf = map_df[["Question", "_question_norm", "_valid"]].drop_duplicates().reset_index(drop=True)
    matrix = []
    for _, row in qdf.iterrows():
        if not row["_valid"]:
            matrix.append(["N/A"] * len(unit_order))
        else:
            qn = row["_question_norm"]
            row_vals = [("Y" if qn in presence_by_unit.get(u, set()) else "N") for u in unit_order]
            matrix.append(row_vals)
    check_df = pd.DataFrame(matrix, columns=unit_order)
    check_df.insert(0, "Question", qdf["Question"].where(qdf["Question"].astype(str).str.strip() != "", "(blank)"))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        check_df.to_excel(writer, sheet_name="Presence", index=False)

# =========================
# Top-level generation entrypoint (formerly main())
# =========================
def run_generation(map_path: Path, colleges_root: Path, output_dir: Path):
    global MAP_PATH, COLLEGES_ROOT, OUTPUT_DIR
    MAP_PATH = Path(map_path)
    COLLEGES_ROOT = Path(colleges_root)
    OUTPUT_DIR = Path(output_dir)
    _init_output_dirs()

    map_df = _read_map(MAP_PATH)

    valid_map_norms = set(map_df.loc[map_df["_valid"], "_question_norm"].unique())
    if not valid_map_norms:
        print("[ERROR] MAP file has no valid (non-blank) Question + MAP Category rows.", flush=True)
        raise SystemExit(3)

    colleges, departments = [], []

    for child in sorted(COLLEGES_ROOT.iterdir()):
        if not child.is_dir():
            continue

        files_here = list(_iter_excels_here(child))
        deeper     = list(_iter_excels_deeper(child))

        if files_here:
            if deeper:
                _note_deep_folder(child)
            departments.append(child)
            continue

        subdirs = [d for d in child.iterdir() if d.is_dir()]
        immediate_dep_with_files = False
        for dep in sorted(subdirs):
            dep_here   = list(_iter_excels_here(dep))
            dep_deeper = list(_iter_excels_deeper(dep))
            if dep_here:
                immediate_dep_with_files = True
                if dep_deeper:
                    _note_deep_folder(dep)
            else:
                if dep_deeper:
                    _note_deep_folder(dep)
        if immediate_dep_with_files:
            colleges.append(child)
        else:
            if deeper:
                _note_deep_folder(child)

    all_present_norms = _collect_all_present_norms_depth_limited(COLLEGES_ROOT)
    if valid_map_norms.isdisjoint(all_present_norms):
        print("[ERROR] No questions from the MAP are present in the provided Excel files. Nothing to generate.", flush=True)
        raise SystemExit(4)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    combined_results = []

    have_colleges = len(colleges) > 0
    have_departments = len(departments) > 0

    if have_colleges and not have_departments:
        questions_root = COLLEGES_BASE / "Questions_Check"
        for col in colleges:
            print(f"Processing college: {col.name}", flush=True)
            dept_dirs = [d for d in sorted(col.iterdir()) if d.is_dir() and list(_iter_excels_here(d))]
            if not dept_dirs:
                continue

            presence_by_dept = {}
            dept_names = []
            dept_scores_for_heatmap = {}

            for dep in dept_dirs:
                print(f"  - Department: {dep.name}", flush=True)
                out_cats    = COLLEGES_CATS / col.name
                out_details = COLLEGES_DETAILS / col.name
                out_graphs  = COLLEGES_GRAPHS / col.name
                res, present = compute_unit(dep, map_df, out_cats, out_details, out_graphs)
                combined_results.append(res)
                presence_by_dept[dep.name] = present
                dept_names.append(dep.name)
                dept_scores_for_heatmap[dep.name] = res[["Category", "Priority Score"]].copy()

            _write_questions_check_matrix(
                map_df, presence_by_dept, dept_names,
                questions_root / f"{_safe_filename(col.name)}_Questions_Check.xlsx",
            )

            if len(dept_scores_for_heatmap) >= HEATMAP_MIN_UNITS:
                matrix = _build_heatmap_matrix(dept_scores_for_heatmap)
                _save_heatmap_excel_and_png(
                    matrix_df=matrix,
                    title=f"{col.name} - Priority Heatmap",
                    out_dir=COLLEGES_HEATMAP / col.name,
                    base_name=f"{col.name}"
                )
            else:
                print(f"[INFO] Heatmap skipped for '{col.name}' - needs >={HEATMAP_MIN_UNITS} departments; got {len(dept_scores_for_heatmap)}.", flush=True)

    elif have_departments and not have_colleges:
        print("Detected departments at the root (no colleges). Using flat outputs.", flush=True)
        presence_by_dept = {}
        dept_names = []
        dept_scores_for_heatmap = {}

        for dep in departments:
            print(f"Processing department: {dep.name}", flush=True)
            res, present = compute_unit(dep, map_df, CATEGORIES_DIR, DETAILS_DIR, GRAPH_DIR)
            combined_results.append(res)
            presence_by_dept[dep.name] = present
            dept_names.append(dep.name)
            dept_scores_for_heatmap[dep.name] = res[["Category", "Priority Score"]].copy()

        _write_questions_check_matrix(
            map_df, presence_by_dept, dept_names, OUTPUT_DIR / "Questions_Check.xlsx",
        )

        if len(dept_scores_for_heatmap) >= HEATMAP_MIN_UNITS:
            matrix = _build_heatmap_matrix(dept_scores_for_heatmap)
            _save_heatmap_excel_and_png(
                matrix_df=matrix,
                title="Departments - Priority Heatmap",
                out_dir=DEPTS_BASE_HEATMAP,
                base_name="Departments"
            )
        else:
            print(f"[INFO] Global departments heatmap skipped - needs >={HEATMAP_MIN_UNITS}; got {len(dept_scores_for_heatmap)}.", flush=True)

    else:
        print("Detected a mix of colleges and top-level departments.", flush=True)

        questions_root = COLLEGES_BASE / "Questions_Check"
        for col in colleges:
            print(f"Processing college: {col.name}", flush=True)
            dept_dirs = [d for d in sorted(col.iterdir()) if d.is_dir() and list(_iter_excels_here(d))]
            if not dept_dirs:
                continue

            presence_by_dept = {}
            dept_names = []
            dept_scores_for_heatmap = {}

            for dep in dept_dirs:
                print(f"  - Department: {dep.name}", flush=True)
                out_cats    = COLLEGES_CATS / col.name
                out_details = COLLEGES_DETAILS / col.name
                out_graphs  = COLLEGES_GRAPHS / col.name
                res, present = compute_unit(dep, map_df, out_cats, out_details, out_graphs)
                combined_results.append(res)
                presence_by_dept[dep.name] = present
                dept_names.append(dep.name)
                dept_scores_for_heatmap[dep.name] = res[["Category", "Priority Score"]].copy()

            _write_questions_check_matrix(
                map_df, presence_by_dept, dept_names,
                questions_root / f"{_safe_filename(col.name)}_Questions_Check.xlsx",
            )

            if len(dept_scores_for_heatmap) >= HEATMAP_MIN_UNITS:
                matrix = _build_heatmap_matrix(dept_scores_for_heatmap)
                _save_heatmap_excel_and_png(
                    matrix_df=matrix,
                    title=f"{col.name} - Priority Heatmap",
                    out_dir=COLLEGES_HEATMAP / col.name,
                    base_name=f"{col.name}"
                )
            else:
                print(f"[INFO] Heatmap skipped for '{col.name}' - needs >={HEATMAP_MIN_UNITS} departments; got {len(dept_scores_for_heatmap)}.", flush=True)

        root_dept = [d for d in departments]
        if root_dept:
            presence_by_dept = {}
            dept_names = []
            dept_scores_for_heatmap = {}

            for dep in root_dept:
                print(f"Processing department: {dep.name}", flush=True)
                res, present = compute_unit(dep, map_df, DEPTS_BASE_CATS, DEPTS_BASE_DETAILS, DEPTS_BASE_GRAPHS)
                combined_results.append(res)
                presence_by_dept[dep.name] = present
                dept_names.append(dep.name)
                dept_scores_for_heatmap[dep.name] = res[["Category", "Priority Score"]].copy()

            _write_questions_check_matrix(
                map_df, presence_by_dept, dept_names, DEPTS_BASE / "Questions_Check.xlsx",
            )

            if len(dept_scores_for_heatmap) >= HEATMAP_MIN_UNITS:
                matrix = _build_heatmap_matrix(dept_scores_for_heatmap)
                _save_heatmap_excel_and_png(
                    matrix_df=matrix,
                    title="Departments - Priority Heatmap",
                    out_dir=DEPTS_BASE_HEATMAP,
                    base_name="Departments"
                )
            else:
                print(f"[INFO] Root departments heatmap skipped - needs >={HEATMAP_MIN_UNITS}; got {len(dept_scores_for_heatmap)}.", flush=True)

    if not combined_results:
        raise RuntimeError("No results produced. Check folder structure and files.")

    combined = pd.concat(combined_results, ignore_index=True)
    combined["Priority Score"] = combined["Priority Score"].round(3)
    combined_out = OUTPUT_DIR / "All_Programs_Priority_Summary.csv"
    combined.to_csv(combined_out, index=False)

    print("\nAnalysis Completed Successfully.", flush=True)
    print(f"Check this Folder for Outputs: {OUTPUT_DIR}", flush=True)


# =========================
# GUI (single-file app)
# =========================

APP_TITLE = "gradSERU College Graph Generator"
BANNER_FILE = "UMN_Logo.png"
BANNER_HEIGHT = 72

class BannerFrame(tk.Frame):
    def __init__(self, master, height=BANNER_HEIGHT):
        super().__init__(master, bg="#7a0019", height=height)
        self.pack_propagate(False)
        self._img_ref = None
        self._load_banner(height)

    def _load_banner(self, height):
        p = Path(__file__).with_name(BANNER_FILE)
        if p.exists():
            try:
                if PIL_OK:
                    img = Image.open(p)
                    ratio = height / img.height
                    img = img.resize((int(img.width * ratio), height), Image.LANCZOS)
                    photo = ImageTk.PhotoImage(img)
                else:
                    photo = tk.PhotoImage(file=str(p))
                self._img_ref = photo
                lbl = tk.Label(self, image=photo, bg="#7a0019")
                lbl.image = photo
                lbl.pack(fill="both", expand=True)
                return
            except Exception:
                pass
        tk.Label(
            self,
            text="UNIVERSITY OF MINNESOTA\nDriven to Discover",
            fg="white",
            bg="#7a0019",
            font=("Georgia", 14, "bold"),
        ).pack(fill="both", expand=True)

class IntroPage(ttk.Frame):
    def __init__(self, master, app, on_start):
        super().__init__(master)
        self.app = app
        self.on_start = on_start
        self._build()

    def _build(self):
        BannerFrame(self, height=BANNER_HEIGHT).pack(fill="x")
        outer = ttk.Frame(self); outer.pack(fill="both", expand=True)

        self.canvas = tk.Canvas(outer, highlightthickness=0)
        vbar = ttk.Scrollbar(outer, orient="vertical", command=self.canvas.yview)
        self.canvas.configure(yscrollcommand=vbar.set)
        vbar.pack(side="right", fill="y")
        self.canvas.pack(side="left", fill="both", expand=True)

        self.inner = ttk.Frame(self.canvas)
        self.canvas_window = self.canvas.create_window((0, 0), window=self.inner, anchor="nw")
        self.inner.bind("<Configure>", lambda e: self.canvas.configure(scrollregion=self.canvas.bbox("all")))
        self.canvas.bind("<Configure>", self._sync_inner_width)

        pad = dict(padx=24)
        ttk.Label(self.inner, text="gradSERU Analysis", font=("Segoe UI", 20, "bold")).pack(anchor="w", pady=(8,10), **pad)

        intro = (
            "This tool aggregates questions from the gradSERU Survey and organizes them under "
            "the newly defined MAP categories. It then generates a priority score for each "
            "category across departments or colleges provided as input. These scores highlight "
            "the categories and aspects with opportunities for development."
        )
        self.lbl_intro = ttk.Label(self.inner, text=intro, font=("Segoe UI", 12), justify="left")
        self.lbl_intro.pack(anchor="w", **pad)

        ttk.Label(self.inner, text="Instructions on Running the Application", font=("Segoe UI", 16, "bold")).pack(anchor="w", pady=(10, 0), **pad)
        ttk.Label(self.inner, text="Input Needed -", font=("Segoe UI", 14, "bold")).pack(anchor="w", pady=(5, 5), **pad)
        how_to_run = (
            "You need to provide two inputs:\n"
            "1. MAP file – A mapping of gradSERU survey questions to the categories you want each question to fall under.\n"
            "2. Data Source Folder – A folder that contains collection of folders each for each program or college you want results for. Each folder should "
            "contain the Excel sheets that are downloaded from the gradSERU datasource with all questions that are in the map file along with the student's agreement scores."
        )
        self.lbl_how = ttk.Label(self.inner, text=how_to_run, font=("Segoe UI", 12), justify="left")
        self.lbl_how.pack(anchor="w", **pad)

        ttk.Label(self.inner, text="Rules -", font=("Segoe UI", 14, "bold")).pack(anchor="w", pady=(5, 5), **pad)
        rules = ("The questions in the MAP file must exactly match the formatting and case (case-sensitive) of the questions in the gradSERU Survey.")
        self.lbl_rules = ttk.Label(self.inner, text=rules, font=("Segoe UI", 12), justify="left")
        self.lbl_rules.pack(anchor="w", **pad)

        ttk.Label(self.inner, text="Outputs -", font=("Segoe UI", 14, "bold")).pack(anchor="w", pady=(5, 5), **pad)
        outputs = (
            "1. Categories Folder – Collection of Excel files for each program/college showing the score for each category ranging from 0 to 1. Where 1 means high priority and 0 means low priority.\n"
            "2. Graphs Folder – Collection of images visually representing the category scores.\n"
            "3. Details Folder – Collection of Excel files with breakdown of each category with all the questions under that category for each program/college.\n"
            "4. Questions Breakdown Excel File – A report showing which questions are included or missing under each category for each program/college."
        )
        self.lbl_outputs = ttk.Label(self.inner, text=outputs, font=("Segoe UI", 12), justify="left")
        self.lbl_outputs.pack(anchor="w", **pad)

        ttk.Separator(self.inner).pack(fill="x", pady=16, padx=24)
        ttk.Button(self.inner, text="Start", command=self.on_start).pack(anchor="w", padx=24, pady=(0,16))

        self._wrap_labels = [self.lbl_intro, self.lbl_how , self.lbl_rules, self.lbl_outputs]

    def _sync_inner_width(self, event):
        canvas_width = event.width
        self.canvas.itemconfigure(self.canvas_window, width=canvas_width)
        effective = max(canvas_width - 48, 200)
        for lbl in getattr(self, "_wrap_labels", []):
            lbl.configure(wraplength=effective)

class RunnerPage(ttk.Frame):
    def __init__(self, master, app, on_back):
        super().__init__(master)
        self.app = app
        self.on_back = on_back
        self.colleges_root_var = tk.StringVar()
        self.map_path_var = tk.StringVar()
        self.output_dir_var = tk.StringVar()
        self._build()
        self._wire()

    def _build(self):
        BannerFrame(self, height=60).pack(fill="x")
        main = ttk.Frame(self, padding=16); main.pack(fill="both", expand=True); main.columnconfigure(0, weight=1)

        ttk.Label(main, text="Run Process to Generate Graphs", font=("Segoe UI", 16, "bold")).grid(row=0, column=0, sticky="w", pady=(0,8))

        # Root (title + subtitle)
        ttk.Label(main, text="Path to Folder Containing gradSERU Data", font=("Segoe UI", 14, "bold")).grid(row=1, column=0, sticky="w")
        self.lbl_root_sub = ttk.Label(
            main,
            text="This is the folder that has the Excel sheets downloaded from the gradSERU data source.",
            font=("Segoe UI", 12),
            justify="left",
            wraplength=10,
        )
        self.lbl_root_sub.grid(row=2, column=0, sticky="we", pady=(2,4))

        r1 = ttk.Frame(main); r1.grid(row=3, column=0, sticky="ew", pady=(2,8)); r1.columnconfigure(0, weight=1)
        ttk.Entry(r1, textvariable=self.colleges_root_var).grid(row=0, column=0, sticky="ew")
        ttk.Button(r1, text="Browse…", command=self._pick_root).grid(row=0, column=1, padx=(8,0))

        # Map (title + subtitle)
        ttk.Label(main, text="Map Excel file:", font=("Segoe UI", 14, "bold")).grid(row=4, column=0, sticky="w", pady=(0,0))
        self.lbl_map_sub = ttk.Label(
            main,
            text="This is the Map which you have created for analysis. (Kindly refer the Sample_MAP.xlsx for reference)",
            font=("Segoe UI", 12),
            justify="left",
            wraplength=10,
        )
        self.lbl_map_sub.grid(row=5, column=0, sticky="we", pady=(2,4))

        r2 = ttk.Frame(main); r2.grid(row=6, column=0, sticky="ew", pady=(2,8)); r2.columnconfigure(0, weight=1)
        ttk.Entry(r2, textvariable=self.map_path_var).grid(row=0, column=0, sticky="ew")
        ttk.Button(r2, text="Browse…", command=self._pick_map).grid(row=0, column=1, padx=(8,0))

        ttk.Label(main, text="Output folder (auto):", font=("Segoe UI", 12, "bold")).grid(row=7, column=0, sticky="w")
        r3 = ttk.Frame(main); r3.grid(row=8, column=0, sticky="ew", pady=(2,12)); r3.columnconfigure(0, weight=1)
        ttk.Entry(r3, textvariable=self.output_dir_var, state="readonly").grid(row=0, column=0, sticky="ew")

        btn_row = ttk.Frame(main); btn_row.grid(row=9, column=0, sticky="w", pady=(0,8))
        self.run_btn = ttk.Button(btn_row, text="Run Graph Generation", command=self._run_clicked)
        self.run_btn.pack(side="left")
        ttk.Button(btn_row, text="Back", command=self.on_back).pack(side="left", padx=(12,0))

        ttk.Label(main, text="Status & Results:", font=("Segoe UI", 12, "bold")).grid(row=10, column=0, sticky="w")
        self.log = tk.Text(main, height=16, wrap="word"); self.log.grid(row=11, column=0, sticky="nsew")
        main.rowconfigure(11, weight=1); main.columnconfigure(0, weight=1)
        self.log.configure(state="disabled")

        self._runner_wrap_labels = [self.lbl_root_sub, self.lbl_map_sub]
        self.bind("<Configure>", self._sync_runner_wrap)

    def _sync_runner_wrap(self, event):
        effective = max(self.winfo_width() - 48, 200)
        for lbl in getattr(self, "_runner_wrap_labels", []):
            lbl.configure(wraplength=effective)

    def _wire(self):
        self.colleges_root_var.trace_add("write", lambda *_: self._update_output())

    def _update_output(self):
        root = self.colleges_root_var.get().strip()
        out = str(Path(root).with_name(Path(root).name + "_Output")) if root else ""
        self.output_dir_var.set(out)

    def _pick_root(self):
        d = filedialog.askdirectory(title="Select root folder")
        if d: self.colleges_root_var.set(d)

    def _pick_map(self):
        f = filedialog.askopenfilename(
            title="Select gradSERU map Excel (.xlsx only)",
            filetypes=[("Excel (.xlsx)", "*.xlsx")],
            defaultextension=".xlsx"
        )
        if f:
            if Path(f).suffix.lower() != ".xlsx":
                messagebox.showerror("Invalid file", "Please select a .xlsx Excel file.")
                return
            self.map_path_var.set(f)

    def _append_log(self, text):
        self.log.configure(state="normal"); self.log.insert("end", text)
        self.log.see("end"); self.log.configure(state="disabled")

    # Run generator in a background thread, capturing stdout/stderr into the GUI log.
    def _run_clicked(self):
        root = self.colleges_root_var.get().strip()
        mapp = self.map_path_var.get().strip()
        if not root or not Path(root).is_dir():
            messagebox.showerror("Invalid", "Select a valid gradSERU data folder"); return
        if not mapp or not Path(mapp).is_file():
            messagebox.showerror("Invalid", "Select a valid map Excel file"); return
        if Path(mapp).suffix.lower() != ".xlsx":
            messagebox.showerror("Invalid", "Map must be a .xlsx Excel file."); return

        outdir = str(Path(root).with_name(Path(root).name + "_Output"))
        self._append_log(f"Running...\n  Data: {root}\n  MAP:  {mapp}\n  Out:  {outdir}\n\n")
        self.run_btn.config(state="disabled")
        t = threading.Thread(target=self._run_generator_thread, args=(mapp, root, outdir), daemon=True)
        t.start()

    def _run_generator_thread(self, mapp, root, outdir):
        class Tee:
            def __init__(self, cb):
                self.cb = cb
                self._orig = sys.__stdout__
            def write(self, s):
                try:
                    self._orig.write(s)
                except Exception:
                    pass
                # Send safe text to GUI
                self.cb(s)
            def flush(self):
                try:
                    self._orig.flush()
                except Exception:
                    pass

        tee = Tee(self._append_log)
        try:
            with redirect_stdout(tee), redirect_stderr(tee):
                run_generation(Path(mapp), Path(root), Path(outdir))
            self._append_log("\nProgram Ran Successfully.\n")
        except Exception as e:
            self._append_log(f"\nERROR: {e}\n")
        finally:
            self.run_btn.config(state="normal")

class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title(APP_TITLE); self.geometry("1000x650")
        try:
            ttk.Style(self).theme_use("clam")
        except:
            pass
        self.container = ttk.Frame(self); self.container.pack(fill="both", expand=True)
        self.show_intro()

    def _clear(self):
        for w in self.container.winfo_children():
            w.destroy()

    def show_intro(self):
        self._clear()
        IntroPage(self.container, self, on_start=self.show_runner).pack(fill="both", expand=True)

    def show_runner(self):
        self._clear()
        RunnerPage(self.container, self, on_back=self.show_intro).pack(fill="both", expand=True)

if __name__ == "__main__":
    App().mainloop()
