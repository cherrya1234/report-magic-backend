import os
import json
from uuid import uuid4
from typing import Optional, Dict, Any, List
import boto3
import openai
import pandas as pd
from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fpdf import FPDF

# =========================
# Config
# =========================
DEBUG_PLANNER = True          # print plan + columns + execution status in logs
PLANNER_RETRIES = 1           # retry once if invalid JSON
MAX_PREVIEW_ROWS = 50

# =========================
# App & CORS (allow all origins, no credentials)
# =========================
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],          # broad: works for vercel preview domains too
    allow_credentials=False,      # must be False with "*"
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
    expose_headers=["Content-Disposition"],
    max_age=86400,
)

# =========================
# Env/Clients
# =========================
BUCKET_NAME = os.getenv("S3_BUCKET", "report-magician-files")

s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-1",
)

openai.api_key = os.getenv("OPENAI_API_KEY")

# In-memory store (for demo/memory mode)
# session_data[session_id] = {"email": str, "project": str, "questions": list, "files": [s3_keys]}
session_data: Dict[str, Dict[str, Any]] = {}

# =========================
# Helpers: normalization & loading
# =========================
def _snake(s: str) -> str:
    s = re.sub(r"[^\w]+", "_", s.strip().lower())
    s = re.sub(r"_+", "_", s).strip("_")
    return s

LIKELY_DATE_NAMES = {
    "move_in", "move_in_date", "movein", "start_date",
    "move_out", "move_out_date", "moveout", "end_date",
    "date", "start", "end"
}

def _standardize_df(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize columns, parse likely dates, create unit_size, compute length_of_stay_days."""
    # 1) snake_case all columns
    original_cols = list(df.columns)
    df = df.rename(columns={c: _snake(str(c)) for c in original_cols})

    # 2) parse likely date columns
    for c in df.columns:
        if c in LIKELY_DATE_NAMES or "date" in c:
            try:
                df[c] = pd.to_datetime(df[c], errors="coerce")
            except Exception:
                pass

    # 3) unify unit_size if present under various headers
    for candidate in ["unit_size", "unitsize", "size", "unit_dimensions"]:
        if candidate in df.columns:
            df["unit_size"] = df[candidate].astype(str).str.replace(r"\s+", "", regex=True)
            break

    # 4) compute length_of_stay_days if we have move-in/out (or start/end)
    move_in_candidates  = [c for c in df.columns if c in {"move_in", "move_in_date", "movein", "start_date", "start"}]
    move_out_candidates = [c for c in df.columns if c in {"move_out", "move_out_date", "moveout", "end_date", "end"}]
    mi = move_in_candidates[0] if move_in_candidates else None
    mo = move_out_candidates[0] if move_out_candidates else None
    if mi and mo:
        today = pd.Timestamp.utcnow().normalize()
        out = df[mo].fillna(today)
        try:
            df["length_of_stay_days"] = (out - df[mi]).dt.days
        except Exception:
            df["length_of_stay_days"] = pd.NA

    return df

def _load_df_from_s3_key(s3_client, bucket: str, key: str) -> pd.DataFrame:
    buf = io.BytesIO()
    s3_client.download_fileobj(bucket, key, buf)
    buf.seek(0)
    df = pd.read_excel(buf)  # openpyxl required
    df = _standardize_df(df)
    return df

def _load_merged_session_df(s3_client, bucket: str, sess: dict) -> pd.DataFrame:
    keys = sess.get("files", [])
    dfs = []
    for k in keys:
        try:
            df = _load_df_from_s3_key(s3, BUCKET_NAME, k)
            df["__source_file__"] = k
            dfs.append(df)
        except Exception as e:
            print("[ask] failed to load", k, repr(e), file=sys.stderr)
    if not dfs:
        return pd.DataFrame()
    merged = pd.concat(dfs, ignore_index=True, sort=True)
    return merged

def _df_sample_csv(df: pd.DataFrame, n=20) -> str:
    if len(df) == 0:
        return ""
    return df.sample(min(n, len(df)), random_state=42).to_csv(index=False)

def _df_profile(df: pd.DataFrame) -> str:
    parts = []
    parts.append("SCHEMA:")
    for col in df.columns:
        try:
            nulls = int(df[col].isna().sum())
        except Exception:
            nulls = 0
        parts.append(f"- {col}: dtype={str(df[col].dtype)}, nulls={nulls}")
    num = df.select_dtypes(include=["number"])
    if not num.empty:
        parts.append("
NUMERIC_SUMMARY=" + str(num.describe().round(3).to_dict()))
    cat = df.select_dtypes(exclude=["number"])
    if not cat.empty:
        cats = {}
        for c in cat.columns[:8]:
            cats[c] = cat[c].astype(str).value_counts().head(5).to_dict()
        parts.append("
CATEGORICAL_TOP_VALUES=" + str(cats))
    return "
".join(parts)

# =========================
# Plan execution
# =========================
def _apply_filters(df: pd.DataFrame, filters: List[Dict[str, Any]]) -> pd.DataFrame:
    out = df.copy()
    for f in filters or []:
        col = f.get("column")
        op  = f.get("op")
        val = f.get("value")
        if not col or col not in out.columns:
            continue
        series = out[col]
        if op in ("eq", "=="):
            out = out[series.astype(str).str.lower() == str(val).lower()]
        elif op in ("ne", "!="):
            out = out[series.astype(str).str.lower() != str(val).lower()]
        elif op in (">", "gt"):
            out = out[pd.to_numeric(series, errors="coerce") > pd.to_numeric(val, errors="coerce")]
        elif op in ("<", "lt"):
            out = out[pd.to_numeric(series, errors="coerce") < pd.to_numeric(val, errors="coerce")]
        elif op in ("contains", "icontains"):
            out = out[series.astype(str).str.contains(str(val), case=False, na=False)]
        elif op in ("between",):
            lo = f.get("low"); hi = f.get("high")
            s = pd.to_numeric(series, errors="coerce")
            out = out[(s >= pd.to_numeric(lo, errors="coerce")) & (s <= pd.to_numeric(hi, errors="coerce"))]
        elif op in ("date_between",):
            s = pd.to_datetime(series, errors="coerce")
            start = pd.to_datetime(f.get("start"), errors="coerce")
            end   = pd.to_datetime(f.get("end"),   errors="coerce")
            out = out[(s >= start) & (s <= end)]
    return out

def _execute_plan(df: pd.DataFrame, plan: Dict[str, Any]) -> Tuple[str, Optional[pd.DataFrame]]:
    task = plan.get("task", "aggregate").lower()
    filters = plan.get("filters", [])
    gby = plan.get("groupby", [])
    metrics = plan.get("metrics", [])
    limit = int(plan.get("limit", 50))
    sort = plan.get("sort", [])

    # Apply filters
    work = _apply_filters(df, filters)

    # Process the aggregation if required
    if task == "aggregate":
        agg_map = {}
        for metric in metrics:
            agg_type = metric.get("agg", "mean")
            col = metric.get("column")
            agg_map[col] = agg_type

        # Perform aggregation
        work = work.groupby(gby).agg(agg_map).reset_index()

    # Apply sorting if requested
    if sort:
        for s in sort:
            work = work.sort_values(by=s["column"], ascending=s.get("direction", "asc") == "asc")

    # Limit the number of rows returned
    work = work.head(limit)

    return f"Processed {task} task.", work

# =========================
# Routes
# =========================
@app.get("/")
def health():
    return {"ok": True, "service": "Report Magician backend (debug planner + normalization)"}

@app.post("/api/upload")
async def upload_excel(
    projectName: str = Form(...),
    email: str = Form(...),
    file: UploadFile = File(...),
    session_id: Optional[str] = Form(None),
):
    try:
        if not session_id:
            session_id = str(uuid4())

        unique_filename = f"{int(uuid4().int % 1e10)}_{file.filename}"
        s3_key = f"projects/{projectName}/{session_id}/uploads/{unique_filename}"

        file.file.seek(0)
        s3.upload_fileobj(file.file, BUCKET_NAME, s3_key)
        s3.head_object(Bucket=BUCKET_NAME, Key=s3_key)  # verify upload

        sess = session_data.setdefault(session_id, {
            "email": email,
            "project": projectName,
            "questions": [],
            "files": []
        })
        sess["email"] = email or sess.get("email")
        sess["project"] = projectName or sess.get("project")
        sess["files"].append(s3_key)

        print(f"[upload] session={session_id} s3_key={s3_key}")
        return {"session_id": session_id, "s3_key": s3_key}
    except Exception as e:
        print("Upload error:", repr(e), file=sys.stderr)
        raise HTTPException(status_code=500, detail="Upload failed.")

@app.post("/api/ask")
async def ask_question(request: Request):
    try:
        data = await request.json()
        session_id = data.get("session_id")
        user_q = data.get("prompt")

        if not session_id or not user_q:
            raise HTTPException(status_code=400, detail="Missing session_id or prompt.")

        sess = session_data.get(session_id)
        if not sess or not sess.get("files"):
            raise HTTPException(status_code=400, detail="No files found for this session.")

        df = _load_merged_session_df(s3, BUCKET_NAME, sess)
        if df.empty:
            raise HTTPException(status_code=400, detail="Could not load any data from uploaded files.")

        # Alias map (exclude helper column)
        cols = [c for c in df.columns if c != "__source_file__"]
        alias_map = _column_alias_map(cols)

        # Planner (retry logic)
        def plan_once() -> Optional[dict]:
            planner_system = (
                "You are a senior data analyst. Convert the user's question into a SMALL JSON plan "
                "that can be executed with pandas. Use only these keys: "
                "task, filters, groupby, metrics, limit, sort, k, rank_by. "
                "Allowed tasks: aggregate, list_rows, topk. "
                "Allowed aggs: count, sum, mean, avg, median, min, max, nunique. "
                "Filters are case-insensitive for text. If question is descriptive, return task=list_rows. "
                "IMPORTANT: Return ONLY valid JSON, no backticks, no commentary."
            )
            planner_user = (
                f"AVAILABLE_COLUMNS = {list(alias_map.values())}

"
                "QUESTION = {user_q}

"
                "Example output:
"
                "{
"
                '  "task": "aggregate",
'
                '  "filters": [{"column":"unit_size","op":"eq","value":"10x10"}],
'
                '  "metrics": [{"agg":"mean","column":"length_of_stay_days","alias":"avg_stay_days"}],
'
                '  "limit": 50
'
                "}"
            )
            resp = openai.ChatCompletion.create(
                model="gpt-3.5-turbo",
                temperature=0.0,
                messages=[
                    {"role": "system", "content": planner_system},
                    {"role": "user", "content": planner_user},
                ],
            )
            text = resp["choices"][0]["message"]["content"].strip()
            try:
                return json.loads(text)
            except Exception:
                return None

        plan = plan_once()
        if plan is None and PLANNER_RETRIES > 0:
            plan = plan_once()

        used_fallback = False
        table = None
        summary_msg = None
        if plan:
            plan = _remap_plan_columns(plan, alias_map)
            try:
                summary_msg, table = _execute_plan(df, plan)
            except Exception:
                used_fallback = True
        else:
            used_fallback = True

        if table is not None and not table.empty:
            preview_rows = min(len(table), MAX_PREVIEW_ROWS)
            csv_preview = table.head(preview_rows).to_csv(index=False)
            answer = summary_msg or "Computed result."
            full_answer = f"{answer}

Preview (first {preview_rows} rows):

{csv_preview}"
            return {"answer": full_answer}

        # Fallback answer
        profile = _df_profile(df)
        sample_csv = _df_sample_csv(df, n=20)
        system_msg = "You are a precise data analyst. Use ONLY the provided data profile and sample rows."
        user_ctx = f"DATA PROFILE:
{profile}

SAMPLE ROWS (CSV, up to 20):
{sample_csv}

QUESTION: {user_q}"
        resp = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            temperature=0.2,
            messages=[{"role": "system", "content": system_msg}, {"role": "user", "content": user_ctx}],
        )
        answer = resp["choices"][0]["message"]["content"]
        return {"answer": answer}
