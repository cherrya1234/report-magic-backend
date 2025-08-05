import os, io, json, sys
from uuid import uuid4

import boto3
import openai  # legacy SDK 0.28.1 (pinned in requirements)
import pandas as pd
from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fpdf import FPDF

# =========================
# Config
# =========================
DEBUG_PLANNER = True          # print plan + columns
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
session_data = {}

# =========================
# Helpers: load & merge
# =========================
def _load_df_from_s3_key(s3_client, bucket: str, key: str) -> pd.DataFrame:
    buf = io.BytesIO()
    s3_client.download_fileobj(bucket, key, buf)
    buf.seek(0)
    # Use openpyxl engine implicitly for .xlsx
    return pd.read_excel(buf)

def _load_merged_session_df(s3_client, bucket: str, sess: dict) -> pd.DataFrame:
    keys = sess.get("files", [])
    dfs = []
    for k in keys:
        try:
            df = _load_df_from_s3_key(s3_client, bucket, k)
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
        parts.append("\nNUMERIC_SUMMARY=" + str(num.describe().round(3).to_dict()))
    cat = df.select_dtypes(exclude=["number"])
    if not cat.empty:
        cats = {}
        for c in cat.columns[:8]:
            cats[c] = cat[c].astype(str).value_counts().head(5).to_dict()
        parts.append("\nCATEGORICAL_TOP_VALUES=" + str(cats))
    return "\n".join(parts)

# =========================
# Column normalization / matching
# =========================
def _normalize(name: str) -> str:
    return "".join(ch for ch in name.strip().lower() if ch.isalnum() or ch == "_")

def _column_alias_map(columns: list[str]) -> dict:
    """
    Builds a map normalized_name -> actual_column for fuzzy matching
    e.g., "Unit Size" -> "unitsize" and also "unit_size" -> "unitsize".
    """
    aliases = {}
    for c in columns:
        aliases[_normalize(c)] = c
        aliases[_normalize(c.replace(" ", "_"))] = c
    return aliases

def _remap_plan_columns(plan: dict, alias_map: dict) -> dict:
    """Remap plan filters/groupby/metrics column names via alias_map if needed."""
    def map_col(name: str | None) -> str | None:
        if not name:
            return name
        key = _normalize(name)
        return alias_map.get(key, name)

    out = json.loads(json.dumps(plan))  # deep copy
    # filters
    for f in out.get("filters", []) or []:
        f["column"] = map_col(f.get("column"))
    # groupby
    if isinstance(out.get("groupby"), list):
        out["groupby"] = [map_col(g) for g in out["groupby"]]
    # metrics
    for m in out.get("metrics", []) or []:
        m["column"] = map_col(m.get("column"))
    # sort
    for s in out.get("sort", []) or []:
        s["column"] = map_col(s.get("column"))
    # rank_by
    if out.get("rank_by"):
        out["rank_by"] = map_col(out["rank_by"])
    return out

# =========================
# Safe pandas executor (tiny plan)
# =========================
ALLOWED_AGGS = {"count", "sum", "mean", "avg", "median", "min", "max", "nunique"}
RENAME_AGG = {"avg": "mean"}

def _apply_filters(df: pd.DataFrame, filters: list[dict]) -> pd.DataFrame:
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

def _execute_plan(df: pd.DataFrame, plan: dict) -> tuple[str, pd.DataFrame | None]:
    task = (plan.get("task") or "aggregate").lower()
    filters = plan.get("filters") or []
    gby = plan.get("groupby") or []
    metrics = plan.get("metrics") or []
    limit = int(plan.get("limit") or 50)
    sort  = plan.get("sort") or []

    work = _apply_filters(df, filters)

    for m in metrics:
        col = m.get("column")
        if col and col in work.columns:
            work[col] = pd.to_numeric(work[col], errors="ignore")

    if task in ("list_rows", "rows"):
        out = work[gby or work.columns].head(limit)
        return (f"Showing up to {len(out)} row(s).", out)

    if task in ("aggregate", "groupby", "summarize", "summary"):
        if gby:
            gb = work.groupby(gby, dropna=False)
        else:
            gb = None

        agg_map = {}
        for m in metrics:
            agg = (m.get("agg") or "").lower()
            agg = RENAME_AGG.get(agg, agg)
            col = m.get("column")
            if agg not in ALLOWED_AGGS:
                continue
            if agg == "count" and (not col or col == "*"):
                if gb is None:
                    out = pd.DataFrame({"count": [len(work)]})
                else:
                    out = gb.size().reset_index(name="count")
                if sort:
                    for s in sort:
                        out = out.sort_values(by=s["column"], ascending=(s.get("direction","asc")=="asc"))
                return ("Computed counts.", out.head(limit))
            if col and col in work.columns:
                agg_map.setdefault(col, []).append(agg)

        if not agg_map:
            if gb is None:
                out = pd.DataFrame({"count": [len(work)]})
            else:
                out = gb.size().reset_index(name="count")
            if sort:
                for s in sort:
                    out = out.sort_values(by=s["column"], ascending=(s.get("direction","asc")=="asc"))
            return ("Computed counts.", out.head(limit))

        if gb is None:
            out = work.agg(agg_map)
            if isinstance(out, pd.Series):
                out = out.to_frame().T
            else:
                out.columns = ["_".join(c).strip() if isinstance(c, tuple) else c for c in out.columns]
            msg = "Computed summary metrics."
        else:
            out = gb.agg(agg_map).reset_index()
            out.columns = ["_".join(c).strip() if isinstance(c, tuple) else c for c in out.columns]
            msg = "Computed grouped summary metrics."
        if sort:
            for s in sort:
                out = out.sort_values(by=s["column"], ascending=(s.get("direction","asc")=="asc"))
        return (msg, out.head(limit))

    if task in ("topk", "rank"):
        k = int(plan.get("k") or 10)
        rank_by = plan.get("rank_by")
        if rank_by and rank_by in work.columns:
            out = work.sort_values(by=rank_by, ascending=False).head(k)
            return (f"Top {k} by {rank_by}.", out)
        return ("Ranking failed: missing rank_by column.", None)

    out = work.head(limit)
    return (f"Showing up to {len(out)} row(s).", out)

# =========================
# Routes
# =========================
@app.get("/")
def health():
    return {"ok": True, "service": "Report Magician backend (debug planner)"}

@app.post("/api/upload")
async def upload_excel(
    projectName: str = Form(...),
    email: str = Form(...),
    file: UploadFile = File(...),
    session_id: str | None = Form(None),
):
    try:
        if not session_id:
            session_id = str(uuid4())

        unique_filename = f"{int(uuid4().int % 1e10)}_{file.filename}"
        s3_key = f"projects/{projectName}/{session_id}/uploads/{unique_filename}"

        file.file.seek(0)
        s3.upload_fileobj(file.file, BUCKET_NAME, s3_key)
        s3.head_object(Bucket=BUCKET_NAME, Key=s3_key)  # verify

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

        # Debug: show shape + few rows
        print(f"[ask] session={session_id} merged shape={df.shape}", file=sys.stderr)
        print("[ask] head:\n" + df.head(5).to_string(index=False), file=sys.stderr)

        # Column normalization / aliasing
        cols = list(df.columns)
        alias_map = _column_alias_map([c for c in cols if c != "__source_file__"])

        if DEBUG_PLANNER:
            print("[planner] available columns:", cols, file=sys.stderr)

        # === 1) Try to get a JSON plan from the model (with retry) ===
        def plan_once() -> dict | None:
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
                f"AVAILABLE_COLUMNS = {list(alias_map.values())}\n\n"
                f"QUESTION = {user_q}\n\n"
                "Example output:\n"
                "{\n"
                '  "task": "aggregate",\n'
                '  "filters": [{"column":"Unit Size","op":"eq","value":"10x10"}],\n'
                '  "groupby": ["City"],\n'
                '  "metrics": [{"agg":"mean","column":"Rent","alias":"avg_rent"}],\n'
                '  "limit": 50,\n'
                '  "sort": [{"column":"avg_rent","direction":"desc"}]\n'
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
            if DEBUG_PLANNER:
                print("[planner] raw plan text:", text, file=sys.stderr)
            try:
                return json.loads(text)
            except Exception:
                return None

        plan = plan_once()
        if plan is None and PLANNER_RETRIES > 0:
            if DEBUG_PLANNER:
                print("[planner] retrying once due to invalid JSON", file=sys.stderr)
            plan = plan_once()

        used_fallback = False
        table = None
        summary_msg = None
        if plan:
            # Remap column names to real ones (normalize)
            plan = _remap_plan_columns(plan, alias_map)
            if DEBUG_PLANNER:
                print("[planner] final plan:", json.dumps(plan, indent=2), file=sys.stderr)
            try:
                summary_msg, table = _execute_plan(df, plan)
            except Exception as e:
                print("[planner] execute failed:", repr(e), file=sys.stderr)
                used_fallback = True
        else:
            used_fallback = True

        # === 2) If we got a table, answer with preview ===
        if (table is not None) and (not table.empty):
            preview_rows = min(len(table), MAX_PREVIEW_ROWS)
            csv_preview = table.head(preview_rows).to_csv(index=False)
            answer = summary_msg or "Computed result."
            full_answer = f"{answer}\n\nPreview (first {preview_rows} rows):\n```\n{csv_preview}\n```"
            sess.setdefault("questions", []).append({"question": user_q, "answer": full_answer})
            print(f"[ask] plan_used={not used_fallback} rows={len(table)}", file=sys.stderr)
            return {"answer": full_answer}

        # === 3) Fallback analyst answer grounded by profile + sample ===
        profile = _df_profile(df)
        sample_csv = _df_sample_csv(df, n=20)
        system_msg = (
            "You are a precise data analyst. Use ONLY the provided data profile and sample rows. "
            "If data is insufficient, say exactly which columns or steps are needed."
        )
        user_ctx = (
            f"DATA PROFILE:\n{profile}\n\n"
            f"SAMPLE ROWS (CSV, up to 20):\n{sample_csv}\n\n"
            f"QUESTION (verbatim from user):\n{user_q}"
        )
        resp = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            temperature=0.2,
            messages=[
                {"role": "system", "content": system_msg},
                {"role": "user", "content": user_ctx},
            ],
        )
        answer = resp["choices"][0]["message"]["content"]
        sess.setdefault("questions", []).append({"question": user_q, "answer": answer})
        print(f"[ask] plan_used=False (fallback).", file=sys.stderr)
        return {"answer": answer}

    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"ask failed: {type(e).__name__}: {e}")

@app.get("/api/export")
async def export_pdf(session_id: str):
    sess = session_data.get(session_id)
    if not sess:
        raise HTTPException(status_code=400, detail="Invalid session_id")

    pdf = FPDF()
    pdf.add_page()
    pdf.set_auto_page_break(auto=True, margin=12)

    pdf.set_font("Arial", "B", 14)
    pdf.cell(0, 10, "Report Magician — Q&A Summary", ln=True)
    pdf.set_font("Arial", size=11)
    pdf.cell(0, 8, f"Project: {sess.get('project','')}", ln=True)
    pdf.cell(0, 8, f"Email: {sess.get('email','')}", ln=True)
    pdf.ln(4)

    qa_list = sess.get("questions", [])
    if not qa_list:
        pdf.set_font("Arial", size=12)
        pdf.multi_cell(0, 8, "No questions asked in this session.")
    else:
        pdf.set_font("Arial", "B", 12)
        pdf.cell(0, 10, "Q&A", ln=True)
        pdf.set_font("Arial", size=12)
        for i, qa in enumerate(qa_list, 1):
            q = qa.get("question", "")
            a = qa.get("answer", "")
            pdf.multi_cell(0, 8, f"{i}. Q: {q}")
            pdf.multi_cell(0, 8, f"   A: {a}")
            pdf.ln(2)

    fname = f"report_{session_id}.pdf"
    pdf.output(fname)
    return FileResponse(fname, media_type="application/pdf", filename=fname)
