
import os
import io, json
from uuid import uuid4

import boto3
import openai  # legacy SDK 0.28.1
import pandas as pd
from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fpdf import FPDF

# -------- App & CORS --------
app = FastAPI()

ALLOWED_ORIGINS = [
    "https://report-magician-frontend.vercel.app",
    "http://localhost:5173",
    "http://localhost:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------- Env / Clients --------
BUCKET_NAME = "report-magician-files"

s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-1",
)

openai.api_key = os.getenv("OPENAI_API_KEY")

# In-memory session store
session_data = {}

# -------- Utilities --------
def _load_df_from_s3_key(s3_client, bucket: str, key: str) -> pd.DataFrame:
    buf = io.BytesIO()
    s3_client.download_fileobj(bucket, key, buf)
    buf.seek(0)
    return pd.read_excel(buf)  # openpyxl required

def _load_merged_session_df(s3_client, bucket: str, sess: dict) -> pd.DataFrame:
    keys = sess.get("files", [])
    dfs = []
    for k in keys:
        try:
            df = _load_df_from_s3_key(s3_client, bucket, k)
            df["__source_file__"] = k
            dfs.append(df)
        except Exception as e:
            print("[ask] failed to load", k, e)
    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True, sort=True)

def _df_sample_csv(df: pd.DataFrame, n=20) -> str:
    if len(df) == 0:
        return ""
    return df.sample(min(n, len(df)), random_state=42).to_csv(index=False)

def _df_profile(df: pd.DataFrame) -> str:
    parts = []
    parts.append("SCHEMA:")
    for col in df.columns:
        parts.append(f"- {col}: dtype={str(df[col].dtype)}, nulls={int(df[col].isna().sum())}")
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

# ---- safe pandas executor over a very small plan ----
ALLOWED_AGGS = {"count", "sum", "mean", "avg", "median", "min", "max", "nunique"}
RENAME_AGG = {"avg": "mean"}

def _apply_filters(df: pd.DataFrame, filters: list[dict]) -> pd.DataFrame:
    out = df.copy()
    for f in filters or []:
        col = f.get("column")
        op  = f.get("op")
        val = f.get("value")
        if col not in out.columns:
            continue
        series = out[col]
        # simple ops only
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
        # ignore unsupported ops silently
    return out

def _execute_plan(df: pd.DataFrame, plan: dict) -> tuple[str, pd.DataFrame | None]:
    """
    Plan schema (example):
    {
      "task": "aggregate",  # or "list_rows", "topk", "pivot"
      "filters": [{"column":"Unit Size","op":"eq","value":"10x10"}],
      "groupby": ["City"],
      "metrics": [{"agg":"mean","column":"Rent","alias":"avg_rent"}],
      "limit": 50,
      "sort": [{"column":"avg_rent","direction":"desc"}]
    }
    """
    task = (plan.get("task") or "aggregate").lower()
    filters = plan.get("filters") or []
    gby = plan.get("groupby") or []
    metrics = plan.get("metrics") or []
    limit = int(plan.get("limit") or 50)
    sort  = plan.get("sort") or []

    work = _apply_filters(df, filters)

    # Ensure dtypes usable
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
                # count rows
                if gb is None:
                    out = pd.DataFrame({"count": [len(work)]})
                else:
                    out = gb.size().reset_index(name="count")
                # optional sort/limit
                if sort:
                    for s in sort:
                        out = out.sort_values(by=s["column"], ascending=(s.get("direction","asc")=="asc"))
                return ("Computed counts.", out.head(limit))
            if col and col in work.columns:
                agg_map.setdefault(col, []).append(agg)

        if not agg_map:
            # default to row count
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
            # flatten columns if multiindex
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

    # default: list a few rows
    out = work.head(limit)
    return (f"Showing up to {len(out)} row(s).", out)

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

        # 1) load merged data
        df = _load_merged_session_df(s3, BUCKET_NAME, sess)
        if df.empty:
            raise HTTPException(status_code=400, detail="Could not load any data from uploaded files.")

        # 2) LLM planner → produce a tiny JSON plan
        cols = list(df.columns)
        planner_system = (
            "You are a senior data analyst. Convert the user's question into a SMALL JSON plan "
            "that can be executed with pandas. Use only these keys: "
            "task, filters, groupby, metrics, limit, sort, k, rank_by. "
            "Allowed tasks: aggregate, list_rows, topk. "
            "Allowed aggs: count, sum, mean, avg, median, min, max, nunique. "
            "Filters are case-insensitive for text. If question is descriptive, return task=list_rows."
            "IMPORTANT: Return ONLY valid JSON, no backticks, no commentary."
        )
        planner_user = (
            f"AVAILABLE_COLUMNS = {cols}\n\n"
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
        plan_resp = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            temperature=0.0,
            messages=[
                {"role": "system", "content": planner_system},
                {"role": "user", "content": planner_user},
            ],
        )
        plan_text = plan_resp["choices"][0]["message"]["content"].strip()
        try:
            plan = json.loads(plan_text)
        except Exception:
            # If it returns text, fallback to descriptive answer later
            plan = {"task": "fallback"}

        # 3) Try to execute the plan safely
        table = None
        summary_msg = None
        if plan.get("task") != "fallback":
            summary_msg, table = _execute_plan(df, plan)

        # 4) If we got a table or a summary, craft a concise answer
        if table is not None:
            # Make a compact CSV for display-sized results
            preview_rows = min(len(table), 50)
            csv_preview = table.head(preview_rows).to_csv(index=False)
            answer = summary_msg or "Computed result."
            # store Q&A + attach tiny preview as code block
            full_answer = f"{answer}\n\nPreview (first {preview_rows} rows):\n```\n{csv_preview}\n```"
            sess.setdefault("questions", [])
            sess["questions"].append({"question": user_q, "answer": full_answer})
            return {"answer": full_answer}

        # 5) Fallback: grounded LLM answer using profile + sample
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
        sess.setdefault("questions", [])
        sess["questions"].append({"question": user_q, "answer": answer})
        return {"answer": answer}

    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"ask failed: {type(e).__name__}: {e}")
