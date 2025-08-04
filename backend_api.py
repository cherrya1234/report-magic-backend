
import os
import io
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
    return pd.read_excel(buf)  # requires openpyxl

def _load_merged_session_df(s3_client, bucket: str, sess: dict) -> pd.DataFrame:
    keys = sess.get("files", [])
    if not keys:
        return pd.DataFrame()
    dfs = []
    for k in keys:
        try:
            df = _load_df_from_s3_key(s3_client, bucket, k)
            df["__source_file__"] = k  # keep provenance
            dfs.append(df)
        except Exception as e:
            print("Failed to load", k, "error:", repr(e))
    if not dfs:
        return pd.DataFrame()
    # Outer concat to union columns across sheets
    merged = pd.concat(dfs, ignore_index=True, sort=True)
    return merged

def _df_sample_csv(df: pd.DataFrame, n=20) -> str:
    if len(df) == 0:
        return ""
    return df.sample(min(n, len(df)), random_state=42).to_csv(index=False)

def _df_profile(df: pd.DataFrame, max_cats_cols=8) -> str:
    parts = []
    parts.append("SCHEMA:")
    for col in df.columns:
        parts.append(f"- {col}: dtype={str(df[col].dtype)}, nulls={int(df[col].isna().sum())}")
    num = df.select_dtypes(include=["number"])
    if not num.empty:
        parts.append("\nNUMERIC_SUMMARY=" + str(num.describe().round(3).to_dict()))
    # brief categorical peek
    cat = df.select_dtypes(exclude=["number"])
    if not cat.empty:
        cats = {}
        for c in cat.columns[:max_cats_cols]:
            cats[c] = cat[c].astype(str).value_counts().head(5).to_dict()
        parts.append("\nCATEGORICAL_TOP_VALUES=" + str(cats))
    return "\n".join(parts)

# -------- Health --------
@app.get("/")
def health():
    return {"ok": True, "service": "Report Magician backend (merged files)"}

# -------- Upload (supports multiple files) --------
@app.post("/api/upload")
async def upload_excel(
    projectName: str = Form(...),
    email: str = Form(...),
    file: UploadFile = File(...),
    session_id: str | None = Form(None),
):
    try:
        # Reuse session if provided; otherwise create new
        if not session_id:
            session_id = str(uuid4())

        unique_filename = f"{int(uuid4().int % 1e10)}_{file.filename}"
        s3_key = f"projects/{projectName}/{session_id}/uploads/{unique_filename}"

        file.file.seek(0)
        s3.upload_fileobj(file.file, BUCKET_NAME, s3_key)

        sess = session_data.setdefault(session_id, {
            "email": email,
            "project": projectName,
            "questions": [],
            "files": []
        })
        # Update email/project if provided again
        sess["email"] = email or sess.get("email")
        sess["project"] = projectName or sess.get("project")

        sess["files"].append(s3_key)

        return {"session_id": session_id, "s3_key": s3_key}
    except Exception as e:
        print("Upload error:", repr(e))
        raise HTTPException(status_code=500, detail="Upload failed.")

# -------- Ask (rewrite to analyst brief, then grounded answer) --------
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

        # Load and merge all session files
        df = _load_merged_session_df(s3, BUCKET_NAME, sess)
        if df.empty:
            raise HTTPException(status_code=400, detail="Could not load any data from uploaded files.")

        profile = _df_profile(df)
        sample_csv = _df_sample_csv(df, n=20)

        # 1) Rewrite question to analyst brief (internal)
        rewrite_system = (
            "You are a senior data analyst. Rewrite the user's question into a concise analysis brief with:
"
            "- objective(s)
- metric(s)
- filters/segments (if any)
- grouping/dimensions (if any)
- timeframe (if mentioned)
"
            "- explicit assumptions when unspecified
Return only the brief."
        )
        rw = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            temperature=0.0,
            messages=[
                {"role": "system", "content": rewrite_system},
                {"role": "user", "content": f"User question:\n{user_q}"},
            ],
        )
        analyst_brief = rw["choices"][0]["message"]["content"]

        # 2) Answer using only profile + sample
        answer_system = (
            "You are a precise data analyst. Use ONLY the provided data profile and sample rows to answer. "
            "If information is missing, state exactly what columns or steps are needed. "
            "Keep steps short; give a clear, actionable answer."
        )
        user_context = (
            f"DATA PROFILE\n{profile}\n\n"
            f"SAMPLE ROWS (CSV, up to 20)\n{sample_csv}\n\n"
            f"ANALYST BRIEF (internal)\n{analyst_brief}\n\n"
            f"TASK\nAnswer the brief using only the profile and sample."
        )
        resp = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            temperature=0.2,
            messages=[
                {"role": "system", "content": answer_system},
                {"role": "user", "content": user_context},
            ],
        )
        answer = resp["choices"][0]["message"]["content"]

        # Store only Q&A
        sess.setdefault("questions", [])
        sess["questions"].append({"question": user_q, "answer": answer})

        return {"answer": answer}
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"ask failed: {type(e).__name__}: {e}")

# -------- Export (answers only) --------
@app.get("/api/export")
async def export_pdf(session_id: str):
    sess = session_data.get(session_id)
    if not sess:
        raise HTTPException(status_code=400, detail="Invalid session_id")

    pdf = FPDF()
    pdf.add_page()
    pdf.set_auto_page_break(auto=True, margin=12)

    # Header
    pdf.set_font("Arial", "B", 14)
    pdf.cell(0, 10, "Report Magician — Q&A Summary", ln=True)
    pdf.set_font("Arial", size=11)
    pdf.cell(0, 8, f"Project: {sess.get('project','')}", ln=True)
    pdf.cell(0, 8, f"Email: {sess.get('email','')}", ln=True)
    pdf.ln(4)

    # Q&A (answers only)
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
