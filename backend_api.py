import os
import io
from uuid import uuid4
from datetime import date

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
def df_sample_csv(df: pd.DataFrame, n=20) -> str:
    return df.sample(min(n, len(df)), random_state=42).to_csv(index=False)

def df_profile(df: pd.DataFrame) -> str:
    prof = []
    prof.append("SCHEMA:")
    for col in df.columns:
        dtype = str(df[col].dtype)
        nulls = int(df[col].isna().sum())
        prof.append(f"- {col}: dtype={dtype}, nulls={nulls}")
    prof.append("\nSUMMARY:")
    num = df.select_dtypes(include=["number"])
    if not num.empty:
        desc = num.describe().round(3).to_dict()
        prof.append(f"numeric_describe={desc}")
    cat = df.select_dtypes(exclude=["number"])
    if not cat.empty:
        cats = {}
        for c in cat.columns[:8]:
            vc = cat[c].astype(str).value_counts().head(5).to_dict()
            cats[c] = vc
        prof.append(f"categorical_top_values={cats}")
    return "\n".join(prof)

def load_first_session_df(s3_client, bucket: str, sess: dict) -> pd.DataFrame:
    key = sess["files"][0]
    buf = io.BytesIO()
    s3_client.download_fileobj(bucket, key, buf)
    buf.seek(0)
    return pd.read_excel(buf)

# -------- Health --------
@app.get("/")
def health():
    return {"ok": True, "service": "Report Magician backend"}

# -------- Upload --------
@app.post("/api/upload")
async def upload_excel(
    projectName: str = Form(...),
    email: str = Form(...),
    file: UploadFile = File(...),
):
    try:
        session_id = str(uuid4())
        unique_filename = f"{int(uuid4().int % 1e10)}_{file.filename}"
        s3_key = f"projects/{projectName}/{session_id}/uploads/{unique_filename}"

        file.file.seek(0)
        s3.upload_fileobj(file.file, BUCKET_NAME, s3_key)

        session_data.setdefault(session_id, {
            "email": email,
            "project": projectName,
            "questions": [],   # list of {question, answer, (optional analyst_brief)}
            "files": []
        })
        session_data[session_id]["files"].append(s3_key)

        return {"session_id": session_id, "s3_key": s3_key}
    except Exception as e:
        print("Upload error:", repr(e))
        raise HTTPException(status_code=500, detail="Upload failed.")

# -------- Ask (rewrite + grounded answer) --------
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

        # 1) Load data and build compact profile + sample
        df = load_first_session_df(s3, BUCKET_NAME, sess)
        profile = df_profile(df)
        sample_csv = df_sample_csv(df, n=20)

        # 2) Rewrite the question into an analyst-style brief (internal only)
        rewrite_system = (
            "You are a senior data analyst. "
            "Rewrite the user's question into a concise analysis brief with objectives, metrics, filters, group-bys, "
            "timeframe (if any), and assumptions if unspecified. Return only the brief."
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

        # 3) Answer grounded in the profile + tiny sample
        answer_system = (
            "You are a precise data analyst. Use ONLY the provided data profile and sample rows. "
            "If information is missing, say what column(s) or transformations are needed. "
            "Show steps briefly (filters, group-bys, metrics) and give a clear answer."
        )
        user_context = (
            f"DATA PROFILE\n{profile}\n\n"
            f"SAMPLE ROWS (CSV, up to 20)\n{sample_csv}\n\n"
            f"ANALYST BRIEF\n{analyst_brief}\n\n"
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

        # Save only Q & A for PDF (we keep brief internally but do NOT export it)
        sess.setdefault("questions", [])
        sess["questions"].append({
            "question": user_q,
            "answer": answer,
            "analyst_brief": analyst_brief,  # kept in memory ONLY; export ignores this
        })

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

    # Q&A (answers only; NO analyst brief)
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