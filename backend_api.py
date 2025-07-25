from fastapi import FastAPI, File, UploadFile, Form
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import List
import pandas as pd
import io
from openai import OpenAI
from fpdf import FPDF
import os
import uuid

app = FastAPI()

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def root():
    return {"message": "✅ Report Magic API is running!"}

session_store = {}   # session_id -> { filename: df }
summary_store = {}   # session_id -> summaries
qa_store = {}        # session_id -> list[(q, a)]

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def safe_summary_text(session_id: str) -> str:
    if session_id not in summary_store:
        return ""
    text = []
    for f, meta in summary_store[session_id].items():
        shape = meta.get("shape")
        cols = meta.get("columns", [])
        text.append(f"=== {f} ===\nShape: {shape}\nColumns: {', '.join(cols)}\n")
    return "\n".join(text)

def build_prompt(question: str, project_name: str, email: str, dfs: dict, max_rows: int = 20) -> str:
    parts = [
        f"Project: {project_name}",
        f"Email: {email}",
        "",
        f"Context (first {max_rows} rows per file):"
    ]
    for name, df in dfs.items():
        head_rows = df.head(max_rows)
        parts.append(f"File: {name}\n{head_rows.to_csv(index=False)}")
    parts.extend(["", f"Question: {question}", "Answer:"])
    return "\n".join(parts)

@app.post("/api/upload")
async def upload_excel_files(
    files: List[UploadFile] = File(...),
    project_name: str = Form(...),
    email: str = Form(...),
    question: str = Form(...),
    session_id: str = Form(None)
):
    if session_id is None:
        session_id = str(uuid.uuid4())

    results = {}
    dfs = {}

    for file in files:
        try:
            contents = await file.read()
            df = pd.read_excel(io.BytesIO(contents))
            dfs[file.filename] = df
            summary = {
                "filename": file.filename,
                "shape": df.shape,
                "columns": df.columns.tolist(),
                "missing_values": df.isnull().sum().fillna(0).astype(int).to_dict(),
                "dtypes": df.dtypes.astype(str).to_dict(),
                "preview": df.head(5).fillna("").to_dict(orient="records"),
            }
            results[file.filename] = summary
        except Exception as e:
            results[file.filename] = {"error": str(e)}

    session_store[session_id] = dfs
    summary_store[session_id] = results
    qa_store[session_id] = []

    prompt = build_prompt(question, project_name, email, dfs, max_rows=20)
    try:
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
        )
        answer = response.choices[0].message.content
    except Exception as e:
        answer = f"Error contacting OpenAI: {e}"

    qa_store[session_id].append((question, answer))

    return {
        "status": "success",
        "session_id": session_id,
        "files": results,
        "answer": answer
    }

@app.post("/api/clear")
async def clear_qa(data: dict):
    session_id = data.get("session_id")
    if not session_id or session_id not in qa_store:
        return {"status": "no_session"}
    qa_store[session_id].clear()
    return {"status": "cleared"}

@app.get("/api/export")
async def export_pdf(session_id: str):
    if session_id not in summary_store:
        return {"error": "Session not found"}

    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", size=12)
    summary_text = safe_summary_text(session_id)
    pdf.multi_cell(0, 10, f"Data Summary:\n\n{summary_text}")

    if qa_store.get(session_id):
        pdf.ln(10)
        pdf.set_font("Arial", style='B', size=12)
        pdf.cell(0, 10, "Q&A:", ln=True)
        pdf.set_font("Arial", size=12)
        for q, a in qa_store[session_id]:
            pdf.multi_cell(0, 10, f"Q: {q}\nA: {a}\n")

    filename = f"{session_id}_report.pdf"
    pdf.output(filename)
    return FileResponse(filename, media_type="application/pdf", filename="report.pdf")
