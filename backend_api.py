from fastapi import FastAPI, File, UploadFile, Form
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import List
import pandas as pd
import io
import openai
from fpdf import FPDF
import os
import uuid

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

@app.get("/")
def root():
    return {"message": "✅ Report Magic API is running!"}

datasets = {}
session_store = {}
summary_store = {}
qa_store = {}

openai.api_key = os.getenv("OPENAI_API_KEY")

@app.post("/api/upload")
async def upload_excel_files(
    files: List[UploadFile] = File(...),
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
                "missing_values": df.isnull().sum().to_dict(),
                "dtypes": df.dtypes.astype(str).to_dict(),
                "preview": df.head(5).to_dict(orient="records")
            }

            results[file.filename] = summary

        except Exception as e:
            results[file.filename] = {"error": str(e)}

    session_store[session_id] = dfs
    summary_store[session_id] = results
    qa_store[session_id] = []

    return {
        "status": "success",
        "session_id": session_id,
        "files": results
    }

@app.post("/api/ask")
async def ask(data: dict):
    session_id = data.get("session_id")
    question = data.get("question", "")

    if not session_id or session_id not in session_store:
        return {"answer": "⚠️ No data found for this session. Please upload files again."}

    dfs = session_store[session_id]
    context = ""
    for name, df in dfs.items():
        context += f"File: {name}\n{df.head(5).to_csv(index=False)}\n\n"

    prompt = f"Context:\n{context}\n\nQuestion: {question}\nAnswer:"

    response = openai.ChatCompletion.create(
        model="gpt-4",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.3
    )
    answer = response.choices[0].message.content
    qa_store[session_id].append((question, answer))

    return {"answer": answer}

@app.get("/api/export")
async def export_pdf(session_id: str):
    if session_id not in summary_store:
        return {"error": "Session not found"}

    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", size=12)

    summary_text = ""
    for f, meta in summary_store[session_id].items():
        summary_text += f"=== {f} ===\nShape: {meta['shape']}\nColumns: {', '.join(meta['columns'])}\n\n"

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