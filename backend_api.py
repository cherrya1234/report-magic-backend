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

# Allow frontend to connect
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

# Global variables for session data
datasets = {}
session_store = {}
summary_text = ""
qa_answers = []

# Load OpenAI API key from environment
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

    global datasets, summary_text
    datasets.clear()
    summary_text = ""

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

            # Optionally update summary_text for PDF export
            summary_text += f"\n=== {file.filename} ===\n"
            summary_text += f"Shape: {df.shape}\n"
            summary_text += f"Columns: {', '.join(df.columns)}\n"

        except Exception as e:
            results[file.filename] = {"error": str(e)}

    # Store for later use (Q&A)
    session_store[session_id] = dfs
    datasets.update(dfs)

    return {
        "status": "success",
        "session_id": session_id,
        "files": results
    }

@app.post("/api/ask")
async def ask(data: dict):
    global qa_answers
    question = data.get("question", "")
    context = ""

    for name, df in datasets.items():
        context += f"File: {name}\n{df.head(5).to_csv(index=False)}\n\n"
    prompt = f"Context:\n{context}\n\nQuestion: {question}\nAnswer:"
    
    response = openai.ChatCompletion.create(
        model="gpt-4",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.3
    )

    answer = response.choices[0].message.content
    qa_answers.append((question, answer))
    
    return {"answer": answer}

@app.get("/api/export")
async def export_pdf():
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", size=12)

    pdf.multi_cell(0, 10, f"Data Summary:\n\n{summary_text}")

    if qa_answers:
        pdf.ln(10)
        pdf.set_font("Arial", style='B', size=12)
        pdf.cell(0, 10, "Q&A:", ln=True)
        pdf.set_font("Arial", size=12)
        for q, a in qa_answers:
            pdf.multi_cell(0, 10, f"Q: {q}\nA: {a}\n")

    filename = "report.pdf"
    pdf.output(filename)
    
    return FileResponse(filename, media_type="application/pdf", filename=filename)

