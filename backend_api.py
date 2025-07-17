from fastapi import FastAPI, File, UploadFile
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import List
import pandas as pd
import io
import openai
from fpdf import FPDF
import os

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

datasets = {}
summary_text = ""
qa_answers = []

openai.api_key = os.getenv("OPENAI_API_KEY")

@app.post("/api/upload")
async def upload(files: List[UploadFile] = File(...)):
    global datasets, summary_text
    datasets.clear()
    summary_text = ""
    for file in files:
        contents = await file.read()
        df = pd.read_excel(io.BytesIO(contents))
        datasets[file.filename] = df
    summary_text = "\n\n".join([f"{name}: {df.shape[0]} rows, {df.shape[1]} columns\nColumns: {', '.join(df.columns)}" for name, df in datasets.items()])
    return {"summary": summary_text}

@app.post("/api/ask")
async def ask(data: dict):
    global qa_answers
    question = data.get("question", "")
    context = ""
    for name, df in datasets.items():
        context += f"File: {name}\n{df.head(5).to_csv(index=False)}\n\n"
    prompt = f"Context:\n{context}\n\nQuestion: {question}\nAnswer:"
    response = openai.ChatCompletion.create(model="gpt-4", messages=[{"role": "user", "content": prompt}], temperature=0.3)
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