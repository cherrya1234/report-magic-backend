import os
from uuid import uuid4

import boto3
import openai  # legacy SDK (0.28.1)
from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware

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

BUCKET_NAME = "report-magician-files"

s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-1",
)

openai.api_key = os.getenv("OPENAI_API_KEY")

session_data = {}

@app.get("/")
def health():
    return {"status": "ok", "service": "Report Magician backend"}

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

        session_data.setdefault(session_id, {"email": email, "project": projectName, "questions": [], "files": []})
        session_data[session_id]["files"].append(s3_key)

        return {"session_id": session_id, "s3_key": s3_key}
    except Exception as e:
        print("Upload error:", repr(e))
        raise HTTPException(status_code=500, detail="Upload failed.")

@app.post("/api/ask")
async def ask_question(request: Request):
    try:
        data = await request.json()
        session_id = data.get("session_id")
        prompt = data.get("prompt")

        if not session_id or not prompt:
            raise HTTPException(status_code=400, detail="Missing session_id or prompt.")

        resp = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
        )
        answer = resp["choices"][0]["message"]["content"]

        session_data.setdefault(session_id, {"questions": []})
        session_data[session_id]["questions"].append({"question": prompt, "answer": answer})

        return {"answer": answer}
    except HTTPException:
        raise
    except Exception as e:
        print("OpenAI error:", repr(e))
        raise HTTPException(status_code=500, detail="Error getting answer")