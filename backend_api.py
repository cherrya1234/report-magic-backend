import os
from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from uuid import uuid4
import boto3
import openai

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

session_data = {}

s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)

BUCKET_NAME = "report-magician-files"

openai.api_key = os.getenv("OPENAI_API_KEY")

@app.post("/api/upload")
async def upload_excel(
    projectName: str = Form(...),
    email: str = Form(...),
    file: UploadFile = File(...)
):
    try:
        session_id = str(uuid4())
        unique_filename = f"{int(uuid4().int % 1e10)}_{file.filename}"
        s3_key = f"projects/{projectName}/{session_id}/uploads/{unique_filename}"

        s3.upload_fileobj(file.file, BUCKET_NAME, s3_key)

        session_data[session_id] = {
            "email": email,
            "project": projectName,
            "questions": []
        }

        return {
            "session_id": session_id,
            "s3_key": s3_key
        }

    except Exception as e:
        print("Upload error:", str(e))
        raise HTTPException(status_code=500, detail="Upload failed.")

@app.post("/api/ask")
async def ask_question(request: Request):
    try:
        data = await request.json()
        session_id = data.get("session_id")
        prompt = data.get("prompt")

        if not session_id or not prompt:
            raise HTTPException(status_code=400, detail="Missing session_id or prompt.")

        session_info = session_data.get(session_id, {})

        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3
        )

        answer = response["choices"][0]["message"]["content"]

        if session_id in session_data:
            session_data[session_id]["questions"].append({"question": prompt, "answer": answer})

        return {"answer": answer}

    except Exception as e:
        print("OpenAI error:", str(e))
        raise HTTPException(status_code=500, detail="Error getting answer")