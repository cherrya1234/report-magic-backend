import os
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from uuid import uuid4
import boto3

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

        # Upload to S3
        s3.upload_fileobj(file.file, BUCKET_NAME, s3_key)

        # Save session data
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
