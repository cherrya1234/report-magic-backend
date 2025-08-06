from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import List
import boto3
from botocore.exceptions import NoCredentialsError
import uuid
import os

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)

BUCKET_NAME = "report-magician-files"

@app.post("/api/upload")
async def upload_files(
    files: List[UploadFile] = File(...),
    session_id: str = Form(...),
    project_name: str = Form(...),
    email: str = Form(...)
):
    uploaded_files = []
    for file in files:
        try:
            file_key = f"{session_id}/{file.filename}"
            s3_client.upload_fileobj(file.file, BUCKET_NAME, file_key)
            uploaded_files.append(file.filename)
        except NoCredentialsError:
            raise HTTPException(status_code=500, detail="AWS credentials not found")
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    return {"message": "Files uploaded successfully", "uploaded_files": uploaded_files}