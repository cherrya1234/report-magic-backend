
from fastapi import FastAPI, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
import boto3
import uuid
import os

app = FastAPI()

# CORS for frontend integration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

s3_client = boto3.client(
    "s3",
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)

@app.post("/api/upload")
async def upload(
    files: list[UploadFile] = File(...),
    session_id: str = Form(...),
    project_name: str = Form(...)
):
    uploaded_files = []
    for file in files:
        filename = f"{uuid.uuid4().hex}_{file.filename}"
        s3_key = f"projects/{project_name}/{session_id}/uploads/{filename}"
        s3_client.upload_fileobj(
            file.file,
            BUCKET_NAME,
            s3_key,
            ExtraArgs={"ACL": "private", "ContentType": file.content_type}
        )
        uploaded_files.append({
            "file": file.filename,
            "s3_key": s3_key,
        })
    return {"uploaded": uploaded_files}
