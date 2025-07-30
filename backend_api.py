import os
import uuid
import boto3
from fastapi import FastAPI, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET = os.getenv("S3_BUCKET")

s3 = boto3.client("s3", region_name=AWS_REGION,
                  aws_access_key_id=AWS_ACCESS_KEY_ID,
                  aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

@app.post("/api/upload")
async def upload_files(
    project_name: str = Form(...),
    session_id: str = Form(...),
    files: list[UploadFile] = File(...)
):
    results = {}
    for file in files:
        filename = f"{uuid.uuid4().hex}_{file.filename}"
        s3_key = f"projects/{project_name}/{session_id}/uploads/{filename}"
        try:
            s3.upload_fileobj(
                file.file,
                S3_BUCKET,
                s3_key,
                ExtraArgs={"ACL": "private"}
            )
            results[file.filename] = {
                "status": "uploaded",
                "s3_key": s3_key
            }
        except Exception as e:
            results[file.filename] = {
                "status": "failed",
                "error": str(e)
            }
    return results