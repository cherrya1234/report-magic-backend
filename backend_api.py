from fastapi import FastAPI, File, UploadFile, Form, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional
import boto3
from botocore.exceptions import ClientError
import uuid
import os
import time
import re

app = FastAPI()

# CORS (consider restricting to your frontend origin in production)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "report-magician-files")
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL")  # optional

s3 = boto3.client(
    "s3",
    region_name=AWS_REGION,
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    endpoint_url=S3_ENDPOINT_URL,
)

def slugify(value: str) -> str:
    v = value.strip().lower()
    v = re.sub(r"[^a-z0-9]+", "-", v)
    return re.sub(r"-+", "-", v).strip("-") or "project"

def safe_filename(name: str) -> str:
    base = name.split("/")[-1].split("\\")[-1]
    return re.sub(r"[^A-Za-z0-9._-]", "_", base)

@app.get("/")
def root():
    return {"message": "✅ Report Magician Backend is live with S3 private uploads."}

@app.post("/upload-to-s3")
async def upload_to_s3(
    file: UploadFile = File(...),
    project_name: str = Form(...),
    session_id: Optional[str] = Form(None),
):
    """
    Upload a single file to S3 under:
      projects/{project-slug}/{session_id}/uploads/{timestamp}_{uuid}_{filename}
    Returns the S3 key and the session_id (generated if not provided).
    """
    if not session_id:
        session_id = str(uuid.uuid4())

    contents = await file.read()
    ts = int(time.time())
    project_slug = slugify(project_name)
    key_name = f"{ts}_{uuid.uuid4().hex}_{safe_filename(file.filename)}"

    s3_key = f"projects/{project_slug}/{session_id}/uploads/{key_name}"

    try:
        s3.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_key,
            Body=contents,
            ContentType=file.content_type or "application/octet-stream",
        )
    except ClientError as e:
        return {"error": f"S3 upload failed: {e.response.get('Error', {}).get('Message', str(e))}"}

    return {"message": "Upload successful", "key": s3_key, "session_id": session_id}

@app.get("/presign-get")
def presign_get(key: str = Query(...), expires_in: int = Query(3600)):
    """
    Generate a time-limited presigned URL (GET) for a private S3 object.
    """
    try:
        url = s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": S3_BUCKET_NAME, "Key": key},
            ExpiresIn=int(expires_in),
        )
        return {"url": url}
    except Exception as e:
        return {"error": str(e)}
