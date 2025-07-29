
from fastapi import FastAPI, UploadFile, File, Form
from fastapi.responses import JSONResponse
import boto3
import uuid
import os

app = FastAPI()

s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)

BUCKET_NAME = "report-magician-files"

@app.post("/api/upload")
async def upload(file: UploadFile = File(...), project: str = Form(...), email: str = Form(...)):
    key = f"projects/{project}/{uuid.uuid4()}/uploads/{uuid.uuid4()}_{file.filename}"
    s3.upload_fileobj(file.file, BUCKET_NAME, key)
    return {"status": "uploaded", "key": key}
