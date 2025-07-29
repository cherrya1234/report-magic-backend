from fastapi import FastAPI, File, UploadFile, Form
from fastapi.middleware.cors import CORSMiddleware
import boto3
import uuid
import os

app = FastAPI()

# Enable CORS for frontend access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize S3 client
s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name='us-east-1'  # Adjust as needed
)

BUCKET_NAME = "report-magician-files"

@app.post("/upload-to-s3")
async def upload_to_s3(
    file: UploadFile = File(...),
    project_name: str = Form(...)
):
    contents = await file.read()
    key = f"{project_name}/{uuid.uuid4()}_{file.filename}"

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=contents,
        ContentType=file.content_type
    )

    return {"message": "Upload successful", "key": key}

@app.get("/")
def root():
    return {"message": "✅ Report Magician Backend is live!"}