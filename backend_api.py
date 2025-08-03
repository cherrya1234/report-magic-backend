import os
from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from uuid import uuid4
import boto3
from openai import OpenAI
from openai.types.chat import ChatCompletionMessage

app = FastAPI()

origins = [
    "https://report-magician-frontend.vercel.app",
    "http://localhost:3000"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
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

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

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
async def ask_question(session_id: str = Body(...), question: str = Body(...)):
    if session_id not in session_store:
        return {"error": "Invalid session ID"}

    dfs = session_store[session_id]

    # Convert all dataframes to markdown
    combined_markdown = ""
    for name, df in dfs.items():
        combined_markdown += f"### File: {name}\n"
        combined_markdown += df.head(20).to_markdown() + "\n\n"

    prompt = f"""
You are a data analyst. A user has uploaded multiple Excel sheets. Below is a preview of the data (20 rows max per file).

{combined_markdown}

Question: {question}

Answer:
"""
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3
        )

        answer = response.choices[0].message.content

        if session_id in session_data:
            session_data[session_id]["questions"].append({"question": prompt, "answer": answer})

        return {"answer": answer}

    except Exception as e:
        print("OpenAI error:", str(e))
        raise HTTPException(status_code=500, detail="Error getting answer")
