from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "Backend running"}
@app.post("/api/upload")
async def upload(files: List[UploadFile] = File(...), ...):
