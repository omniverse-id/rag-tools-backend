from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware # Tambah ini
import pandas as pd
import io

app = FastAPI(
    title="Parquet to JSONL Converter API",
    description="API untuk mengkonversi file Parquet menjadi JSONL.",
    version="1.0.0"
)

# Konfigurasi CORS
origins = [
    "http://localhost:5173",  # Ini adalah alamat frontend React-mu
    "http://127.0.0.1:5173",  # Tambahkan juga ini sebagai antisipasi
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def read_root():
    return {"message": "Selamat datang di Parquet to JSONL Converter API!"}

@app.post("/convert/")
async def convert_file(file: UploadFile = File(...)):
    """
    Mengkonversi file Parquet yang diunggah ke format JSONL.
    """
    if not file.filename.endswith('.parquet'):
        raise HTTPException(status_code=400, detail="Hanya file .parquet yang diizinkan.")
    
    try:
        contents = await file.read()
        df = pd.read_parquet(io.BytesIO(contents))
        jsonl_data = df.to_json(orient='records', lines=True)
        
        return JSONResponse(
            content=jsonl_data,
            media_type="application/jsonlines"
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Terjadi kesalahan saat konversi: {e}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)