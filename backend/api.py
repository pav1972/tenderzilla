from fastapi import FastAPI

app = FastAPI()


@app.get("/")
def root():
    return {"message": "TenderZilla API is running"}


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "tenderzilla-api"
    }


@app.get("/test")
def test():
    return {
        "test": "success",
        "message": "API is working correctly"
    }