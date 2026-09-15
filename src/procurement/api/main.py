from fastapi import FastAPI

from procurement.api.ops import router as ops_router

app = FastAPI(title="Procurement Lakehouse Ops API", version="0.2.0")
app.include_router(ops_router)
