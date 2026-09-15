from fastapi import FastAPI

from procurement.api.ops import router as ops_api_router
from procurement.api.ops.ui import router as ops_ui_router

app = FastAPI(title="Procurement Lakehouse Ops API", version="0.2.0")
app.include_router(ops_ui_router)
app.include_router(ops_api_router)
