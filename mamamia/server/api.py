from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Any, List, Optional
from .registry import LogRegistry

app = FastAPI(title="Mamamia Message Delivery System")
registry = LogRegistry()


@app.on_event("startup")
async def startup_event():
    registry.start_reaper(interval=30.0)


class ProduceRequest(BaseModel):
    payload: Any
    metadata: Optional[dict] = None


class LeaseRequest(BaseModel):
    client_id: str
    duration: float = 30.0


class SettleRequest(BaseModel):
    client_id: str
    success: bool


@app.post("/logs/{log_id}/messages")
async def produce(log_id: str, request: ProduceRequest):
    storage = registry.get_storage()
    msg_id = await storage.append(log_id, request.payload, request.metadata)
    return {"message_id": msg_id}


@app.post("/logs/{log_id}/groups/{group_id}/acquire_next")
async def acquire_next(log_id: str, group_id: str, request: LeaseRequest):
    orch = registry.get_orchestrator(log_id)
    message = await orch.acquire_next(
        log_id, group_id, request.client_id, request.duration
    )
    if not message:
        return {"message": None}
    return {"message": message}


@app.post("/logs/{log_id}/groups/{group_id}/messages/{message_id}/settle")
async def settle(log_id: str, group_id: str, message_id: int, request: SettleRequest):
    orch = registry.get_orchestrator(log_id)
    try:
        await orch.settle(
            log_id, group_id, message_id, request.client_id, request.success
        )
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    return {"status": "settled"}
