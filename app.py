import httpx
import uvicorn
from pydantic import BaseModel
from fastapi import FastAPI, Request, HTTPException
from contextlib import asynccontextmanager

orchestrator_url = "http://localhost:8081/"

class LoadTestRequest(BaseModel):
    test_type: str
    test_server: str = "http://localhost:8080/ping"
    test_message_delay: int
    message_count_per_driver: int
    
class TestConfig(BaseModel):
    TestType: str    

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.requests_client = httpx.AsyncClient()
    yield
    await app.requests_client.aclose()

app = FastAPI(lifespan=lifespan)

@app.post("/trigger-load-test")
async def trigger_load_test(request_data: LoadTestRequest):
    # Convert pydantic model to dict to send as JSON payload
    payload = request_data.model_dump()

    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(f"{orchestrator_url}trigger-load-test", json=payload)
            response.raise_for_status()  # Raise an exception for 4xx or 5xx responses
            return response.json()
    except httpx.HTTPError as exc:
        # Handle HTTP errors
        raise HTTPException(status_code=exc.response.status_code, detail="HTTP Error occurred")

    except httpx.RequestError:
        # Handle other request errors
        raise HTTPException(status_code=500, detail="Request Error occurred")
        
@app.get("/test-config")
async def get_test_config():
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{orchestrator_url}test-config")
            response.raise_for_status()  # Raise an exception for 4xx or 5xx responses
            return response.json()
    except httpx.HTTPError as exc:
        # Handle HTTP errors
        raise HTTPException(status_code=exc.response.status_code, detail="HTTP Error occurred")
    except httpx.RequestError:
        # Handle other request errors
        raise HTTPException(status_code=500, detail="Request Error occurred")

@app.get("/metrics/{nodeid}")
async def retrieve_metrics_for_node(nodeid: str):
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{orchestrator_url}metrics/{nodeid}")
            response.raise_for_status()  # Raise an exception for 4xx or 5xx responses
            return response.json()
    except httpx.HTTPError as exc:
        # Handle HTTP errors
        raise HTTPException(status_code=exc.response.status_code, detail="HTTP Error occurred")
    except httpx.RequestError:
        # Handle other request errors
        raise HTTPException(status_code=500, detail="Request Error occurred")

@app.get("/all-metrics")
async def retrieve_all_metrics():
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{orchestrator_url}all-metrics")
            response.raise_for_status()  # Raise an exception for 4xx or 5xx responses
            return response.json()
    except httpx.HTTPError as exc:
        # Handle HTTP errors
        raise HTTPException(status_code=exc.response.status_code, detail="HTTP Error occurred")
    except httpx.RequestError:
        # Handle other request errors
        raise HTTPException(status_code=500, detail="Request Error occurred")
    
@app.get("/heartbeat/{nodeid}")
async def retrieve_heartbeat(nodeid: str):
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{orchestrator_url}heartbeat/{nodeid}")
            response.raise_for_status()  # Raise an exception for 4xx or 5xx responses
            return response.json()
    except httpx.HTTPError as exc:
        # Handle HTTP errors
        raise HTTPException(status_code=exc.response.status_code, detail="HTTP Error occurred")
    except httpx.RequestError:
        # Handle other request errors
        raise HTTPException(status_code=500, detail="Request Error occurred")
    
@app.get("/all-nodes")
async def retrieve_all_nodes():
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{orchestrator_url}all-nodes")
            response.raise_for_status()  # Raise an exception for 4xx or 5xx responses
            return response.json()
    except httpx.HTTPError as exc:
        # Handle HTTP errors
        raise HTTPException(status_code=exc.response.status_code, detail="HTTP Error occurred")
    except httpx.RequestError:
        # Handle other request errors
        raise HTTPException(status_code=500, detail="Request Error occurred") 
if __name__ == "__main__":
    uvicorn.run(app, port=8000)