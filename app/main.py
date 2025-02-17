from fastapi import FastAPI
from motor.motor_asyncio import AsyncIOMotorClient
import httpx
from dotenv import load_dotenv
from typing import Any, Dict
import os

app = FastAPI()

load_dotenv()

API_KEY = os.getenv("API_KEY")
BASE_URL = "https://api.ember-energy.org"
ZONE = "IND"
PERIOD = "monthly"  # Options: "monthly", "yearly"
START_DATE = "2024-11"  
END_DATE = "2024-12"

QUERY_URL = (
    f"{BASE_URL}/v1/electricity-generation/{PERIOD}"
    f"?entity_code={ZONE}&is_aggregate_series=false&start_date={START_DATE}"
    f"&end_date={END_DATE}&api_key={API_KEY}"
)


MONGO_URI = "mongodb+srv://smartgrid:yQPJi5bLVrWLsd6s@psgsynccluster.yuuy7.mongodb.net/"
client = AsyncIOMotorClient(MONGO_URI)
db = client["test_db"]
collection = db["WorldEnergy"]

@app.get("/")
async def get_world_data():
    async with httpx.AsyncClient() as client:
        response = await client.get(QUERY_URL)

    if response.status_code == 200:
        data = response.json()
        return data
    else:
        return {"error": "Unable to fetch data", "status_code": response.status_code}

@app.post("/add")
async def store_world_data():
    async with httpx.AsyncClient() as client:
        response = await client.get(QUERY_URL)

    if response.status_code == 200:
        data = response.json()
        
        for record in data['data']:
            record_data = {
                "entity": record['entity'],
                "entity_code": record['entity_code'],
                "date": record['date'],
                "series": record['series'],
                "generation_twh": record['generation_twh'],
                "share_of_generation_pct": record['share_of_generation_pct'],
            }
            await collection.insert_one(record_data)
            
        return {"message": "Data Inserted"}
    else:
        return {"error": "Failed to fetch data", "status_code": response.status_code}