from fastapi import FastAPI
from motor.motor_asyncio import AsyncIOMotorClient
import httpx
from dotenv import load_dotenv
from typing import Any, Dict
import os
import json

app = FastAPI()

load_dotenv()

API_KEY = os.getenv("API_KEY")
BASE_URL = "https://api.ember-energy.org"
ZONE = "IND,BRA,USA,UAE"
PERIOD = "monthly"  
START_DATE = "2020"  
END_DATE = "2024"

QUERY_URL = (
    f"{BASE_URL}/v1/electricity-generation/{PERIOD}"
    f"?entity_code={ZONE}&is_aggregate_series=false&start_date={START_DATE}"
    f"&end_date={END_DATE}&api_key={API_KEY}"
)

MONGO_URI = "mongodb+srv://smartgrid:yQPJi5bLVrWLsd6s@psgsynccluster.yuuy7.mongodb.net/"
client = AsyncIOMotorClient(MONGO_URI)
db = client["test_db"]
collection = db["WorldEnergy"]
collection_tangedco = db["Tangedco"]

@app.get("/")
async def get_world_data():
    async with httpx.AsyncClient() as client:
        response = await client.get(QUERY_URL)

    if response.status_code == 200:
        return response.json()
    else:
        return {"error": "Unable to fetch data", "status_code": response.status_code}

@app.post("/add")
async def store_world_data():
    async with httpx.AsyncClient() as client:
        response = await client.get(QUERY_URL)

    if response.status_code == 200:
        data = response.json()
        inserted_count = 0
        
        for record in data['data']:
            record_data = {
                "entity": record['entity'],
                "entity_code": record['entity_code'],
                "date": record['date'],
                "series": record['series'],
                "generation_twh": record['generation_twh'],
                "share_of_generation_pct": record['share_of_generation_pct'],
            }

            existing_record = await collection.find_one({
                "entity_code": record_data["entity_code"],
                "date": record_data["date"],
                "series": record_data["series"]
            })

            if not existing_record:
                await collection.insert_one(record_data)
                inserted_count += 1
            
        return {"message": f"Inserted {inserted_count} new records"}
    else:
        return {"error": "Failed to fetch data", "status_code": response.status_code}
    
@app.post("/add/tangedco")
async def insert_tangedco_data():
    with open("TANGEDCO_data.json", "r") as file:
        tangedco_data = json.load(file)

    data_to_insert = tangedco_data if isinstance(tangedco_data, list) else [tangedco_data]
    
    await collection_tangedco.insert_many(data_to_insert)
    
    return {"message": "Tangedco Data inserted successfully"}

@app.delete("/delete")
async def delete_world_data():
    result = await collection.delete_many({})
    return {"message": f"Deleted {result.deleted_count} record(s) from the database."}
