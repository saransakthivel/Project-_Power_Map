from fastapi import FastAPI
import requests
app = FastAPI()

API_KEY = "3XZqHR3vs1Kx5"
BASE_URL = "https://api.electricitymap.org/v3/carbon-intensity/latest"

@app.get("/si")
async def get_south_india():
    headers = {
        "authorization" : f"Bearer {API_KEY}"
    }

    params = {
        "zone" : "IN-SO"
    }

    response = requests.get(BASE_URL, headers=headers, params=params)

    if response.status_code == 200:
        data = response.json()
        return data
    else:
        return {"error" : "unable to fetch data"}