from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import requests

#Initializing the Fast API object
app = FastAPI()

#Middleware to make api to accept the requests from all kinds of sources
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# To get the file from Blob Storage
@app.on_event("startup")
async def Startup():
    # Blob URL
    blob_url = "https://911dataset.blob.core.windows.net/dataset/911_call_dataset.csv"
    # Path to save the downloaded file
    save_path = "assets/911_call_dataset.csv"
    # Download the file
    response = requests.get(blob_url)
    # Check if the request was successful
    if response.status_code == 200:
        # Write the content to a local file
        with open(save_path, 'wb') as file:
            file.write(response.content)
        print("File downloaded successfully!")
    else:
        print("Failed to download the file :", response.status_code)

#To calculate calls per year
@app.get("/callsperyear")
async def callsperyear():
    file_path="assets/911_call_dataset.csv"
    data=pd.read_csv(file_path)
    years=data["Year"].unique()
    years = [int(year) for year in years]
    print(years)
    print(type(years[0]))
    year_data=[]
    for year in years:
        data_filtered = data[(data['Year'] == year)]
        year_data.append({"Year":year,"call_count":len(data_filtered)})
    return {"result":year_data}

#To calculate calls per state
@app.get("/callsperstate")
async def callsperstate():
    file_path="assets/911_call_dataset.csv"
    data=pd.read_csv(file_path)
    states=data["State"].unique()
    state_data=[]
    for state in states:
        data_filtered = data[(data['State'] == state)]
        state_data.append({"state":state,"call_count":len(data_filtered)})
    return {"result":state_data}

# API to get the unique states and years
@app.get("/uniqueyearstate")
def getKeysData():
    file_path="assets/911_call_dataset.csv"
    data=pd.read_csv(file_path)
    states=[]
    years=[]
    for i in data["State"].unique():
        states.append(str(i))

    for i in data["Year"].unique():
        years.append(int(i))
    return {"states":states,"years":years}

# To get the calls per month
@app.get("/callspermonth/{year}/{state}")
async def callspermonth(year:int,state:str):
    month_data=[]
    file_path="assets/911_call_dataset.csv"
    data=pd.read_csv(file_path)
    data_filtered = data[(data['Year'] == year) & (data['State'] == state)]
    for month in range(1,13):
        data_month=data_filtered[(data_filtered["Month"]==month)]
        month_data.append({"month":month,"calls":len(data_month)})
    return {"result":month_data}

# To get the calls for kind of Emergency
@app.get("/callsfortypeofemergency/{year}/{state}")
def callsForTypeOfEmergency(year:int,state:str):
    emergency_data=[]
    file_path="assets/911_call_dataset.csv"
    data=pd.read_csv(file_path)
    e_types=data["Emergency_Type"].unique()
    data_filtered = data[(data['Year'] == year) & (data['State'] == state)]
    print(len(data_filtered))
    for emergency in e_types:
        data_emergency=data_filtered[(data_filtered['Emergency_Type'] == str(emergency))]
        emergency_data.append({"Emergency":str(emergency),"call_count":len(data_emergency)})
    return {"result":emergency_data}

# To get the calls from differenet kinds of sources
@app.get("/callsources/{year}/{state}")
def deathsforagegroup(year:int,state:str):
    source_data=[]
    file_path="assets/911_call_dataset.csv"
    data=pd.read_csv(file_path)
    sources=data["Call_Source"].unique()
    data_filtered = data[(data['Year'] == year) & (data['State'] == state)]
    for i in sources:
        data_source = data_filtered[(data_filtered['Call_Source'] == str(i))]
        source_data.append({"source": i,"call_count":len(data_source)})
    return {"result":source_data}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
