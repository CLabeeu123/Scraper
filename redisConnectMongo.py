# Import the neede packages
import pandas as pd
import numpy as np
import logging
import pymongo as mongo
import json
import redis
from bs4 import BeautifulSoup
import requests
import time
import multiprocessing

# Make connection to your database without security
client = mongo.MongoClient("mongodb://127.0.0.1:27017")

# Make a connection with redis
connect = Redis.Redis()

# Connect to the existing names
database = client["Database"]
DataInBase = database["Data"]

# Make new list or arrays
ToHash = []
ToTime = []
ToBtc = []
ToUsd = []

# Make a function
def ToMongoDB(connect, DataInBase):

    #Fill in your lists or arrays
    ToHash = list(map(str, connect.lrange("LRANGE (name) 0 -1", 0, -1)))
    ToTime = list(map(str, connect.lrange("Time", 0, -1)))
    ToBtc = list(map(float, connect.lrange("Amount(BTC)", 0, -1)))
    ToUsd = list(map(float, connect.lrange("Amount(USD)", 0, -1)))

    #Pass on the values
    money = max(ToUsd)
    index = ToUsd.index(money)
    hashing = ToHash[index]
    timing = ToTime[index]
    bitcoining = ToBtc[index]

    #The things you are going to send
    Total = {"Hash": hashing, "Time": timing, "Amount(BTC)": bitcoining, "Amount(USD)": money}
    
    #Store output in DB
    DataInBase.insert_one(Total)
    
# Call youre function and overwrite every 60 seconds
while True:
    ToMongoDB(connect, DataInBase)
    time.sleep(60)