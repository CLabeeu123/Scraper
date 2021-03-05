# Import the neede packages
import pandas as pd
import numpy as np
import pymongo as mongo
import redis
from bs4 import BeautifulSoup
import requests
import time
import multiprocessing

# Make a connection with redis
connect = redis.Redis()

# Make connection to your database without security
client = mongo.MongoClient("mongodb://127.0.0.1:27017")

# Connect to the existing names
database = client["Database"]
DataInBase = database["Data"]

# Make new list or arrays
toHash = []
toTime = []
toBtc = []
toUsd = []

# Make a function
def ToMongoDB(connect, DataInBase):

    #Fill in your lists or arrays
    toHash = list(map(str, connect.lrange("Hash", 0, -1)))
    toTime = list(map(str, connect.lrange("Time", 0, -1)))
    toBtc = list(map(float, connect.lrange("Amount(BTC)", 0, -1)))
    toUsd = list(map(float, connect.lrange("Amount(USD)", 0, -1)))

    #Pass on the values
    money = max(toUsd)
    index = toUsd.index(money)
    hashing = toHash[index]
    timing = toTime[index]
    bitcoining = toBtc[index]

    #The things you are going to send
    Total = {"Hash": hashing, "Time": timing, "Amount(BTC)": bitcoining, "Amount(USD)": money }
    
    #Store output in DB
    DataInBase.insert_one(Total)
    
# Call youre function and overwrite every 60 seconds
while True:
    ToMongoDB(connect, DataInBase)
    time.sleep(60)