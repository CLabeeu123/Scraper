# Import the neede packages
import pandas as pd
import numpy as np
import logging
import pymongo as mongo
import redis
from bs4 import BeautifulSoup
import requests
import time

# Make connection to your database without security
client = mongo.MongoClient("mongodb://127.0.0.1:27017")

# Make a connection with redis
connect = redis.Redis()

# Make new list or arrays
HashTable = []
TimeTable = []
BtcTable = []
UsdTable = []

# Make new list or arrays
FilledHashTable = []
FilledTimeTable = []
FilledBtcTable = []
FilledUsdTable = []

# Connect to the existing names
database = client["Database"]
DataInBase = database["Data"]

# Make a function

def Scraper(HashTable, TimeTable, BtcTable, UsdTable, connect, DataInBase):

    # The website you will be scraping
    r = requests.get(
        'https://www.blockchain.com/btc/unconfirmed-transactions')

    # Put it into new variable
    content = r.content

    # Use the Bs4 package to parse the website
    soup = BeautifulSoup(content, "html.parser")

    # The needed div you want to scrape
    for d in soup.findAll('div', attrs={'class': 'sc-1g6z4xm-0 hXyplo'}):

        # The needed classes you need
        Hash = d.find('a', attrs={
                      'class': 'sc-1r996ns-0 fLwyDF sc-1tbyx6t-1 kCGMTY iklhnl-0 eEewhk d53qjk-0 ctEFcK'})
        Time = d.findAll('span', attrs={
                         'class': 'sc-1ryi78w-0 cILyoi sc-16b9dsl-1 ZwupP u3ufsr-0 eQTRKC'})
        btc = d.find('div', class_='sc-1au2w4e-0 fTyXWG')

        btcConvert = float(btc.text[12:len(btc.text) - 3].strip())

        usd = d.findAll('span', attrs={
                        'class': 'sc-1ryi78w-0 cILyoi sc-16b9dsl-1 ZwupP u3ufsr-0 eQTRKC'})
        usdConvert = float(usd[2].text[1:].replace(',', '').replace('$', ''))

        # If the value is not found it will say it is unknown else it will show the output and push to Redis
        if Hash is not None:
            FilledHashTable.append(Hash.text)
            connect.rpush("Hash", str(Hash.text))

        else:
            FilledHashTable.append("Hash is not known")

        if Time is not None:
            FilledTimeTable.append(Time[0].text)
            connect.rpush("Time", str(Time[0].text))

        else:
            FilledTimeTable.append("Time is not known")

        if btcConvert is not None:
            FilledBtcTable.append(btcConvert)
            connect.rpush("Amount(BTC)", str(btcConvert))

        else:
            FilledBtcTable.append("BTC is not known")

        if usd is not None:
            usdConvert = btcConvert * usdConvert
            FilledUsdTable.append(usdConvert)
            connect.rpush("Amount(USD)", str(usdConvert))

        else:
            FilledUsdTable.append("USD is not known")

    # Store the values and refresh every 60 seconds
    connect.expire("Hash", 60)
    connect.expire("Time", 60)
    connect.expire("Amount(BTC)", 60)
    connect.expire("Amount(USD)", 60)


while True:

    # Call your function
    Scraper(HashTable, TimeTable, BtcTable, UsdTable, connect, DataInBase)

    # Send output every 60 seconds
    time.sleep(60)
