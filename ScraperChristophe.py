#Import the neede packages
import pandas as pd
import numpy as np
import logging

from bs4 import BeautifulSoup
import requests
import time

#Make a function
def Scraper():

    #The website you will be scraping
    website = requests.get('https://www.blockchain.com/btc/unconfirmed-transactions')

    #Put it into new variable
    content = website.content

    #Use the Bs4 package to parse the website
    soup = BeautifulSoup(content, "html.parser")
    
    #Make a new array or list
    filling = []

    #The needed div you want to scrape
    for d in soup.findAll('div', attrs={'class':'sc-1g6z4xm-0 hXyplo'}):
        
        #The needed classes you need
        Hash = d.find('a', attrs={'class':'sc-1r996ns-0 fLwyDF sc-1tbyx6t-1 kCGMTY iklhnl-0 eEewhk d53qjk-0 ctEFcK'})
        Time = d.findAll('span', attrs={'class':'sc-1ryi78w-0 cILyoi sc-16b9dsl-1 ZwupP u3ufsr-0 eQTRKC'})
        btc = d.findAll('span', attrs={'class':'sc-1ryi78w-0 cILyoi sc-16b9dsl-1 ZwupP u3ufsr-0 eQTRKC'})
        usd= d.findAll('span', attrs={'class':'sc-1ryi78w-0 cILyoi sc-16b9dsl-1 ZwupP u3ufsr-0 eQTRKC'})
       
        #Make a new array or list
        filled=[]

        #If the value is not found it will say it is unknown else it will show the output
        if Hash is not None:
            filled.append(Hash.text)
            
        else:
            filled.append("Hash is not known")

        if Time is not None:
            filled.append(Time[0].text)
            
        else:
            filled.append("Time is not known")

        if btc is not None:
            filled.append(btc[1].text)
            
        else:
            filled.append("BTC is not known")

        if usd is not None:
            filled.append(float(usd[2].text.replace(',','').replace('$','')))
            
        else:
            filled.append("USD is not known")
        
        #Fill the function
        filling.append(filled) 

        #Return your function
    return filling

#If they are no mistakes this will keep on running until stopped
while True:

  #Timer that puts output every 60 seconds
  time.sleep(60)

  #Call your function
  Scraper()

  #Make a new array or list
  results = []

  #Put the function results in your array or list
  results.append(Scraper())

  #Call an anonymous lambda function
  flatten = lambda l: [item for sublist in l for item in sublist]

  #Make a DataFrame with the needed data
  data = pd.DataFrame(flatten(results),columns=['Hash', 'Time', 'Amount(BTC)', 'Amount(USD)'])

  #Sort the data with the highest amount in USD
  data = data.sort_values('Amount(USD)', ascending=False)

  #Get the highest amount USD in a log file
  data.head(1).to_csv("Highest.log", header="Hash", index=None, sep='\t', mode='w')

  #Get a list with all the the highest per minute
  data.head(1).to_csv("List.log", header="Hash", index=None, sep='\t', mode='a')
  
  




