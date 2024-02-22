from bs4 import BeautifulSoup
from datetime import datetime
from aiohttp import ClientSession
from time import sleep
import pandas as pd
import asyncio
import csv
import pickle
import warnings
import os

warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=UserWarning)
warnings.simplefilter(action='ignore', category=DeprecationWarning)

import locale
locale.setlocale(locale.LC_TIME, "pl_PL")

# Loading translation dicts to map polish values into english
with open('colour_translation.csv', 'r') as data:
  for line in csv.DictReader(data):
      colour_translation = line
with open('country_mapping.csv', 'r') as data:
  for line in csv.DictReader(data):
      country_mapping = line
with open('drivetrain_translation.csv', 'r') as data:
  for line in csv.DictReader(data):
      drivetrain_translation = line
with open('fuel_translation.csv', 'r') as data:
  for line in csv.DictReader(data):
      fuel_translation = line
with open('paint_translation.csv', 'r') as data:
  for line in csv.DictReader(data):
      paint_translation = line
with open('type_translation.csv', 'r') as data:
  for line in csv.DictReader(data):
      type_translation = line
with open('makes.csv', 'r') as data:
  for line in csv.DictReader(data):
      makes = line
      
# Main function returns list of dicts as it's most convenient to store at the time
# This turns it into a dataframe, translates it to english, cleans it up and applies sensible dtypes
def get_dataframe(listings):
    # Raw dataframe with polish values
    offers_pl = pd.DataFrame.from_dict(listings)
    offers_pl.columns = offers_pl.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(', '').str.replace(')', '')

    # Target dataframe
    offers = pd.DataFrame()

    # Column name translations and reordering
    offers[['id', 'url', 'added_on', 'price', 'currency', 'latitude', 'longitude']] = \
        offers_pl[['id', 'url', 'date', 'price', 'currency', 'latitude', 'longitude']] 
    #offers[['make', 'model', 'generation', 'version', 'type', 'year', 'total_mileage']] = \
    #    offers_pl[['marka_pojazdu', 'model_pojazdu', 'generacja', 'wersja', 'typ_nadwozia', 'rok_produkcji', 'przebieg']]      
    offers[['make', 'model', 'version', 'type', 'year', 'total_mileage']] = \
        offers_pl[['marka_pojazdu', 'model_pojazdu', 'wersja', 'typ_nadwozia', 'rok_produkcji', 'przebieg']]
    offers[['fuel_type', 'engine_size', 'horsepower', 'drivetrain', 'auto_transmission', 'urban_mileage']] = \
        offers_pl[['rodzaj_paliwa', 'pojemność_skokowa', 'moc', 'napęd', 'skrzynia_biegów', 'spalanie_w_mieście']]
    offers[['doors', 'seats', 'colour', 'paint']] = \
        offers_pl[['liczba_drzwi', 'liczba_miejsc', 'kolor', 'rodzaj_koloru']]
    offers[['private_offer', 'first_owner', 'pl_registered', 'country']] = \
        offers_pl[['oferta_od', 'pierwszy_właściciel_od_nowości', 'zarejestrowany_w_polsce', 'kraj_pochodzenia']]
    offers[['used', 'accident_free', 'aso_serviced', 'has_vin', 'financing', 'leasing']] = \
        offers_pl[['stan', 'bezwypadkowy', 'serwisowany_w_aso', 'pokaż_oferty_z_numerem_vin', 'możliwość_finansowania', 'leasing']]
    offers[['bluetooth', 'carplay', 'android', 'cruisecontrol', 'camera_rear', 'rain_sensor']] = \
        offers_pl[['bluetooth', 'carplay', 'android', 'cruisecontrol', 'camera_rear', 'rain_sensor']]
        
    # Translating polish to english
    offers = offers.replace({'fuel_type': fuel_translation})
    offers = offers.replace({'colour': colour_translation})
    offers = offers.replace({'paint': paint_translation})
    offers = offers.replace({'drivetrain': drivetrain_translation})
    offers = offers.replace({'type': type_translation})
    offers = offers.replace({'country': country_mapping})
    
    # By default otomoto lists below attributes only if they are 'Tak' (Yes), let's add 'Nie's for easier mapping
    offers[['accident_free','aso_serviced', 'has_vin', 'financing', 'leasing', 'used', 'pl_registered', 'first_owner']] =  \
        offers[['accident_free','aso_serviced', 'has_vin', 'financing', 'leasing', 'used', 'pl_registered', 'first_owner']].fillna('Nie')

    offers = offers.replace({'Nie': False, 'Tak': True})
    offers = offers.replace({'Osoby prywatnej': True, 'Firmy': False})
    offers = offers.replace({'Używane': True, 'Nowy': False})
    offers = offers.replace({'Automatyczna': True, 'Manualna': False})

    # Translating common car model names (Seria A -> Series A etc)
    offers['model'] = offers['model'].str.replace('Seria', 'Series', regex=False)
    offers['model'] = offers['model'].str.replace('Klasa', 'Class', regex=False)

    # Convert geographic data into floats
    offers = offers.astype({'latitude': 'float', 'longitude': 'float'})
    
    # Convert columns with inherently low number of possible values into categories
    offers = offers.astype({'currency': 'category', 'make': 'category', 'type': 'category', 'fuel_type': 'category'})
    offers = offers.astype({'drivetrain': 'category', 'colour': 'category', 'paint': 'category', 'country': 'category'}) 

    offers = offers.astype({'year': 'Int16'})
    
    # Convert Tak/Nie (Yes/No) into boolean
    offers = offers.astype({'private_offer': 'bool', 'has_vin': 'bool', 'financing': 'bool', 'leasing': 'bool', 'first_owner': 'bool'})
    offers = offers.astype({'pl_registered': 'bool', 'accident_free': 'bool', 'used': 'bool', 'aso_serviced': 'bool', 'auto_transmission': 'bool'})

    # Clean numeric values from text
    offers['urban_mileage'] = offers['urban_mileage'].str.replace(' l/100km', '', regex=False).str.replace(' ', '', regex=False)
    offers['urban_mileage'] = offers['urban_mileage'].str.replace(',', '.', regex=False).astype('float')
    offers['engine_size'] = offers['engine_size'].str.replace(' cm3', '', regex=False)
    offers['engine_size'] = offers['engine_size'].str.replace(' ', '', regex=False).astype('Int16')

    offers['price'] = offers['price'].str.replace(r'\D+', '', regex=True).astype('Int32')
    offers['total_mileage'] = offers['total_mileage'].str.replace(r'\D+', '', regex=True).astype('Int32')

    offers['horsepower'] = offers['horsepower'].str.replace(r'\D+', '', regex=True).astype('Int16')
    offers['doors'] = offers['doors'].str.replace(r'\D+', '', regex=True).astype('Int16')
    offers['seats'] = offers['seats'].str.replace(r'\D+', '', regex=True).astype('Int16')

    # For some reason otomoto includes car type prefix in model name, let's drop that part since it's redundant
    offers['model'] = offers['model'].str.split(" ", 1).str[1]
    #offers['generation'] = offers['generation'].str.split('(').str[0][:-1]
    
    return offers

listings = []

# Primary function that sets up event loop and tracks task completion
# This function is fed only chunks of data to prevent overloading network connection
def fetch_async(urls):
    loop = asyncio.get_event_loop() 
    future = asyncio.ensure_future(fetch_all(urls)) 
    loop.run_until_complete(future) 
    
# Function to create task queue to execute in async parallel
async def fetch_all(urls):
    tasks = []
    async with ClientSession() as session:
        for url in urls:
            try:
                task = asyncio.ensure_future(fetch(url, session))
                tasks.append(task) 
            except:
                print('asyncio get error for ' + url)
        _ = await asyncio.gather(*tasks) 
    
# Function to do actual offer details fetching    
async def fetch(offer_url, session):
    try:
        # Encapsulating in try statement as very rarely valid urls lead to infinite redirects
        async with session.get(offer_url) as response:
            # This is a bit of naive way to retry but works for this purpose
            # Most issues are network or otomoto related and almost never persist for over a minute
            try:
                listing = await response.text()
            except:
                sleep(60)
                listing = await response.text()
                
            listing_html = BeautifulSoup(listing, 'html.parser')
                
            listing_attrs = {}
            
            # Getting offer attributes from offer page
            try:
                # ID is included in URL but stored separately for convenience
                id = offer_url[-13:-5]
                listing_attrs['id'] = id
                
                url = offer_url
                listing_attrs['url'] = url
                
                # This is presented in polish on otomoto but we can still parse it with earlier locale override
                date_text = listing_html.select('span.offer-meta__value')[0].text.strip()
                date_val = datetime.strptime(date_text, '%H:%M, %d %B %Y')
                listing_attrs['date'] = date_val

                price = listing_html.select('div.offer-price')
                listing_attrs['price'] = price[0]['data-price'].strip()

                currency = listing_html.select('span.offer-price__currency')
                listing_attrs['currency'] = currency[0].text.strip()
                
                # Trying to parse addresses from offers is a terrible idea
                # Private offers are mostly okay since they list municipality, county and voivodeship
                # but corporate ones are a true mess. Thankfully otomoto embeds Google map that
                # includes simple geographic coordinates that we can later map onto administrative regions later
                
                latitude = listing_html.select('input[id="adMapData"]')
                listing_attrs['latitude'] = latitude[0]['data-map-lat'].strip()

                longitude = listing_html.select('input[id="adMapData"]')
                listing_attrs['longitude'] = longitude[0]['data-map-lon'].strip()
                
                # Some sellers use otomoto built-in feature list, others opt into putting them into description
                # We'll try to get some info on common niceties from both
                try:
                    features = listing_html.select('div.offer-features')[0].text.strip().lower()
                except:
                    features = listing_html.select('div.offer-description__description')[0].text.strip().lower()
                    
                if 'bluetooth' in features:
                    listing_attrs['bluetooth'] = True
                else:
                    listing_attrs['bluetooth'] = False
                if 'carplay' in features:
                    listing_attrs['carplay'] = True
                else:
                    listing_attrs['carplay'] = False
                if 'android' in features:
                    listing_attrs['android'] = True
                else:
                    listing_attrs['android'] = False
                if 'tempomat' in features:
                    listing_attrs['cruisecontrol'] = True
                else:
                    listing_attrs['cruisecontrol'] = False
                if 'kamera parkowania' in features:
                    listing_attrs['camera_rear'] = True
                else:
                    listing_attrs['camera_rear'] = False
                if 'czujnik deszczu' in features:
                    listing_attrs['rain_sensor'] = True
                else:
                    listing_attrs['rain_sensor'] = False

                # Every offer has some mandatory features that can be scraped from this element
                all_params = listing_html.select('li.offer-params__item')

                for param in all_params:
                    key = param.find('span').text
                    try:
                        value = param.find('a')['title']
                    except: 
                        value = param.find('div').text.strip()
                    listing_attrs[key] = value
            except Exception as zonk:
                pass
                #print(f'Couldn\'t parse listing: ' + offer_url + str(zonk))
                
            listings.append(listing_attrs)
    except:
        print('Could not resolve URL contents at all ' + offer_url)

# This is the offer list from previous scraping script
picklefile = 'offer_urls__.pkl'
with open (picklefile,'rb') as pick:
    all_listings = pickle.load(pick)

# Main part, you can experiment how many offers are to be scraped in parallel
# 1k works, 10k doesn't, probably doens't make sense to make it this big anyway

if __name__ == '__main__':
    chunk_size = 128
    imax = int(len(all_listings) / chunk_size) + 1
    for i in range(1, imax + 1):
        print('Chunk: ' + str(i) + ' of ' + str(imax) + ', ' + str(round(i/imax*100, 2)) + '%')
        urls = all_listings[(i-1)*chunk_size:i*chunk_size]
        fetch_async(urls)
        offers = get_dataframe(listings)
        offers.to_feather('offer_dump.feather')
        
        # We could make this rolling to be able to tell progress in case of issues but probably not needed
        #offers_file = 'offers_dump_{0}.feather'
        #offers.to_feather(offers_file.format(i))
        #os.remove(offers_file.format(i-1))
    print('Done')