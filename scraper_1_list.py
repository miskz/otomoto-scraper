from bs4 import BeautifulSoup
import requests
from time import sleep
import csv
import pickle
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=UserWarning)
warnings.simplefilter(action='ignore', category=DeprecationWarning)

# Script will convert polish dates into datetime values
import locale
locale.setlocale(locale.LC_TIME, "pl_PL")


# Otomoto doesn't allow browsing offers past page 500 so we'll have to partition it
# Car maker with most offers at the time was Volskwagen with 16k which is 500 at 32 results per page so we'll use that
# Since search results will constantly shift as new offers are added it's  best to scrape offer URLs first and scrape contents later
# It's also probably it's best to scrape at night although I've had some issues with stability (not sure if on my end or otomoto)

with open('makes.csv', 'r') as data:
  for line in csv.DictReader(data):
      makes = line

all_listings = []
iteration = 0
limit = 500
make_list = list(makes.values())

def get_searchresults(page, search_url):
    response = requests.get(search_url + '&page=' + str(page))
    search_results = BeautifulSoup(response.text, 'html.parser')
    return search_results

for brand in make_list:
    search_url = 'https://www.otomoto.pl/osobowe/uzywane/{0}?search%5Bfilter_enum_damaged%5D=0'.format(brand)
    print(search_url)

    listings = []
    page = 1
    next_page = True
    prev_results = []
    
    while next_page:
        attempt = 0
        prev_fail = False
        parse_msg = '{0} page {1}'
        
        # Don't attempt to pull pages after limit reached
        if page > limit: 
            break_msg = 'breaking @ {0} page {1}'
            print(break_msg.format(brand, str(page-1)))
            break

        # Sometimes otomoto returns exact same results on next page (even before reaching limit)
        # Retrying couple of times seems to solve the issue, we're also tracking consecutive fails and abort
        # if failed two times in a row
        
        while attempt < 3:
            print(parse_msg.format(brand, page) + ' attempt ' + str(attempt+1))            
            try:
                search_results = get_searchresults(page, search_url)
            except:
                # This helps with any instability in connection or otomoto, not the prettiest way to retry but worked so far
                print('Could not get search results, retrying after 1 min sleep')
                sleep(60)
                search_results = get_searchresults(page, search_url)
                
            results = search_results.select('main[data-testid="search-results"] > article > div > h2 > a')
            
            if results == prev_results:
                sleep(1)
                attempt += 1
                if attempt > 2: prev_fail = True
            else:
                if len(results) > 0:
                    for result in results:
                        try:
                            listing = result['href']
                            listings.append(listing)
                        except:
                            pass
                page += 1
                prev_results = results
                prev_fail = False
                break
        if prev_fail: next_page = False

    all_listings = all_listings + listings
    
# Remove inevitable duplicates from the list    
all_listings = list(dict.fromkeys(all_listings))
print(len(all_listings))

# Save to file for part 2 of the script to pick up and parse
picklefile = 'offer_urls__.pkl'
with open (picklefile, 'wb') as pick:
    pickle.dump(all_listings, pick)