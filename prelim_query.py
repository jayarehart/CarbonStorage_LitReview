# preliminary query for the literature review



## Import libraries
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from elsapy.elsclient import ElsClient
from elsapy.elsprofile import ElsAuthor, ElsAffil
from elsapy.elsdoc import FullDoc, AbsDoc
from elsapy.elssearch import ElsSearch
import json
import pandas as pd

## ------------------------------------------------
## Import the queries from the google sheets
scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('./google_sheets_api/carbon-storagelLit-review-ff54c3990264.json', scope)
gc = gspread.authorize(creds)

# Import the worksheet and the specific sheet.
wks = gc.open("Search_Terms").worksheet("Queries")

data = wks.get_all_values()
headers = data.pop(0)

df = pd.DataFrame(data, columns=headers)
print(df.head())

## ------------------------------------------------
# Run each query through scopus
# query = 'AUTHOR-NAME(Arehart) AND PUBYEAR > 2019'

def search_my_query(query):
    if type(query) == str:
        ## Load configuration
        con_file = open("config.json")
        config = json.load(con_file)
        con_file.close()

        ## Initialize client
        client = ElsClient(config['apikey'])

        ## Initialize doc search object using Scopus and execute search, retrieving all results
        print('......Searching Scopus......')
        print('......for..... ' + query + ' ....')
        doc_srch = ElsSearch(query, 'scopus')
        doc_srch.execute(client, get_all=True)
        print("doc_srch has", len(doc_srch.results), "results.")

        return doc_srch.results_df
    else:
        print('the query must be a string. no searches run...')


for index, row in df.iterrows():
    query = row['Query']
    query_results_df = search_my_query(query)
    row['Number_of_Articles'] = query_results_df.shape[0]
    print(query_results_df.shape[0])

# query_results = search_my_query(df.Query[0])
#
# print(query_results['dc:title'].head())

## Things to-do
# -  write back to google sheet with the number of articles for each search query
# -  combine dataframe of all articles, ignoring duplicates, including the search query that gave them
# -  get abstracts of each article included in the list and write it back to a new sheet in ght egoogle sheet
# -  finish building out queries in the google sheet.