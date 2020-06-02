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
import itertools
import numpy as np
from pybliometrics.scopus import AbstractRetrieval
from datetime import datetime

## ------------------------------------------------
## Import the queries from the google sheets
scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('./google_sheets_api/carbon-storagelLit-review-ff54c3990264.json', scope)
gc = gspread.authorize(creds)

# Import the worksheet and the specific sheet.
def read_gsheet(Workbook="Search_Terms", Worksheet="Queries"):
    wks = gc.open(Workbook).worksheet(Worksheet)
    data = wks.get_all_values()
    headers = data.pop(0)
    df = pd.DataFrame(data, columns=headers)
    return(df)


## ------------------------------------------------
## Build the queries from the wordbank
query_build_df = read_gsheet(Workbook="Search_Terms", Worksheet="Word_Bank")
a = [list(query_build_df['Scales']), list(query_build_df['Mechanism']), list(query_build_df['Accounting'])]
combos_list = list(itertools.product(*a))

# create combinations of words from the word bank
searches_list = []
for element in combos_list:
    if element[0] != '' and element[1] != '' and element[2] != '':
        print(element[0] + ' AND ' + element[1] + ' AND ' + element[2])
        searches_list.append('ABS(' + element[0] + ' AND ' + element[1] + ' AND ' + element[2] + ')')
        # searches_list.append('ABS(' + element[0] + ' AND ' + element[1] + ' AND ' + element[2] + ')')
        # searches_list.append('ABS(' + element[0] + ' AND ' + element[1] + ' AND ' + element[2] + ') AND PUBYEAR > 2010')

col1 = np.linspace(0, len(searches_list)-1, len(searches_list))
searches_array = np.array(searches_list)
searches_array_2d = np.vstack((col1, searches_array)).T
# Write back to queries google sheet
# worksheet = gc.open("Search_Terms").worksheet("List_of_Queries")
# worksheet.update('A:B', searches_array_2d.tolist())
## ------------------------------------------------


def search_my_query(my_query):
    '''
    Function to search a query in scopus
    :param my_query: string of query desired to be searched in scopus
    :return: resultant dataframe with query from scopus
    '''
    if type(my_query) == str:
        ## Load configuration
        con_file = open("config.json")
        config = json.load(con_file)
        con_file.close()

        ## Initialize client
        client = ElsClient(config['APIKey'])

        ## Initialize doc search object using Scopus and execute search, retrieving all results
        print('......Searching Scopus......')
        print('......for..... ' + query + ' ....')
        doc_srch = ElsSearch(query, 'scopus')
        doc_srch.execute(client, get_all=True)
        print("doc_srch has", len(doc_srch.results), "results.")

        return doc_srch.results_df
    else:
        print('the query must be a string. no searches run...')
        return

# Get list of queries to search for.
# queries_df = read_gsheet(Workbook="Search_Terms", Worksheet="Queries")

# Load search results
re_search_scopus = False
if re_search_scopus == True:
    # Search Scopus
    queries_df = pd.DataFrame({'Query':searches_list,'num_results':None})
    master_df = pd.DataFrame()
    num_results = []
    for index, row in queries_df.iterrows():
        print(str(index) + ' / ' + str(len(queries_df)))
        query = row['Query']
        query_results_df = search_my_query(query)
        row['Number_of_Articles'] = query_results_df.shape[0]
        queries_df['num_results'][index] = query_results_df.shape[0]
        # num_results.append(query_results_df.shape[0])

        print(query_results_df.shape[0])
        master_df = master_df.append(query_results_df)
        # master_df = master_df.drop_duplicates(subset='prism:doi')
        master_df['@_fa'].rename('fa')
        master_df

        # save raw search results
        master_df.to_csv('./search_results.csv')
    else:
        ## Code block if need to retain the raw search results
        master_df = pd.read_csv('./search_results.csv')
        master_df['prism:coverDate'] = master_df['prism:coverDate'].astype('datetime64[ns]')

## Filter data before sending it to google sheets
# drop duplicate search returns
master_df_2 = master_df.drop_duplicates(subset='prism:doi', keep="first")
test = master_df[master_df.duplicated(subset='prism:doi')]

# only get "Journal" result type
master_df_2 = master_df_2[master_df_2['prism:aggregationType'] == "Journal"]
# get publication year
master_df_2['PUBYEAR'] = master_df_2['prism:coverDate'].dt.year
# filter only publication year of interest
master_df_2 = master_df_2[master_df_2['PUBYEAR'] >= 2010]

# Functions to write pandas dataframe back to google sheet
def iter_pd(df):
    for val in df.columns:
        yield val
    for row in df.to_numpy():
        for val in row:
            if pd.isna(val):
                yield ""
            else:
                yield val

def pandas_to_sheets(pandas_df, sheet, clear = True):
    # Updates all values in a workbook to match a pandas dataframe
    if clear:
        sheet.clear()
    (row, col) = pandas_df.shape
    cells = sheet.range("A1:{}".format(gspread.utils.rowcol_to_a1(row + 1, col)))
    for cell, val in zip(cells, iter_pd(pandas_df)):
        cell.value = val
    sheet.update_cells(cells)

# write master_df back to google sheets
to_sheets = master_df_2[['dc:identifier', 'dc:title', 'dc:creator', 'prism:publicationName', 'prism:coverDisplayDate', 'prism:doi', 'citedby-count', 'prism:aggregationType', 'PUBYEAR']]
worksheet = gc.open("Search_Terms").worksheet("Query_Results")
pandas_to_sheets(to_sheets, worksheet)

# write queries back to google sheets
worksheet = gc.open("Search_Terms").worksheet("Queries")
pandas_to_sheets(queries_df, worksheet)



# ## Load configuration
# con_file = open("config.json")
# config = json.load(con_file)
# con_file.close()
#
# ## Initialize client
# client = ElsClient(config['APIKey'])
# ## Get abstracts for searched documents
# ab = AbstractRetrieval("10.1016/j.enbuild.2018.02.042")




## Things to-do
# -  get abstracts of each article included in the list and write it back to a new sheet in the google sheet
