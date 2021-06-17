# Databricks notebook source
import pyspark
import pandas as pd
from pyspark.sql import SparkSession

# COMMAND ----------

# Save data to blob storage
import os, uuid
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
connect_str=dbutils.secrets.get(scope='keyvault-managed',key="comicuniverse-key1-conn-str")
container_name="load"
# Create the BlobServiceClient object which will be used to create a container client
blob_service_client = BlobServiceClient.from_connection_string(connect_str)

# COMMAND ----------

try:
  os.mkdir('/dbfs/data_comics')
except Exception:
  pass
blob_client = blob_service_client.get_blob_client(container=container_name, blob="marvel_characters.csv")
with open("/dbfs/data_comics/marvel_characters.csv", "wb+") as download_file:
    download_file.write(blob_client.download_blob().readall())
blob_client = blob_service_client.get_blob_client(container=container_name, blob="dc_characters.csv")
with open("/dbfs/data_comics/dc_characters.csv", "wb") as download_file:
    download_file.write(blob_client.download_blob().readall())

# COMMAND ----------

# Create dataframe
df_mar = pd.read_csv('/dbfs/data_comics/marvel_characters.csv', sep='\|', index_col=0, error_bad_lines=False, engine='python', encoding='utf-8').dropna()
df_mar['Universe'] = 'Marvel'

df_dc = pd.read_csv('/dbfs/data_comics/dc_characters.csv', sep='\|', index_col=0, error_bad_lines=False, engine='python', comment=';', encoding='utf-8').dropna()
df_dc['Universe'] = 'DC'

df = pd.concat([df_mar,df_dc], axis=0).reset_index().drop(columns='index')
df['WikiLink'].replace(" ", "_", inplace=True, regex=True)

# COMMAND ----------

import re
import json 
import urllib.request
from tqdm import tqdm     #used to time loops
import collections
import os
import nltk
from nltk import word_tokenize
import pprint
import networkx as nx
from nltk.corpus import stopwords
from datetime import datetime
import pickle
import shutil

# COMMAND ----------

# SHould be made parallelle or async
baseurl = 'https://en.wikipedia.org/w/api.php?'
action = 'action=query'
title = 'titles='
content = 'prop=revisions&rvprop=content'
dataformat = 'format=json'

wiki_strings = []
for idx, row in tqdm(df.iterrows()):
    query = '%s%s&%s&%s&%s' % (baseurl,action,f'titles={urllib.parse.quote(row.WikiLink)}',content,dataformat)
    wiki_strings.append(urllib.request.urlopen(query).read().decode('utf-8'))

df['wiki_string_raw'] = pd.Series(wiki_strings)

# COMMAND ----------

nltk.download('stopwords')
stop_words = set(stopwords.words('english'))
tokenizer = nltk.RegexpTokenizer(r'\w+')

def parse_wiki_data(row, process=True):
    wikidict = json.loads(row)
    page_id = list(wikidict['query']['pages'].keys())[0]
    
    if page_id == '-1':
        return None
    
    else:
        raw_text = wikidict['query']['pages'][page_id]['revisions'][0]['*']
        if not process:
          return raw_text
        processed=tokenizer.tokenize(" ".join([raw_text.lower()]))
        return " ".join([x for x in processed if x not in stop_words])
    

# COMMAND ----------

df['wiki_string_processed'] = df['wiki_string_raw'].apply(parse_wiki_data)
df['wiki_sentiment'] = df['wiki_string_raw'].apply(parse_wiki_data,args=(False,))

# COMMAND ----------

# UPLOAD DATA
"""
UPLOAD DATA
We will save
- Text response from API
"""

local_path = "/dbfs/data_comics"
try:
  os.mkdir(local_path)
except OSError:
  pass

for idx, row in df.iterrows():
  # Create a file in the local data directory to upload and download
  local_file_name = "wiki__"+row.CharacterName + row.Universe + datetime.today().strftime('%Y_%m_%d') + ".txt"
  upload_file_path = os.path.join(local_path, local_file_name)
  # Write text to the file
  file = open(upload_file_path, 'w')
  file.write(row.wiki_string_raw)
  file.close()
  # Create a blob client using the local file name as the name for the blob
  blob_client = blob_service_client.get_blob_client(container=container_name, blob=local_file_name)
  if idx%15==0:
    print("\nUploading to Azure Storage as blob:\n\t" + local_file_name)
  # Upload the created file
  with open(upload_file_path, "rb") as data:
      blob_client.upload_blob(data)
      
# Cleanup
shutil.rmtree(local_path)

# COMMAND ----------

### Transformation of data
# Load data from previous phase
# TODO: Ensure that it loads correctly
"""
Transformation part
* We will create a couple of dataframes/tables here that will be loaded into an SQL server
- One table which is going to be used to model the social graph - Table 1
- One that we have the primary data on a char
  - How many connections
  - Connections in DC
  - Connections in Marvel
  - Total sentiment
  - Total rank (how many incoming connections compared to others)
- Sentiment table - Contains all information on the processing of the data
"""

# COMMAND ----------

import json
import requests
key = dbutils.secrets.get("keyvault-managed","cognitive-sentiment")
endpoint = "https://northeurope.api.cognitive.microsoft.com/"

def create_payload(list_of_docs):
  return {"documents":[{"language": "en",
      "id": idx, "text": x} for idx,x in enumerate(list_of_docs,1)]}
## Obtain the sentiment data
documents=[x[1000:6000] if x else '' for x in df.wiki_sentiment.values]
res = []
i=10
while i<len(documents):
  print(i)
  response = requests.post("https://northeurope.api.cognitive.microsoft.com/text/analytics/v3.0/sentiment", headers={"Content-Type":"application/json",
                                                                                                      "Ocp-Apim-Subscription-Key":key}, data=json.dumps(create_payload(documents[i-10:i])))
  try:
    response.raise_for_status()
    res+=response.json()['documents']
  except Exception:
    print("failed")
  i+=10

# COMMAND ----------

local_path = "/dbfs/data_comics"
container_name="transform"
try:
  os.mkdir(local_path)
except OSError:
  pass
ii=0
for ele in res:
  name=df.iloc[ii].CharacterName
  ii+=1
  # Create a file in the local data directory to upload and download
  local_file_name = "Api_sentiment_Prod_"+ name +"_" + datetime.today().strftime('%Y_%m_%d') + ".json"
  upload_file_path = os.path.join(local_path, local_file_name)
  # Write text to the file
  file = open(upload_file_path, 'w')
  file.write(json.dumps(ele))
  file.close()
  # Create a blob client using the local file name as the name for the blob
  blob_client = blob_service_client.get_blob_client(container=container_name, blob=local_file_name)
  if ii%15==0:
    print("\nUploading to Azure Storage as blob:\n\t" + local_file_name)
  # Upload the created file
  with open(upload_file_path, "rb") as data:
      try:
        blob_client.upload_blob(data)
      except Exception:
        pass
# Cleanup
shutil.rmtree(local_path)

# COMMAND ----------

df = pd.concat([df,pd.DataFrame(columns=["sentiment_neutral", "sentiment_positive", "sentiment_negative", "Document_Sentiment"])])
i=0
for ele in res:
  df.loc[i,"sentiment_neutral"]=ele.get("confidenceScores").get("neutral")
  df.loc[i,"sentiment_positive"]=ele.get("confidenceScores").get("negative")
  df.loc[i,"sentiment_negative"]=ele.get("confidenceScores").get("positive")
  df.loc[i,"Document_Sentiment"]=ele.get("sentiment")
  i+=1

# COMMAND ----------

## Add the GRAPH data to dataframe and upload to transformed bucket
## Easy

# COMMAND ----------

# Create graph
char_linked_list = [] #list with lists of links for each page
for wiki_idx in tqdm(range(len(wiki_strings))):
    char_linked_list.append([list(character_links.split('|')[0] for character_links in re.findall(r'\[\[([^\]]+)\]\]', wiki_strings[wiki_idx]))])

#The links are now matched to other characters in the dataframe, in order to identify which characters have links to other characters
relevant_char_linked_list = []
for link_list in tqdm(range(len(char_linked_list))):
    char_list_matched = []
    if not isinstance(char_linked_list[link_list][0], list):
        continue
    for link in range(len(char_linked_list[link_list][0])):
        if ("[[") in char_linked_list[link_list][0][link] or len(char_linked_list[link_list][0][link]) == 1: #handling potential errors
            continue
        if df['WikiLink'].str.match(char_linked_list[link_list][0][link].replace(" ", "_")).sum() > 0:
            char_list_matched.append(char_linked_list[link_list][0][link].replace(" ", "_"))
    relevant_char_linked_list.append(list(set(char_list_matched)))

# COMMAND ----------

G = nx.DiGraph()

for character in range(len(relevant_char_linked_list)):
    for char_link in range(len(relevant_char_linked_list[character])):
      if relevant_char_linked_list[character][char_link] in list(df['WikiLink']): 
        G.add_edge(df['WikiLink'].iloc[character], relevant_char_linked_list[character][char_link])
    G.add_node(df['WikiLink'].iloc[character], Universe=df['Universe'].iloc[character])

# COMMAND ----------

local_path = "/dbfs/data_comics"
try:
  os.mkdir(local_path)
except OSError:
  pass
container_name_transform="transform"
local_file_name = "UniverseGraph_Prod" + datetime.today().strftime('%Y_%m_%d') + ".txt"
upload_file_path = os.path.join(local_path, local_file_name)
file = open(upload_file_path, 'wb')
pickle.dump(G, file)
file.close()
blob_client = blob_service_client.get_blob_client(container=container_name_transform, blob=local_file_name)
with open(upload_file_path, "rb") as data:
      blob_client.upload_blob(data)
shutil.rmtree(local_path)

# COMMAND ----------

blob_client = blob_service_client.get_blob_client(container=container_name_transform, blob="UniverseGraph_Prod2021_06_16.txt")
with open("/dbfs/UniverseGraph2021_06_13.txt", "wb") as download_file:
    download_file.write(blob_client.download_blob().readall())
G = pickle.load(open('/dbfs/UniverseGraph2021_06_13.txt', 'rb'))

# COMMAND ----------

# DF two with associate information
degree = []
for n, d in G.out_degree():
  degree.append((n,G.in_degree()[n],d))
degree = pd.DataFrame(degree, columns=["Charater","in_degree","out_degree"])

# COMMAND ----------

from pyspark.sql.types import *
# Connection properties
username = '4dm1n157r470r' 
password = '4-v3ry-53cr37-p455w0rd' 

jdbcUrl = "jdbc:sqlserver://comic-sqlserver.database.windows.net:1433;database=comic"
connectionProperties = {
 "user" : dbutils.secrets.get(scope='keyvault-managed',key="comic-sqlserver-username"),
 "password" : dbutils.secrets.get(scope='keyvault-managed',key="comic-sqlserver-password"),
 "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}


# Auxiliar functions
def equivalent_type(f):
    if f == 'datetime64[ns]': return TimestampType()
    elif f == 'int64': return LongType()
    elif f == 'int32': return IntegerType()
    elif f == 'float64': return FloatType()
    else: return StringType()

def define_structure(string, format_type):
    try: typo = equivalent_type(format_type)
    except: typo = StringType()
    return StructField(string, typo)

# Given pandas dataframe, it will return a spark's dataframe.
def pandas_to_spark(pandas_df):
    columns = list(pandas_df.columns)
    types = list(pandas_df.dtypes)
    struct_list = []
    for column, typo in zip(columns, types): 
      struct_list.append(define_structure(column, typo))
    p_schema = StructType(struct_list)
    return sqlContext.createDataFrame(pandas_df, p_schema)

# COMMAND ----------

"""
Graph end - Text only
"""

# Marvel v DC

# Identifying and extracting the giant connected component
largest_connected_component = max(nx.weakly_connected_components(G), key=len)
Gc = G.subgraph(largest_connected_component)       # Extracting the largest connected component as Gc
print('number of components in digraph: ', len(G.nodes))
print('number of components in giant component subgraph: ', len(Gc.nodes))
print('number of edges in digraph: ', len(G.edges))
print('number of edges in giant component subgraph: ', len(Gc.edges))

# COMMAND ----------

# Graph visualisation
_res= []
k=set([x for x in degree.sort_values("in_degree",ascending=False)[0:20].Charater.values])
for ele in list(Gc.edges):
  try:
    to_weight=5
    if ele[1] in k:
      to_weight=20
    if G.nodes[ele[0]]['Universe']!=G.nodes[ele[1]]['Universe']:
      if G.nodes[ele[0]]['Universe']=="DC":
        _res.append(["Cross", ele[0], ele[1], "#FFFF00", 20, "#000000", "#CB1E1E", to_weight])
      else:
        _res.append(["Cross",ele[0], ele[1], "#FFFF00", 20, "#CB1E1E", "#000000", to_weight])
    else:
      if G.nodes[ele[0]]['Universe']=="DC":
        _res.append(["Intra", ele[0], ele[1], "#000000", 5, "#000000", "#000000", to_weight])
      else:
        _res.append(["Intra", ele[0], ele[1],"#CB1E1E", 5, "#CB1E1E", "#CB1E1E", to_weight])
  except Exception:
    pass
  
graph_df = pd.DataFrame(_res)
graph_df=graph_df.rename(columns={0: 'Universe', 1: 'from', 2: 'to',3: 'Edgecolor', 4: 'EdgeWeight',5: 'SourceColor',6: 'TargetColor', 7: 'TargetWeight'})
graph_df["SourceWeight"]=20
graph_df = graph_df[graph_df['from'].apply(lambda x: True if x in k else False)]

# COMMAND ----------

local_path = "/dbfs/data_comics"
try:
  os.mkdir(local_path)
except OSError:
  pass

local_file_name = "WikiDataframe_prod" + datetime.today().strftime('%Y_%m_%d') + ".txt"
upload_file_path = os.path.join(local_path, local_file_name)
file = open(upload_file_path, 'wb')
pickle.dump(df, file)
file.close()
blob_client = blob_service_client.get_blob_client(container=container_name_transform, blob=local_file_name)
with open(upload_file_path, "rb") as data:
      blob_client.upload_blob(data)

# COMMAND ----------

blob_client = blob_service_client.get_blob_client(container=container_name_transform, blob="WikiDataframe_prod2021_06_16.txt")
with open("/dbfs/WikiDataframe_prod2021_06_16.txt", "wb") as download_file:
    download_file.write(blob_client.download_blob().readall())
df = pickle.load(open('/dbfs/WikiDataframe_prod2021_06_16.txt', 'rb'))

# COMMAND ----------

df_db=pandas_to_spark(df)
degree_db=pandas_to_spark(degree)
graph_df_db = pandas_to_spark(graph_df)

# COMMAND ----------

df_db.write.jdbc(url=jdbcUrl, table="CharacterData", properties=connectionProperties)
degree_db.write.jdbc(url=jdbcUrl, table="CharacterDegree", properties=connectionProperties)
graph_df_db.write.jdbc(url=jdbcUrl, table="CharacterGraph", properties=connectionProperties)