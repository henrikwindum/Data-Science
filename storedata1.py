import numpy as np
import pandas as pd 
import psycopg2, time
from cleanup import clean

# Variables:
# Dicts
tags = {}
authordict = {}

# DataFrames
new_webpages = pd.DataFrame(columns=['domain'])

# Sets
new_keywords = set()
new_domains = set()
new_authors = set()

# Lists 
new_types = []

# Read dataset
start_time = time.time()
reader = pd.read_csv("1mio-raw.csv", encoding='utf-8', chunksize=10000)

# Make connection to database
connection = psycopg2.connect(user = "athanar",
                              host = "localhost",
                              port = "5432",
                              database = "datascience")
connection.set_client_encoding('UTF8')
cursor = connection.cursor()

# Drop tables
dropsql = "DROP TABLE IF EXISTS types, article, keyword, tags, webpage, author, writtenby, domain CASCADE;"
cursor.execute(dropsql)

# Read SQL file
def executeScriptFromFile(filename):
    fd = open(filename, 'r')
    sqlFile = fd.read()
    fd.close()
    sqlCommands = sqlFile.split(';')
    for command in sqlCommands:
        try:
            cursor.execute(command)
        except:
            continue  
executeScriptFromFile('script1.sql')

# Inserts dataframe into database
def insertTable(cols, values, target):
    for i, row in values.iterrows():
        sql = u"INSERT INTO "+target+" (" +cols + ") VALUES (" + "%s,"*(len(row)-1) + "%s)"
        cursor.execute(sql, tuple(row)) #TODO: executemany instead
    connection.commit()

# Get typeid for type string
def typeLookup(typeval):
    typeval = new_types.loc[new_types['type'] == typeval].index[0]

# Extract comma separated parts of string column
def extractParts(dict, ids, column):
    tmp = []
    for i in range(len(column)):
        if (isinstance(column.iloc[i], float)):
            tmp.extend(str(column.iloc[i]))
            dict[ids.iloc[i]] =  str(column.iloc[i])
        elif (column.iloc[i] == "[\'\']"):
            continue
        else:
            new_vals = (column.iloc[i]
                        .replace('[', '')
                        .replace(']', '')
                        .replace('\'', '')
                        .replace('\"', '')
                        .lower()
                        .split(', '))
            tmp.extend(new_vals)
            dict[ids.iloc[i]] = new_vals
    return set(tmp)

i = 1
for data in reader:
    # Size; Highly temporary for testing purposes
    if (i >= 3):
        break

    # Clean data
    #data['content'] = data['content'].apply(clean)
    #data['title'] = data['title'].apply(clean)

    # Fetches article from dataframe
    article = data.iloc[:,[1,9,5,15,6,7,8]]
    articleval = "articleID, title, content, summary, scrapedAt, insertedAt, updatedAt"
    insertTable(articleval, article, "Article")

    # Fetches types from dataframe
    if (len(new_types) < 1):
        types = data['type'].drop_duplicates().dropna()
        new_types = list(types)
        insertTable("typeValue", pd.DataFrame(types), "Types")    

    # Fills Typelinks
    articleid = data.iloc[:,[1]].reset_index(drop=True)
    new_list = []
    links = data.iloc[:,[3]].fillna("unknown")
    for j in range(10000):
        value = data.iloc[j,[3]][0]
        try:
            new_list.append(new_types.loc[new_types['type'] == value].index[0])
        except:
            new_list.append(11)
            continue
    new_df = pd.DataFrame(new_list, columns=['typeid'])
    new_df2 = data['type']
    concated = pd.concat([articleid, new_df], axis=1)
    insertTable("articleID, typeID", concated, "Typelinks")

    # Fetches keywords from dataframe and inserts into set
    keywords = extractParts(tags, data['id'],data['meta_keywords'])
    new_keywords = new_keywords.union(keywords)  

    # Fetches domain from dataframe
    domain = set(data.loc[:,'domain'])
    domain_list = list(domain.difference(new_domains))
    new_domains = new_domains.union(domain)
    insertTable("domainURL", pd.DataFrame(domain_list), "Domain")

    # Fetches webpageURL from dataframe
    dom_list = list(new_domains)
    new_webs = data['domain'].apply(lambda x: dom_list.index(x)+1)
    dom_frame = pd.DataFrame({'id': data['id'], 'domain': new_webs, 'url': data['url']})
    insertTable("articleID, domainID, webpageurl", dom_frame, "Webpage")

    # Fetches authors from dataframe
    authors = extractParts(authordict, data['id'], data['authors'])
    new_authors = new_authors.union(authors)

    # Round counter for timing
    print("Round %d took %s seconds" % (i,time.time() - start_time))
    i = i+1

# Inserts keywords into DB
keyword_list = list(new_keywords)
insertTable("keywordValue", pd.DataFrame(keyword_list), "Keyword")

# Inserts tags into tag table
for k, v in tags.items():
    tmp_lst = []
    for kword in v:
        tmp_lst.append([k, keyword_list.index(kword)+1])
    insertTable("articleID, keywordID", pd.DataFrame(tmp_lst), "Tags")

# Inserts author into authors table
author_list = list(new_authors)
insertTable("authorName", pd.DataFrame(author_list), "Author")

# Inserts into WrittenBy table
for k, v in authordict.items():
    tmp_lst = []
    for kword in v:
        tmp_lst.append([k, author_list.index(kword)+1])
    insertTable("articleID, authorID", pd.DataFrame(tmp_lst), "WrittenBy")

""" cursor.execute("select * from domain where domainid < 5")
print(cursor.fetchall()) """
print("Done")