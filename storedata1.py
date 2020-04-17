import numpy as np
import pandas as pd 
import psycopg2, time
from cleanup import clean

# Variables:
# Dicts
tags = {}
authordict = {}

# Sets
new_keywords = set()
new_domains = set()
new_authors = set()

# Lists 
new_types = []

# Read dataset
start_time = time.time()
reader = pd.read_csv(
    "1mio-raw.csv", 
    encoding='utf-8', 
    chunksize=10000)

# Make connection to database
connection = psycopg2.connect(
    user = "athanar",
    host = "localhost",
    port = "5432",
    database = "datascience")
connection.set_client_encoding('UTF8')
cursor = connection.cursor()

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
executeScriptFromFile('create_tables.sql')

# Inserts dataframe into database
def insertTable(cols, vals, target):
    sql = "INSERT INTO "+target+" (" +cols + ") VALUES (" + "%s,"*(len(vals.iloc[0])-1) + "%s)"
    cursor.executemany(sql, vals.values.tolist())
    connection.commit()

# Get typeid for type string
def typeLookup(typeval):
    if (isinstance(typeval, float)):
        return 12
    else:
        return new_types.index(typeval) + 1

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
    data['title'] = data['title'].apply(clean)
    data['summary'] = data['summary'].apply(clean)

    # Fetches article from dataframe
    article = data.iloc[:,[1,9,5,15,6,7,8]]
    articleval = "articleID, title, content, summary, scrapedAt, insertedAt, updatedAt"
    insertTable(articleval, article, "Article")
    
    # Fetches types from dataframe, done like this as the first round 
    # finds all relevant types 
    if (len(new_types) < 1):
        types = data['type'].drop_duplicates().dropna()
        typeframe = pd.DataFrame(types).rename(columns={'type':'typeValue'})
        new_types = list(types)
        insertTable("typeValue", pd.DataFrame(types), "Types") 

    # Fills Typelinks
    articleid = data.iloc[:,[1]]
    typeid = data['type'].apply(typeLookup)
    typelinks = pd.concat([articleid, typeid], axis=1, ignore_index=True)
    insertTable("articleID, typeID", typelinks, "Typelinks")

    # Fetches keywords from dataframe and inserts new keywords
    keywords = extractParts(tags, data['id'],data['meta_keywords'])
    keyword_list = list(keywords.difference(new_keywords))
    new_keywords = new_keywords.union(keywords)  
    insertTable("keywordValue", pd.DataFrame(keyword_list), "Keyword")

    # Fetches domain from dataframe and inserts new domains
    domain = set(data.loc[:,'domain'])
    domain_list = list(domain.difference(new_domains))
    new_domains = new_domains.union(domain)
    insertTable("domainURL", pd.DataFrame(domain_list), "Domain")

    # Fetches webpageURL from dataframe and inserts
    dom_list = list(new_domains)
    new_webs = data['domain'].apply(lambda x: dom_list.index(x)+1)
    dom_frame = pd.DataFrame(
        {'id': data['id'], 'domain': new_webs, 'url': data['url']})
    insertTable("articleID, domainID, webpageurl", dom_frame, "Webpage")

    # Fetches authors from dataframe and inserts new authors
    authors = extractParts(authordict, data['id'], data['authors'])
    author_list = list(authors.difference(new_authors))
    new_authors = new_authors.union(authors)
    insertTable("authorName", pd.DataFrame(author_list), "Author")

    # Round counter for timing
    print("Round %d took %s seconds" % (i,time.time() - start_time))
    i = i+1

# Inserts tags into tag table
keyword_listform = list(new_keywords)
tmp_lst_kw = []
for k, v in tags.items():
    for kword in v:
        tmp_lst_kw.append([k, keyword_listform.index(kword)+1])
insertTable("articleID, keywordID", pd.DataFrame(tmp_lst_kw), "Tags")
print("Keywords. Took %s seconds" % (time.time() - start_time))

# Inserts into WrittenBy table
author_listform = list(new_authors)
tmp_lst_aut = []
for k, v in authordict.items():
    for kword in v:
        tmp_lst_aut.append([k, author_listform.index(kword)+1])
insertTable("articleID, authorID", pd.DataFrame(tmp_lst_aut), "WrittenBy")

cursor.close()
connection.close()
print("Finished. Took %s seconds" % (time.time() - start_time))