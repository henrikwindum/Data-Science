import numpy as np
import pandas as pd 
import psycopg2

# Read dataset
data = pd.read_csv("news_sample.csv", encoding='utf-8')
data['content'] = data['content'].str.strip().str.normalize('NFC')
data['title'] = data['title'].str.strip().str.normalize('NFC')

# Make connection to database
connection = psycopg2.connect(
    user = "athanar",
    password = " ",
    host = "localhost",
    port = "5432",
    database = "datascience")
connection.set_client_encoding('UTF8')
cursor = connection.cursor()

# Drop tables
dropsql = "DROP TABLE IF EXISTS types, article, keyword, tags, urls, author, writtenby CASCADE"
cursor.execute(dropsql)
cursor.execute("SET CLIENT_ENCODING TO 'utf-8';")
connection.commit()

# Read SQL file
def executeScriptsFromFile(filename):
    # Open and read the file as a single buffer
    fd = open(filename, 'r')
    sqlFile = fd.read()
    fd.close()
    # all SQL commands (split on ';')
    sqlCommands = sqlFile.split(';')
    # Execute every command from the input file
    for command in sqlCommands:
        try:
            cursor.execute(command)
        except:
            print("Command skipped: ", command)

executeScriptsFromFile('script1.sql')

# Inserts dataframe into table
def insertTable(cols, values, target):
    for i,row in values.iterrows():
        sql = u"INSERT INTO "+target+" (" +cols + ") VALUES (" + "%s,"*(len(row)-1) + "%s)"
        cursor.execute(sql, tuple(row))
    
    connection.commit()
    

types = data.iloc[:,[3]].drop_duplicates()
typeval = "typeValue"
insertTable(typeval, types, "Types")

article = data.iloc[:,[1, 5, 6, 7, 8, 9, 15]]
articleinfo = "articleID, content, scrapedAt, insertedAt, lastUpdatedAt, title, summary"
insertTable(articleinfo, article, "Article")
print(article.iloc[0])
cursor.execute("SELECT * from Article where articleID = 141;")
print(cursor.fetchall())
cursor.close()
connection.close()