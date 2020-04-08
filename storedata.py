import numpy as np
import pandas as pd 
import psycopg2

# Read dataset
data = pd.read_csv("news_sample.csv", encoding='utf-8')
data['content'] = data['content'].astype('str')
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
        # This will skip and report errors
        # For example, if the tables do not yet exist, this will skip over
        # the DROP TABLE commands
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

article = data.loc[:,['id', 'summary', 'content', 'title', 'inserted_at', 'updated_at', 'scaped_at']]
articleinfo = "articleID, summary, content, title, insertedAt, lastUpdatedAt, scrapedAt"
insertTable(articleinfo, article, "Article")


