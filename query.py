import psycopg2
import pandas as pd 

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
            print(pd.DataFrame(cursor.fetchall()))
        except:
            continue 

executeScriptFromFile('queries.sql')

cursor.close()
connection.close()