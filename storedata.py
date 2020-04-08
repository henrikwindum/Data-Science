import numpy as np
import pandas as pd 
import psycopg2

reader = pd.read_csv("news_sample.csv", encoding='utf-8')
connection = psycopg2.connect(user = "postgres",
                                      password = " ",
                                      host = "localhost",
                                      port = "5432",
                                      database = "datascience")
article = reader.iloc[:,[1,3,5,6,7,8,9,15]].copy()


# Function to access the database locally, and execute a query
# Make sure to change the username, databse and password
def execQuery(query):
    try:
        connection = psycopg2.connect(user = "athanar",
                                      password = " ",
                                      host = "localhost",
                                      port = "5432",
                                      database = "datascience")
        cursor = connection.cursor()
        cursor.execute(query)
        record = cursor.fetchall()
        return record
    except (Exception, psycopg2.Error) as error :
        connection = False
        print ("Error while connecting to PostgreSQL", error)
    finally:
        if(connection):
            cursor.close()
            connection.close()
            print("Executed query and closed connection.")

connection = psycopg2.connect(
                            user = "athanar",
                            password = " ",
                            host = "localhost",
                            port = "5432",
                            database = "datascience")
cursor = connection.cursor()

cols = "articleID, types, content, scrapedAtTimeID, insertedAtTimeID, createdAtTimeID, title, summary"
""" for i,row in article.iterrows():
    sql = "INSERT INTO Article (" +cols + ") VALUES (" + "%s,"*(len(row)-1) + "%s)"
    cursor.execute(sql, tuple(row))
    connection.commit() """