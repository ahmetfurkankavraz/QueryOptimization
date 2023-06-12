import psycopg2
from pymongo import MongoClient

def connect_postgres():
    connection = psycopg2.connect(
        host="localhost",
        port="5432",
        database="adv_db",
        user="postgres",
        password="1234"
    )
    return connection

def connect_mongodb():
    client = MongoClient("mongodb://localhost:27017")
    return client

def insert_into_mongodb(connection, client):
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM \"Variants\";")
    variants = cursor.fetchall()

    db = client["adv_db"]
    collection = db["Variants"]
    inserted_collection = []

    print("Inserting data into MongoDB...")
    count = 0

    for variant in variants:
        variant_data = {
            "Id": variant[0],
            "PositionStart": variant[1],
            "Length": variant[2],
            "Reference": variant[3],
            "Alternative": variant[4],
            "Type": variant[5],
            "GeneName": variant[6],
            "ClinicalSignificance": variant[7],
            "Disease": variant[8],
            "Synonimity": variant[9],
            "GeneRegion": variant[10],
            "Chromosome": variant[11]
        }
        inserted_collection.append(variant_data)
        count += 1
        if count % 10000 == 0:
            print(f"{count} records inserted into MongoDB.")
    
    collection.insert_many(inserted_collection)

    print("Data inserted into MongoDB successfully.")

# Connect to PostgreSQL
postgres_connection = connect_postgres()

# Connect to MongoDB
mongo_client = connect_mongodb()

# Insert data into MongoDB
insert_into_mongodb(postgres_connection, mongo_client)

# Close the connections
postgres_connection.close()
mongo_client.close()
