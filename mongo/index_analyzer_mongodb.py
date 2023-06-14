import json
from typing import List
from pymongo import MongoClient

def read_file(file_path: str) -> List[str]:
    with open(file_path, "r") as file:
        lines = file.readlines()
    return [line.strip("\n").strip() for line in lines]

def connect_mongodb() -> MongoClient:
    client = MongoClient("mongodb://localhost:27017")
    return client

def run_query(client: MongoClient, query: str) -> float:
    
    result = client["adv_db"]["Variants"].find(json.loads(query)).explain()
        
    execution_time = result["executionStats"]["executionTimeMillis"]
    return execution_time

# get the query execution statistics from MongoDB
def get_stats(client: MongoClient, queries: List[str]) -> dict:
    query_multiplier = 1
    execution_times = []

    for q_index in range(len(queries)):
        if q_index % 20 == 0:
           print(f"\nStarting Query group {q_index // 20 + 1}\n")
        query = queries[q_index]
        query = query.replace("Variants", "Variants_partition")
        # print(f"Running query: {query}")
        for _ in range(query_multiplier):
            execution_time = run_query(client, query)
            execution_times.append(execution_time)

        if q_index % 20 == 19:
            # Print the average, minimum, and maximum execution times
            print("\n", end="")
            print(f"Minimum query execution time: {min(execution_times)} milliseconds.")
            print(f"Median query execution time: {sorted(execution_times)[len(execution_times) // 2]} milliseconds.")
            print(f"Average query execution time: {sum(execution_times) / len(execution_times)} milliseconds.")
            print(f"Maximum query execution time: {max(execution_times)} milliseconds.")
            print("\n", end="")

            execution_times = []


# Connect to MongoDB
mongo_client = connect_mongodb()

# Read the query statements
queries = read_file("query_mongo.txt")

# Get the query execution statistics
print("Running the query without indexes")
get_stats(mongo_client, queries)

mongo_client.close()
