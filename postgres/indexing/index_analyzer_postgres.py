import psycopg2
from typing import List
from psycopg2.extensions import cursor, connection

def read_file(file_path: str) -> List[str]:
    with open(file_path, "r") as file:
        lines = file.readlines()
    return [line.strip("\n").strip() for line in lines]

def connect_database() -> connection:
    connection = psycopg2.connect(
        host="localhost",
        port="5432",
        database="adv_db",
        user="postgres",
        password="1234"
    )
    return connection

def run_query(cursor: cursor, query: str) -> float:
    
    cursor.execute("EXPLAIN ANALYZE " + query)
    execution_time = float(cursor.fetchall()[-1][0].split(" ")[2])
    return execution_time

def get_stats(cursor: cursor, queries: List[str]) -> None:
    query_multiplier = 3
    execution_times = []

    for q_index in range(len(queries)):
        if q_index % 20 == 0:
           print(f"\nStarting Query group {q_index // 20 + 1}")
        query = queries[q_index]
        for _ in range(query_multiplier):
            execution_time = run_query(cursor, query)
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


if __name__ == "__main__":
  # Read the index statements
  index_statements = read_file("indexes.txt")

  # Read the query statements
  queries = read_file("query.txt")

  # Connect to the database
  connection = connect_database()

  print( "Program started...\n")

  # Print the stats without indexes
  print("Running the queries without indexes")
  cursor = connection.cursor()
  get_stats(cursor, queries)
  cursor.close()
  connection.close()

  print("\n----------------------------------------\n")

  # Print the stats with indexes
  print("Running the queries with indexes")
  execution_times_without_indexes = []
  for indexes in index_statements:
    
    connection = connect_database()
    cursor = connection.cursor()

    print(f"Creating the index...")
    # Create the indexes
    for index in indexes.split(";"):
      index = index.strip()
      if index != "":
        print(f"Creating the index: {index}")
        cursor.execute(f"{index}")

    print("Running the queries") 
          
    get_stats(cursor, queries)

    connection.commit()
    cursor.close()
    connection.close()

    print("\n----------------------------------------\n")