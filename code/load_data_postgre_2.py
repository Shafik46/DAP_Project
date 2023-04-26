from pymongo import MongoClient
import pandas as pd
import psycopg2

# Connect to the MongoDB instance
print("Connecting to MongoDB....")
mongo_client = MongoClient('mongodb+srv://dap_project:password1234@cluster0.ggbgljh.mongodb.net/test')
mongo_db = mongo_client['dap_project']
mongo_collection = mongo_db['EV_Registration']
print("Retrieving JSON data from MongoDB....")
# Retrieve the JSON data from the MongoDB collection
mongo_data = list(mongo_collection.find())

# Convert the JSON data to a Pandas DataFrame
EV_df = pd.DataFrame(mongo_data)
EV_df.drop('_id', axis=1, inplace=True) # Dropping unwanted column

try:
    # connect to the PostgreSQL database
    print("Connecting to Postgre Server")
    conn = psycopg2.connect(
        host="postgres",
        database="DAP_Project",
        user="airflow",
        password="airflow")
    print("Connected to Postgre Server !!!")

    # write the dataframe to a CSV file
    EV_df.to_csv('data.csv', index=False, header=False)
    count = len(EV_df)

    # open the CSV file and copy its contents to a PostgreSQL table
    print("Inserting data...")
    with open('data.csv', 'r') as f:
        cursor = conn.cursor()
        cursor.copy_expert(
            f'''COPY "DAP_Project"."Electric_Vehicles".ev_registration FROM STDIN WITH (FORMAT csv, HEADER false, DELIMITER ',')''', f)
        conn.commit()
        cursor.execute('''SELECT COUNT(*) FROM "DAP_Project"."Electric_Vehicles".ev_registration''')
        total = cursor.fetchone()[0]
        print(f'{count} rows loaded into the table.')
        cursor.close()

    # print a success message
    print("Data inserted into PostgreSQL table successfully.")
    print(f'{total} rows are in the table.')

except Exception as e:
    # if an error occurs, print the error message
    print("Error inserting data into PostgreSQL table:", e)

