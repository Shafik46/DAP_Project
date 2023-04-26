import requests
import json
import subprocess
def install_package(package_name):
    """
    Installs a package using pip.

    Args:
        package_name (str): Name of the package to install.
    """
    subprocess.call(['pip', 'install', package_name])
install_package('pymongo')

from pymongo import MongoClient

# Set up MongoDB connection
print("Connecting to host...")
#'mongodb+srv://dap_project:password1234@cluster0.ggbgljh.mongodb.net/test'
#'mongodb://localhost:27017/'
with MongoClient('mongodb+srv://dap_project:password1234@cluster0.ggbgljh.mongodb.net/test',
                 serverSelectionTimeoutMS=70000) as client:
    db = client['dap_project']
    collection = db['EV_Population']
    print("Connected!!")

    # Load JSON data from URL
    print("Retrieving data from api....")
    url = 'https://data.wa.gov/api/views/f6w7-q2d2/rows.json?accessType=DOWNLOAD'
    with requests.get(url) as response:
        data = response.json()

    print("Inserting data...")
    # Insert data into MongoDB, avoiding duplicates
    batch_size = 1000
    count = 0
    batch = []
    for i, row in enumerate(data['data'][:5000]):
        document = {column['name']: row[i] for i, column in enumerate(data['meta']['view']['columns'])
                    if 'dataTypeName' in column and column['dataTypeName'] != 'meta_data'}
        batch.append(document)
        if len(batch) == batch_size:
            try:
                result = collection.insert_many(batch, ordered=False)
                count += len(result.inserted_ids)
            except Exception as e:
                print(f"Error inserting documents: {str(e)}")
            batch = []

    # Insert the remaining documents in the batch
    if len(batch) > 0:
        try:
            result = collection.insert_many(batch, ordered=False)
            count += len(result.inserted_ids)
        except Exception as e:
            print(f"Error inserting documents: {str(e)}")

    print(f"{count} documents inserted or updated in the collection.")
