import os
import subprocess
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm


def find_mongodb_tools_path(root_dir, folder_name="mongodb-database-tools-macos-arm64-100.10.0"):
    for dirpath, dirnames, filenames in os.walk(root_dir):
        if folder_name in dirnames:
            return os.path.join(dirpath, folder_name)
    return None

def get_all_collections(db):
    return db.list_collection_names()

def export_collection_to_bson(db_name, collection, host, port, mongodump_path, export_directory):
    export_command = [
        mongodump_path,
        "--host", host,
        "--port", str(port),
        "--db", db_name,
        "--collection", collection,
        "--out", export_directory
    ]

    print(f"Execute command: {" ".join(export_command)}")
    result = subprocess.run(export_command, executable=mongodump_path)
    if result.returncode != 0:
        print(f"Error when exporting collection {collection}")

def export_collections_to_bson(db_name, exclude_collections, include_collections, mongodump_path, export_directory, max_workers=4):
    
    host = "localhost"
    port = 27017
    client = MongoClient(host=host, port=port)
    db = client[db_name]
    collections = get_all_collections(db)

    if include_collections:
        collections_to_export = [col for col in collections if col in include_collections]
    else:
        collections_to_export = [col for col in collections if col not in exclude_collections]

    with ThreadPoolExecutor(max_workers) as executor:
        futures = {
            executor.submit(export_collection_to_bson, db_name, collection, host, port, mongodump_path, export_directory): collection 
            for collection in collections_to_export
        }
        for future in tqdm(as_completed(futures), total=len(futures), desc="Export progress"):
            future.result()

    print("All collections have been exported.")


if __name__ == "__main__":
    
    db_name = "FrozenBacktest"
    exclude_collections = []
    include_collections = []

    # auto search "mongodb-database-tools" file path
    current_dir = os.path.dirname(os.path.abspath(__file__))
    mongodb_tools_path = find_mongodb_tools_path(current_dir)

    if mongodb_tools_path is None:
        print('"mongodb-database-tools" folder not found, please check directory strcture.')
    else:
        mongodump_path = os.path.join(mongodb_tools_path, "bin", "mongodump.exe")
        export_directory = os.path.join(current_dir, "exports")

        if not os.path.exists(export_directory):
            os.makedirs(export_directory)

        export_collections_to_bson(db_name, exclude_collections, include_collections, mongodump_path, export_directory)
