import os
import subprocess

host = "localhost"
port = 27017

def restore_mongodb(db_name, backup_dir):
    # retrieve all .bson files
    bson_files = [f for f in os.listdir(backup_dir) if f.endswith('.bson')]
    
    # auto check mongorestore file path
    mongorestore_path = None
    for root, dirs, files in os.walk(os.path.dirname(os.path.realpath(__file__))):
        if 'mongorestore.exe' in files:
            mongorestore_path = os.path.join(root, 'mongorestore.exe')
            break

    if not mongorestore_path:
        raise FileNotFoundError('"mongorestore.exe" not found, please make sure "mongodb-database-tools" folder has the same level as the current script.')

    for bson_file in bson_files:
        collection_name = bson_file.split('.')[0]  # extract collection names
        metadata_file = os.path.join(backup_dir, f"{collection_name}.metadata")
        
        # make up mongorestore commands
        restore_command = [
            mongorestore_path,
            "--host", host,
            "--port", str(port),
            "--db", db_name,
            "--collection", collection_name,
            os.path.join(backup_dir, bson_file)
        ]
        
        # execute mongorestore command
        print(f"Execute command: {" ".join(restore_command)}")
        subprocess.run(restore_command)
        
        # if .metadata file exists, also restore
        if os.path.exists(metadata_file):
            restore_metadata_command = [
                mongorestore_path,
                "--host", host,
                "--port", str(port),
                "--db", db_name,
                "--collection", collection_name,
                metadata_file
            ]
            print(f"Execute command: {" ".join(restore_metadata_command)}")
            subprocess.run(restore_metadata_command)


if __name__ == "__main__":
    
    database_name = "FactorBase"  # substitute for target database storage name
    # retrieve directory of current script
    current_directory = os.path.dirname(os.path.realpath(__file__))
    # set export directory
    backup_directory = os.path.join(current_directory, "exports", database_name) 
    restore_mongodb(database_name, backup_directory)
