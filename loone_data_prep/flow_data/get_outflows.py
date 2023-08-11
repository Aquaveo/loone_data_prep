import sys
import os
from glob import glob
from loone_data_prep.flow_data import hydro


DBKEYS = ["91370", "91373", "91379", "91508", "91510", "91513", "91677",
          "15628", "15640", "15626", "00865","JW224","00436","15018","91606","JW223"]

def main(workspace: str, dbkeys: list = DBKEYS) -> dict:
    # Retrieve outflow data
    for dbkey in dbkeys:
        hydro.get(workspace, dbkey, "2000-01-01")

    # Check if all files were downloaded
    files = glob(f'{workspace}/*FLOW*_cmd.csv')

    for file in files:
        file_dbkey = file.split('_')[-2]

        if file_dbkey in dbkeys:
            # Remove dbkey from file name
            new_file_name = file.replace(f'_{file_dbkey}', '')
            os.rename(file, new_file_name)

            # Remove dbkey from dbkeys so we know it successfully downloaded
            dbkeys.remove(file_dbkey)

    if len(dbkeys) > 0:
        return {'error': f'The data from the following dbkeys could not be downloaded: {dbkeys}'}

    return {'success': 'Completed outflow flow data download.'}

if __name__ == "__main__":
    workspace = sys.argv[1].rstrip("/")
    main(workspace)