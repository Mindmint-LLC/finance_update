
#%%

from googleapiclient.discovery import build
from google.oauth2 import service_account
import os
from googleapiclient.http import MediaIoBaseDownload
import io
import pandas as pd
import dlogging
logger = dlogging.NewLogger(__file__, use_cd=True)


SERVICE_ACCOUNT_FILE = os.getenv('DBT_KEYFILE')
SCOPES = ['https://www.googleapis.com/auth/drive']

creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
service = build('drive', 'v3', credentials=creds)
folder_id = os.getenv('NETSUITE_FOLDER')


#%%

def folder_contents(folder_id):
    results = service.files().list(
        q=f"'{folder_id}' in parents",
        fields="files(id, name, mimeType)",
        includeItemsFromAllDrives=True,
        supportsAllDrives=True
    ).execute()
    contents = results.get('files', [])
    return contents


def csv_to_dataframe(file_id):
    request = service.files().get_media(
        fileId=file_id
    )
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
    fh.seek(0)
    df_temp = pd.read_csv(fh, skiprows=6)
    return df_temp


#%%

def main():
    logger.info('Beginning gdrive walk')
    file_list = []
    df = pd.DataFrame()
    folder_list = folder_contents(folder_id)

    while folder_list:
        file = folder_list.pop(0)
        if file['mimeType'] == 'text/csv':
            file_list.append(file)
        if file['mimeType'] == 'application/vnd.google-apps.folder':
            new_folder_list = folder_contents(file['id'])
            folder_list += new_folder_list

    for file in file_list:
        logger.info(f'Working on {file['name']}')
        df_temp = csv_to_dataframe(file['id'])
        df_temp['File'] = file['name']
        df = pd.concat([df, df_temp])

    logger.info('All files loaded, returning dataframe')

    return df


#%%