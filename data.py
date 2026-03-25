import pandas as pd
import boto3
import shutil
import dotenv as de
from datetime import date
import os

class Xlsx_to_paraquet:
    def __init__(self,**kwargs):
        self.source_path = kwargs.get('source_path')
        self.destination_path = kwargs.get('destination_path')
        self.AWS_ACCESS_KEY = kwargs.get('AWS_ACCESS_KEY')
        self.AWS_SECRECT_KEY = kwargs.get('AWS_SECRECT_KEY')
        self.REGION = kwargs.get('REGION')
        self.BUCKET_NAME = kwargs.get('BUCKET_NAME')
        self.AWS_FOLDER = kwargs.get('AWS_FOLDER')
        
    def file_transfer(self):
        files = [f for f in os.listdir(self.source_path) if f.endswith(('.xlsx','.xls','.csv'))]
        for file in files:
            file_name = os.path.join(self.source_path,file)
            if file.endswith('.csv'):
                df = pd.read_csv(file_name)
            else:
                df = pd.read_excel(file_name)
            parquet_name = file.replace('.xlsx','.parquet').replace('.xls','.parquet').replace('.csv','.parquet')
            parquet_path = os.path.join(self.destination_path,parquet_name)
            df.to_parquet(parquet_path)
            print(f'successfully transfered from {file} to {parquet_name}')

    def upload_to_s3(self):
        try:
            s3_client = boto3.client('s3',
                                     aws_access_key_id = self.AWS_ACCESS_KEY,
                                     aws_secret_access_key = self.AWS_SECRECT_KEY,
                                     region_name = self.REGION)
            print('Connection established successfully')
        except Exception as e:
            print(f'Unable to create connection to AWS : {e}')
        files = [f for f in os.listdir(self.destination_path) if f.endswith('.parquet')]
        for file in files :
            local_path = os.path.join(self.destination_path,file)
            s3_file = f'{self.AWS_FOLDER}{file}'
            try:
                s3_client.upload_file(local_path,self.BUCKET_NAME,s3_file)
                print(f'{file} uploaded successfully to S3 bucket {self.BUCKET_NAME}')
            except Exception as e:
                print(f'Uploading failed due to : {e}')
    def delete_local_files(self):
        files = [f for f in os.listdir(self.destination_path) if f.endswith('.parquet')]
        for file in files:
            file_path = os.path.join(self.destination_path,file)
            try:
                os.remove(file_path)
                print(f'{file} deleted successfully from local storage')
            except Exception as e:
                print(f'Unable to delete {file} due to : {e}')
    def archive_files(self):
        archive_folder = os.path.join(self.destination_path,f'archive')
        if not os.path.exists(archive_folder):
            os.makedirs(archive_folder)
        files = [f for f in os.listdir(self.source_path) if f.endswith(('.xlsx','.xls','.csv'))]
        for file in files:
            source_file = os.path.join(self.source_path,file)
            destination_file = os.path.join(archive_folder,file)
            try:
                shutil.move(source_file,destination_file)
                print(f'{file} archived successfully to {archive_folder}')
            except Exception as e:
                print(f'Unable to archive {file} due to : {e}')


config = {'source_path' : r'C:\Users\addep\OneDrive\Desktop\project\source',
          'destination_path': r'C:\Users\addep\OneDrive\Desktop\project\destination'}
env_path = de.find_dotenv()
s3_config = de.dotenv_values(env_path)
modified_config = {**config,**s3_config}
if __name__ == '__main__':
    transfer_files = Xlsx_to_paraquet(**modified_config)
    transfer_files.file_transfer()
    transfer_files.upload_to_s3()
    transfer_files.archive_files()
    transfer_files.delete_local_files()
    


