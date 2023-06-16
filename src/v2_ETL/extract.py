#extract.py file
import io
import logging
import os
import boto3
import pandas as pd
from botocore.exceptions import NoCredentialsError
from dotenv import load_dotenv

#logging.basicConfig(filename=config["log_file_path_v2"], level=logging.INFO, format=' %(asctime)s %(levelname)s %(message)s')
load_dotenv()

aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')

class DataLoadingError(Exception):
    """Exception raised when an error occurs while loading data."""

class DataExtractor:
    def __init__(self, file_paths, bucket):
        """Initialize the DataExtractor with the given file paths."""
        self.file_paths = file_paths
        self.bucket = bucket
        self.data = {}
        self.s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)


    def extract_data(self):
        """Load data from the given file paths in S3."""
        for name, path in self.file_paths.items():
            try:
                # Get the S3 object
                obj = self.s3.get_object(Bucket=self.bucket, Key=path)
                
                # Read the object's StreamingBody
                data = obj['Body'].read()

                # Load data into a pandas DataFrame
                self.data[name] = pd.read_excel(io.BytesIO(data))
                logging.info(f"{name} data loaded successfully.")
            except NoCredentialsError:
                raise DataLoadingError(f"No AWS credentials found")
            except Exception as e:
                raise DataLoadingError(f"An error occurred while loading the {name} data: {e}")
        return self.data
