import kaggle
import os
import boto3

#authenticates the kaggle.json file
kaggle.api.authenticate()
print('Kaggle Credentials authenticated. \n Proceeding to download datasets...')
    
kaggle.api.dataset_download_files('olistbr/brazilian-ecommerce', path = '.', unzip = True)

csv_files = [f for f in os.listdir('.') if f.endswith('.csv')]

print(f"Number of CSV files downloaded: {len(csv_files)}")
print("\nFiles:")
for f in csv_files:
    print(f" - {f}")


# Upload to S3
s3 = boto3.client('s3')
bucket_name = 'netflixdataset-malikola'

print("\nUploading to S3...")
for f in csv_files:
    s3.upload_file(f, bucket_name, f"raw/olist/{f}")
    print(f" - Uploaded: {f}")

print("\nAll files uploaded successfully.")