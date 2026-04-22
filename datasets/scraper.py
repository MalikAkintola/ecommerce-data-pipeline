import kaggle
import os

#authenticates the kaggle.json file
kaggle.api.authenticate()
print('Kaggle Credentials authenticated. \n Proceeding to download datasets...')
    
kaggle.api.dataset_download_files('olistbr/brazilian-ecommerce', path = '.', unzip = True)

csv_files = [f for f in os.listdir('.') if f.endswith('.csv')]

print(f"Number of CSV files downloaded: {len(csv_files)}")
print("\nFiles:")
for f in csv_files:
    print(f" - {f}")