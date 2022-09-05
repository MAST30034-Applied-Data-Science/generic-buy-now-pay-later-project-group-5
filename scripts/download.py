from urllib.request import urlretrieve
import os
import certifi
certifi.where()

# setting directoray for downloading datasets
output_dir = '../data/tables'
# base url where the HVFHV datasets are from
base_url = "https://www.abs.gov.au/census/find-census-data/datapacks/download/"


# download external data
def download_file(base_url, output_dir, year, location):
    # create folder if it does not exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # retrieving datasets from the new york taxi websit for each month
    url_download = f'{base_url}{year}_GCP_all_for_{location}_short-header.zip'
    print(url_download)
    urlretrieve(url_download, f'{output_dir}/{location}_{year}.csv')


download_file(base_url, output_dir, 2021, 'AUS')    