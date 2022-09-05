from urllib.request import urlretrieve
import os
import certifi
certifi.where()

# setting directoray for downloading datasets
output_dir = '../data/tables'
# base url where the HVFHV datasets are from
base_url_income = "https://www.abs.gov.au/census/find-census-data/datapacks/download/"
base_url_shapefile = "https://www.abs.gov.au/statistics/standards/australian-statistical-geography-standard-asgs-edition-3/jul2021-jun2026/access-and-downloads/digital-boundary-files/"


# download external data
def download_income_file(base_url_income, output_dir, year, location):
    # create folder if it does not exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # retrieving datasets from the new york taxi websit for each month
    url_download = f'{base_url_income}{year}_GCP_all_for_{location}_short-header.zip'
    print(url_download)
    urlretrieve(url_download, f'{output_dir}/{location}_{year}.csv')


# download external data
def download_shapefile(base_url_shapefile, output_dir, year, stats_area):
    # create folder if it does not exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # retrieving datasets from the new york taxi websit for each month
    url_download = f'{base_url_shapefile}{stats_area}_{year}_AUST_SHP_GDA2020.zip'
    print(url_download)
    urlretrieve(url_download, f'{output_dir}/{stats_area}shapefile_{year}.csv')
 

download_shapefile(base_url_shapefile, output_dir, 2021, 'SA2')