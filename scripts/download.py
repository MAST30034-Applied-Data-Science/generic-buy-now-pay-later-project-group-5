from urllib.request import urlretrieve
import os
import certifi
certifi.where()
import requests, zipfile, io
import argparse

# base url where the HVFHV datasets are from
base_url_income = "https://www.abs.gov.au/census/find-census-data/datapacks/download/"
base_url_shapefile = "https://www.abs.gov.au/statistics/standards/australian-statistical-geography-standard-asgs-edition-3/jul2021-jun2026/access-and-downloads/digital-boundary-files/"
url_sa2_correspondence = "https://www.abs.gov.au/statistics/standards/australian-statistical-geography-standard-asgs-edition-3/jul2021-jun2026/access-and-downloads/correspondences/CG_SA2_2016_SA2_2021.csv"
url_coding_index = "https://data.gov.au/data/dataset/1646f764-82ad-4c21-b49c-63480f425a4a/resource/c6051960-6012-452c-ac68-dba55a1f837a/download/asgs2016codingindexes.zip"
coding_index_file = "2019 Locality to 2016 SA2 Coding Index.csv"

# Want population, education, income
# Identified age as an important grouping category because young people seem more likely to use afterpay


# download external data
def download_income_file(base_url_income, output_dir, year, location):
    # create folder if it does not exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # retrieving datasets from the new york taxi websit for each month
    url_download = f'{base_url_income}{year}_GCP_all_for_{location}_short-header.zip'
    # print(url_download)
    urlretrieve(url_download, f'{output_dir}/{location}_{year}.zip')


# download external data
def download_shapefile(base_url_shapefile, output_dir, year, stats_area):
    # create folder if it does not exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    sf_path = f'{output_dir}/shapefile'
    if not os.path.exists(sf_path):
        os.makedirs(sf_path)

    url_download = f'{base_url_shapefile}{stats_area}_{year}_AUST_SHP_GDA2020.zip'
    # print(url_download)
    sf_name = f'{stats_area}shapefile_{year}'
    sf_dir = f'{sf_path}/{sf_name}.zip'
    urlretrieve(url_download, sf_dir)
    r = requests.get(url_download)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall(path=sf_path)
    
    
# download 2016 SA2 to 2021 SA2 correspondence  
def download_correspondence(output_dir) :
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    urlretrieve(url_sa2_correspondence, f'{output_dir}/sa2_correspondence.csv')

# download coding index for postcode to SA2 conversion
def download_index(output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    r = requests.get(url_coding_index)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extract(coding_index_file, path=output_dir)

def main(): 

    parser = argparse.ArgumentParser()
    parser.add_argument('--output', help = 'the desired output file for data',type = str)
    args = parser.parse_args()

    download_correspondence(args.output)
    download_index(args.output)
    download_income_file(base_url_income, args.output, 2021, 'AUS')
    download_shapefile(base_url_shapefile, args.output, 2021, 'SA2')

if __name__ == "__main__":
    main()
