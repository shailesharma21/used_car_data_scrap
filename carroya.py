import requests
import json
import pandas as pd
from random import randint
from time import sleep
from datetime import datetime

print("start: ", datetime.now())
start_url = "https://carroya-pro-portal-api.avaldigitallabs.com/find-vehicle-detail/"
car_urls = pd.read_csv("car_urls_1805.csv")

header = {
    "accept": "application/json, text/plain, */*",
    "accept-encoding": "gzip, deflate, br",
    "accept-language": "en-US,en;q=0.9",
    "origin": "https://www.carroya.com",
    "referer": "https://www.carroya.com/",
    "sec-ch-ua": '" Not A;Brand";v="99", "Chromium";v="101", "Google Chrome";v="101"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "cross-site",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.64 Safari/537.36",
    "x-client-id": "1427838632.1652335850",
}


def scraper(url):
    sleep(randint(3, 6))
    car_id = car_urls["URL"][url].split("/")[-1]
    try:
        url_response = requests.get(start_url + car_id, headers=header)
        car_data = json.loads(url_response.text)
        car_detail = {
            "Car id": car_data["data"]["id"],
            "Brand": car_data["data"]["title"],
            "Condition": car_data["data"]["status"],
            "Version": car_data["data"]["version"],
            "Kilometers": car_data["data"]["kilometers"],
            "Year": car_data["data"]["year"],
            "City": car_data["data"]["city"],
            "Color": car_data["data"]["color"],
            "Cylindrical": car_data["data"]["cylindrical"],
            "Box type": car_data["data"]["box"],
            "Fuel": car_data["data"]["fuel"],
            "Accessories": car_data["data"]["accessories"],
            "Seller comment": car_data["data"]["comments"],
            "Main features": car_data["data"]["main-features"],
            "Technical details": car_data["data"]["technical_sheet"],
        }
        return car_detail
    except:
        car_detail = {"Car id": car_id}
        return car_detail


out_list = []
# checking for 200 urls
for url in range(0, 200):
    out_list.append(scraper(url))

# Create dataframe from all cars data
car_df = pd.DataFrame(out_list)
car_df.to_csv("scraped_car_info_200urls.csv")
print("end: ", datetime.now())
