import requests
import json
import pandas as pd
from random import randint
from time import sleep
from datetime import datetime


print("start: ", datetime.now())
start_url = "https://carroya-pro-portal-api.avaldigitallabs.com/find-vehicle-detail/"

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


def scrap_car_id(page_num):
    url = "https://carroya-pro-portal-api.avaldigitallabs.com/find-filters"
    payload = json.dumps(
        {"seoArray": [], "params": {"page": str(page_num), "status": "usado"}}
    )
    headers = {"Content-Type": "text/plain"}
    response = requests.request("POST", url, headers=headers, data=payload)
    res_data = response.json()
    car_ids = [x["id"] for x in res_data["results"]["superHighlights"]]
    return car_ids


def scraper(car_id_list):
    car_data_list = []
    for car_id in car_id_list:
        sleep(randint(3, 8))
        try:
            url_response = requests.get(start_url + str(car_id), headers=header)
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
            car_data_list.append(car_detail)
        except:
            car_detail = {"Car id": str(car_id)}
            car_data_list.append(car_detail)
    return car_data_list


all_car_data = []
# Using a fixed no of pages for testing in for loop
for page in range(1, 14):
    sleep(randint(3, 8))
    try:
        car_ids = scrap_car_id(page)
        if car_ids:
            out_car_data = scraper(car_ids)
            all_car_data.extend(out_car_data)
        else:
            continue
    except:
        pass

# Create dataframe from all cars data
car_df = pd.DataFrame(all_car_data)
car_df.to_csv("car_data_for_208.csv")
print("end: ", datetime.now())
