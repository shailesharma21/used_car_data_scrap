import requests
import json
import pandas as pd
from random import randint
from time import sleep
from datetime import datetime
from config_carroya import start_url, header
from logger_function import create_logger
import logging

print("start: ", datetime.now())
scrape_logger = create_logger("carroya")


def scrap_car_id(page_num):
    """
    This function gets all car ids from given page number
    """
    try:
        url = "https://carroya-pro-portal-api.avaldigitallabs.com/find-filters"
        payload = json.dumps(
            {"seoArray": [], "params": {"page": str(page_num), "status": "usado"}}
        )
        headers = {"Content-Type": "text/plain"}
        response = requests.request("POST", url, headers=headers, data=payload).json()
        car_ids = [x["id"] for x in response["results"]["superHighlights"]]
        scrape_logger.info(f"Scraped {len(car_ids)} car ids for page no {page_num}")
        return car_ids
    except:
        scrape_logger.exception("Error getting car id for page: ", page_num)
        return []


def scraper(car_id_list):
    """
    This function scraps car data for each card id on a page
    """
    car_data_list = []
    for car_id in car_id_list:
        sleep(randint(3, 8))
        try:
            url_response = requests.get(start_url + str(car_id), headers=header)
            car_data = json.loads(url_response.text)
            car_detail = {
                "Car id": car_data["data"]["id"],
                "Condition": car_data["data"]["status"],
                "Brand": car_data["data"]["brand"],
                "Model": car_data["data"]["model"],
                "Version": car_data["data"]["version"],
                "Published date": car_data["data"]["fechapublicacion"].split("T")[0],
                "Kilometers": car_data["data"]["kilometers"],
                "Price": car_data["data"]["price"],
                "Year": car_data["data"]["year"],
                "City": car_data["data"]["city"],
                "Color": car_data["data"]["color"],
                "Cylindrical": car_data["data"]["cylindrical"],
                "Box type": car_data["data"]["box"],
                "Fuel": car_data["data"]["fuel"],
                "Accessories": car_data["data"]["accessories"],
                "Seller comment": car_data["data"]["comments"],
                car_data["data"]["main-features"][0]["name"]: car_data["data"][
                    "main-features"
                ][0]["description"],
                car_data["data"]["main-features"][1]["name"]: car_data["data"][
                    "main-features"
                ][1]["description"],
                car_data["data"]["main-features"][2]["name"]: car_data["data"][
                    "main-features"
                ][2]["description"],
                car_data["data"]["main-features"][3]["name"]: car_data["data"][
                    "main-features"
                ][3]["description"],
                car_data["data"]["main-features"][4]["name"]: car_data["data"][
                    "main-features"
                ][4]["description"],
                car_data["data"]["main-features"][5]["name"]: car_data["data"][
                    "main-features"
                ][5]["description"],
                car_data["data"]["main-features"][6]["name"]: car_data["data"][
                    "main-features"
                ][6]["description"],
                car_data["data"]["main-features"][7]["name"]: car_data["data"][
                    "main-features"
                ][7]["description"],
                car_data["data"]["main-features"][8]["name"]: car_data["data"][
                    "main-features"
                ][8]["description"],
                car_data["data"]["main-features"][9]["name"]: car_data["data"][
                    "main-features"
                ][9]["description"],
                car_data["data"]["main-features"][10]["name"]: car_data["data"][
                    "main-features"
                ][10]["description"],
                car_data["data"]["main-features"][11]["name"]: car_data["data"][
                    "main-features"
                ][11]["description"],
                car_data["data"]["main-features"][12]["name"]: car_data["data"][
                    "main-features"
                ][12]["description"],
                "Technical details": car_data["data"]["technical_sheet"],
                "Url": car_data["data"]["detail"][:-3],
            }
            if car_data["data"]["version"]:
                car_drive_train = car_data["data"]["version"].lower()
                drive_train_types = ["4x4", "4x2"]
                for d_train in drive_train_types:
                    if d_train in car_drive_train:
                        car_detail["Drive train"] = d_train
            if len(car_data["data"]["technical_sheet"]):
                car_detail[
                    car_data["data"]["technical_sheet"][0]["autopart"]
                ] = car_data["data"]["technical_sheet"][0]["specifications"]
                car_detail[
                    car_data["data"]["technical_sheet"][1]["autopart"]
                ] = car_data["data"]["technical_sheet"][1]["specifications"]
            car_data_list.append(car_detail)
            scrape_logger.info(f"Scraped data for {car_id}")
        except:
            scrape_logger.exception(f"No car details for:{car_id}")
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
        scrape_logger.exception("Exception occured")

# Create dataframe from all cars data
car_df = pd.DataFrame(all_car_data)
car_df.to_csv("car_data_for_208_with_features.csv")
print("end: ", datetime.now())

logging.shutdown()
