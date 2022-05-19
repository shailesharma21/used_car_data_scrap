import requests
import json
import pandas as pd
from random import randint
from time import sleep
from datetime import datetime
from config import start_url, header
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
                "Brand": car_data["data"]["title"],
                "Condition": car_data["data"]["status"],
                "Version": car_data["data"]["version"],
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
                "Main features": car_data["data"]["main-features"],
                "Technical details": car_data["data"]["technical_sheet"],
                "Url": car_data["data"]["detail"][:-3],
            }
            car_data_list.append(car_detail)
        except:
            scrape_logger.exception("No car details for: ", car_id)
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
        pass

# Create dataframe from all cars data
car_df = pd.DataFrame(all_car_data)
car_df.to_csv("car_data_for_208.csv")
print("end: ", datetime.now())

logging.shutdown()
