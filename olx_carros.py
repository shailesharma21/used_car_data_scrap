import requests
from random import randint
from time import sleep
from datetime import datetime
from config_olx import start_url, headers
from logger_function import create_logger
import logging
import pandas as pd

print("start: ", datetime.now())
scrape_logger = create_logger("olx_carros")


def olx_data_scrap(page_num):
    car_data = []
    url = start_url
    payload = {
        "facet_limit": "100",
        "clientId": "pwa",
        "size": "40",
        "location_facet_limit": "20",
        "location": "1000001",
        "page": str(page_num),
        "category": "378",
        "clientVersion": "9.13.0",
        "user": "180db025825x3682f403",
        "platform": "web-desktop",
    }
    response = requests.get(url, headers=headers, params=payload).json()
    car_id_list = [x["id"] for x in response["data"]]
    if car_id_list:
        for car_id in range(0, len(car_id_list)):
            try:
                raw_data = response["data"][car_id]
                car_detail = {
                    "Car id": raw_data["id"],
                    "Brand info": raw_data["title"],
                    "Price": raw_data["price"]["value"]["display"],
                    "City": raw_data["locations_resolved"],
                    "Description": raw_data["description"],
                }
                if raw_data["parameters"]:
                    param_data = raw_data["parameters"]
                    exclude_params = [
                        "previous_owner",
                        "licenseplate",
                        "sellertype",
                        "ad_phone",
                    ]
                    for data in range(0, len(raw_data["parameters"])):
                        if param_data[data]["key"] not in exclude_params:
                            car_detail[param_data[data]["key"]] = param_data[data][
                                "formatted_value"
                            ]
                if "variant" in car_detail.keys():
                    drive_train_types = ["4x4", "4x2"]
                    val = car_detail["variant"].lower()
                    for d_train in drive_train_types:
                        if d_train in val:
                            car_detail["Drive train"] = d_train
                car_data.append(car_detail)
                scrape_logger.info(f"Data scraped for car id: {car_id_list[car_id]}")
            except:
                scrape_logger.exception(
                    f"error in fetching car data for car: {car_id_list[car_id]}"
                )
    else:
        scrape_logger.info(f"No cars present on {page_num}")
    return car_data


all_car_data = []
# Using a fixed no of pages for testing in for loop
for page in range(1, 6):
    sleep(randint(4, 9))
    try:
        out_car_data = olx_data_scrap(page)
        all_car_data.extend(out_car_data)
    except:
        scrape_logger.exception("Exception occured")

# Create dataframe from all cars data
car_df = pd.DataFrame(all_car_data)
car_df.to_csv("olx_car_data_40.csv")
print("end: ", datetime.now())

logging.shutdown()
