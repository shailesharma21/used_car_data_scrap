import logging
from datetime import datetime
import os

# logger code
def create_logger(brand_name=""):
    logger = logging.getLogger(brand_name)
    logger.setLevel(logging.INFO)

    log_format = "%(asctime)s - %(name)s - %(funcName)s - %(levelname)s - %(message)s"
    formatter = logging.Formatter(log_format)

    file_date = datetime.today().strftime("%d%m%y_%H%M%S")
    # log_folder = datetime.today().strftime("%m_%Y")
    os.mkdir("19_05")
    log_file_path = f"C:/Users/shailesh.sharma/Desktop/Used_Car_Pricing/used_car_data_scrap/19_05/{brand_name}_{file_date}.log"

    file_handler = logging.FileHandler(log_file_path, mode="a")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)

    return logger
