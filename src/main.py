import datetime
import os
import time
from pathlib import Path

import dotenv

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_DIR = Path(__file__).resolve().parents[1]

from collect import Collect
from sender import Sender

dotenv.load_dotenv()

BUCKET_NAME = os.getenv("BUCKET_NAME")

logging.info("Iniciando processo...")

logging.info("Coletando dados...")
collect = Collect(years=[datetime.datetime.now().year])
collect.process_years()

send = Sender(BUCKET_NAME)
folder = BASE_DIR / "data"

logging.info("Enviando dados raw...")
send.process_folder(folder / "raw", "*.zip", "raw")
logging.info("Envio de dados raw finalizado!")

logging.info("Enviando dados bronze...")
send.process_folder(folder / "bronze", "*.parquet", "bronze")
logging.info("Envio de dados bronze finalizado!")