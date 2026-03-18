from google.cloud import storage
import os
from dotenv import load_dotenv
from tqdm import tqdm 
from pathlib import Path
import argparse

load_dotenv()

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

BASE_DIR = Path(__file__).resolve().parents[1]

class Sender:

    def __init__(self, bucket):
        self.storage = storage.Client()
        self.bucket= self.storage.bucket(bucket)

    # Realiza o upload do arquivo no folder de destino
    def process_file(self, filename: Path, dest_folder):

        try:
            blob = self.bucket.blob(f"{dest_folder}")
            blob.upload_from_filename(filename)
        
            filename.unlink()
        except Exception as err:
            print(err)
        
    # Processa o diretório de arquivos para realizar o upload dos arquivos desejados
    def process_folder(self, folder: Path, pattern: str, layer: str):
        files = list(folder.rglob(pattern))

        for file in tqdm(files):
            relative_path = file.relative_to(folder)
            gcs_path = Path(layer) / relative_path

            self.process_file(file, gcs_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", type=str, required=True)
    parser.add_argument("--folder", default="data", type=str)
    parser.add_argument("--layer", choices=["raw", "bronze", "all"], default="all")
    args = parser.parse_args()

    send = Sender(args.bucket)
    folder = BASE_DIR / args.folder

    if not folder.exists():
        raise ValueError(f"Pasta não existe: {folder}")

    if args.layer in ["raw", "all"]:
        logging.info("Enviando dados raw")
        send.process_folder(folder / "raw", "*.zip", "raw")

    if args.layer in ["bronze", "all"]:
        logging.info("Enviando dados bronze")
        send.process_folder(folder / "bronze", "*.parquet", "bronze")

    