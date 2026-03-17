import requests
from pathlib import Path
import zipfile
import pandas as pd
import re
from datetime import datetime
import argparse

pd.set_option('display.max_columns', None)

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_DIR = Path(__file__).resolve().parents[1]

class Collect:

    def __init__(self, years=[2023, 2024, 2025]):
        self.years=years

    # Faz uma requisição para a API do INMET e recupera um arquivo .zip com os dados meteorológicos do ano passado como parâmetro
    def get_data(self, year: int, zip_path: Path):
        url = f'https://portal.inmet.gov.br/uploads/dadoshistoricos/{year}.zip'

        with requests.get(url, timeout=10, stream=True) as response:
            response.raise_for_status()

            with open(zip_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

    # Extrai o arquivo .zip
    def extract_zip(self, zip_path: Path, dest_folder: Path):
        with zipfile.ZipFile(zip_path, 'r') as f:
            f.extractall(dest_folder)

    # Normalização dos dados meteorológicos e metadados de cada arquivo
    def normalize_data(self, file: Path):
        inmet_data = pd.read_csv(
            file,
            sep=';',
            encoding="latin1",
            skiprows=8
        )

        metadata = pd.read_csv(
            file,
            sep=';',
            encoding="latin1",
            nrows=7,
            header=None
        )
        metadata = metadata.set_index(metadata.columns[0]).T
        metadata = metadata.rename(columns=lambda x: x.replace(':', '').lower())

        inmet_data['source_file'] = file.name

        return inmet_data, metadata
    
    # Extrai o station_id do nome do arquivo
    def extract_station(self, filename: str) -> str:
        parts = filename.split("_")
        return parts[3].strip().upper()

    # Extrai a data final do nome do arquivo
    def extract_end_date(self, filename: str) -> datetime:
        match = re.search(r'_A_(\d{2}-\d{2}-\d{4})', filename)
        if not match:
            raise ValueError(f"Não conseguiu extrair data de: {filename}")
        return datetime.strptime(match.group(1), "%d-%m-%Y")
    
    # Recupera o nome dos arquivos mais recentes de cada station 
    def get_latest_files(self, files: list) -> dict:
        latest_files = {}

        for file in files:
            station = self.extract_station(file.name)
            end_date = self.extract_end_date(file.name)

            if (
                station not in latest_files
                or end_date > latest_files[station]["end_date"]
            ):
                latest_files[station] = {
                    "file": file,
                    "end_date": end_date
                }

        selected_files = [v["file"] for v in latest_files.values()]
        return selected_files

    # Salva os dados processados em um arquivo .parquet
    def save_data(self, df: pd.DataFrame, filename: Path):
        df.to_parquet(filename, index=False)

    # Processamento do pipeline completo
    def process(self, year):
        data_folder = BASE_DIR / "data"
        data_folder.mkdir(parents=True, exist_ok=True)

        raw_folder = data_folder / "raw"
        raw_folder.mkdir(parents=True, exist_ok=True)
        zip_path = raw_folder / f"{year}.zip"
        self.get_data(year, zip_path)

        extract_folder = raw_folder / "extracted"
        extract_folder.mkdir(parents=True, exist_ok=True)
        self.extract_zip(zip_path, extract_folder)

        files = list(extract_folder.glob("*.[Cs][Ss][Vv]"))

        selected_files = self.get_latest_files(files)
        
        bronze_folder = data_folder / "bronze"
        bronze_folder.mkdir(parents=True, exist_ok=True)
        for file in selected_files:
            df, metadata = self.normalize_data(file)

            station_id = metadata['codigo (wmo)'].iloc[0]
            file_folder = bronze_folder / f"year={year}" / f"station={station_id}"
            file_folder.mkdir(parents=True, exist_ok=True)

            filename_data = file_folder / f'{file.stem.lower()}.parquet'
            self.save_data(df, filename_data)
            filename_meta = file_folder / 'metadata.parquet'
            self.save_data(metadata, filename_meta)
            
        for file in files:
            file.unlink()

    # Processamento do pipeline para os anos definidos no init
    def process_years(self):
        for year in self.years:
            logging.info(f"Coletando dados do ano {year}")
            self.process(year)




if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=int, default=0)
    parser.add_argument("--stop", type=int, default=0)
    parser.add_argument("--years", "-y", nargs="+", type=int)
    args = parser.parse_args()

    if args.years:
        collect = Collect(args.years)

    elif args.start and args.stop:
        years = [i for i in range(args.start, args.stop+1)]
        collect = Collect(years)
   
    collect.process_years()