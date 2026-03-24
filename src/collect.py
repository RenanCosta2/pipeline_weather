import requests
from pathlib import Path
import zipfile
from tqdm import tqdm
import pandas as pd
import re
from datetime import datetime
import argparse

pd.set_option("display.max_columns", None)

import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

BASE_DIR = Path(__file__).resolve().parents[1]

class Collect:

    def __init__(self, years=[2023, 2024, 2025]):
        self.years=years

    # Faz uma requisição para a API do INMET e recupera um arquivo .zip com os dados meteorológicos do ano passado como parâmetro
    def get_data(self, year: int, zip_path: Path):
        url = f"https://portal.inmet.gov.br/uploads/dadoshistoricos/{year}.zip"

        with requests.get(url, timeout=10, stream=True) as response:
            response.raise_for_status()

            with open(zip_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

    # Extrai o arquivo .zip
    def extract_zip(self, zip_path: Path, dest_folder: Path):
        with zipfile.ZipFile(zip_path, "r") as f:
            f.extractall(dest_folder)

    # Normalização dos dados meteorológicos e metadados de cada arquivo
    def normalize_data(self, file: Path):
        inmet_data = pd.read_csv(
            file,
            sep=";",
            encoding="latin1",
            skiprows=8,
            dtype=str
        )

        metadata = pd.read_csv(
            file,
            sep=";",
            encoding="latin1",
            nrows=7,
            header=None,
            dtype=str
        )
        metadata = metadata.set_index(metadata.columns[0]).T
        metadata = metadata.rename(columns=lambda x: x.replace(":", "").lower())

        inmet_data["source_file"] = file.name

        rename_columns = {
            'Data': 'data',
            'Hora UTC': 'hora_utc',
            'PRECIPITAÇÃO TOTAL, HORÁRIO (mm)': 'precipitacao_total_mm',
            'PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)': 'pressao_atmosferica_estacao_mb',
            'PRESSÃO ATMOSFERICA MAX.NA HORA ANT. (AUT) (mB)': 'pressao_atmosferica_max_mb',
            'PRESSÃO ATMOSFERICA MIN. NA HORA ANT. (AUT) (mB)': 'pressao_atmosferica_min_mb',
            'RADIACAO GLOBAL (Kj/m²)': 'radiacao_global_kj_m2',
            'TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)': 'temperatura_ar_c',
            'TEMPERATURA DO PONTO DE ORVALHO (°C)': 'temperatura_orvalho_c',
            'TEMPERATURA MÁXIMA NA HORA ANT. (AUT) (°C)': 'temperatura_max_c',
            'TEMPERATURA MÍNIMA NA HORA ANT. (AUT) (°C)': 'temperatura_min_c',
            'TEMPERATURA ORVALHO MAX. NA HORA ANT. (AUT) (°C)': 'temperatura_orvalho_max_c',
            'TEMPERATURA ORVALHO MIN. NA HORA ANT. (AUT) (°C)': 'temperatura_orvalho_min_c',
            'UMIDADE REL. MAX. NA HORA ANT. (AUT) (%)': 'umidade_relativa_max',
            'UMIDADE REL. MIN. NA HORA ANT. (AUT) (%)': 'umidade_relativa_min',
            'UMIDADE RELATIVA DO AR, HORARIA (%)': 'umidade_relativa',
            'VENTO, DIREÇÃO HORARIA (gr) (° (gr))': 'vento_direcao_gr',
            'VENTO, RAJADA MAXIMA (m/s)': 'vento_rajada_max_ms',
            'VENTO, VELOCIDADE HORARIA (m/s)': 'vento_velocidade_ms',
            'source_file': 'source_file'
        }

        inmet_data = inmet_data.drop(columns=['Unnamed: 19'], errors='ignore')

        inmet_data = inmet_data.rename(columns=rename_columns)

        return inmet_data, metadata
    
    # Extrai o station_id do nome do arquivo
    def extract_station(self, filename: str) -> str:
        parts = filename.split("_")
        return parts[3].strip().upper()

    # Extrai a data final do nome do arquivo
    def extract_end_date(self, filename: str) -> datetime:
        match = re.search(r"_A_(\d{2}-\d{2}-\d{4})", filename)
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

        raw_folder = data_folder / "raw"
        raw_folder.mkdir(parents=True, exist_ok=True)
        zip_path = raw_folder / f"{year}.zip"
        logging.info("Obtendo dados...")
        self.get_data(year, zip_path)

        extract_folder = raw_folder / "extracted"
        extract_folder.mkdir(parents=True, exist_ok=True)
        logging.info("Extraindo arquivo zip")
        self.extract_zip(zip_path, extract_folder)

        files = list(extract_folder.rglob("*.[Cs][Ss][Vv]"))

        selected_files = self.get_latest_files(files)
        
        bronze_folder = data_folder / "bronze"
        bronze_folder.mkdir(parents=True, exist_ok=True)
        logging.info("Organizando dados e persistindo em Parquet (armazenamento local)")
        for file in tqdm(selected_files):
            df, metadata = self.normalize_data(file)

            station_id = metadata["codigo (wmo)"].iloc[0]

            filename_data_folder = bronze_folder / "data" / f"year={year}" / f"station={station_id}"
            filename_data_folder.mkdir(parents=True, exist_ok=True)
            filename_data = filename_data_folder / "data.parquet"
            self.save_data(df, filename_data)

            filename_meta_folder = bronze_folder / "metadata" / f"year={year}" / f"station={station_id}"
            filename_meta_folder.mkdir(parents=True, exist_ok=True)
            filename_meta = filename_meta_folder / "metadata.parquet"
            self.save_data(metadata, filename_meta)
            
        for file in files:
            file.unlink()

    # Processamento do pipeline para os anos definidos no init
    def process_years(self):
        for year in self.years:
            logging.info(f"Coletando dados do ano {year}")
            self.process(year)
            logging.info(f"Extração do ano {year} concluída!")




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