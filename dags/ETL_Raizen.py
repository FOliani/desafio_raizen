#!/usr/bin/env python
# coding: utf-8

##########################################################################################
#                                                                                        #
# etl pipeline to extract and structure the underlying data of two pivot tables:         #
#                                                                                        #
# -sales of oil derivative fuels by UF and product                                       #
# -sales of diesel by UF and type                                                        #
#                                                                                        #
# the totals of the extracted data must be equal to the totals of the pivot tables       #
#                                                                                        #
##########################################################################################

import os
import sqlite3
import logging
import datetime
import pandas as pd
from urllib import request
from airflow import DAG
from airflow.operators.python import PythonOperator

def create_raw_data_dir() -> None:
    """ function which verify the existence of raw_data dir and create if it not exists """

    if os.path.isdir("./raw_data"):
        logging.info("Dir already exists")
        pass
    else:
        logging.info("Creating raw_data dir")
        os.system("mkdir ./raw_data")
        logging.info("Dir successfully created")


def download_raw_data() -> None:
    """ function to extract the raw data and save into local directory """

    file_url = "https://github.com/raizen-analytics/data-engineering-test/raw/master/assets/vendas-combustiveis-m3.xls"
    file_xls = "./raw_data/vendas-combustiveis-m3.xls"
    logging.info("Downloading .xls raw data")
    request.urlretrieve(file_url , file_xls)
    logging.info("Successfully downloaded .xls raw data")


def convert_xls_into_xlsx() -> None:
    """ functtion which convert .xls into .xlsx using libreoffice """

    logging.info("Converting file .xls into .xlsx using libreoffice")
    os.system("libreoffice --headless --invisible --convert-to xlsx ./raw_data/vendas-combustiveis-m3.xls --outdir ./raw_data")
    logging.info("File .xls successfully converted into .xlsx")


def read_transform_and_save():
    """ function which read the data, apply the desired data transformation to extracted sheets and save output as parquet"""
    
    list_specific_sheets = ["DPCache_m3", "DPCache_m3_2"]
    for i in list_specific_sheets:
        logging.info(f"Processing data of sheet {i}")
        xlsx = pd.ExcelFile("./raw_data/vendas-combustiveis-m3.xlsx")
        df = pd.read_excel(xlsx, i)

        #taking only product
        df["product"] = df["COMBUSTÍVEL"].str.split(" \(").str[0]

        #taking only unit
        df["unit"] = df["COMBUSTÍVEL"].str.split(" \(").str[-1].str.split("\)").str[0]

        #renaming "ESTADO" column
        df = df.rename(columns={"ESTADO":"uf"})

        #dropping obsolete columns
        df = df.drop(columns=["COMBUSTÍVEL", "REGIÃO", "TOTAL"], axis=1)

        #creating column 'volume' with respective 'month'
        df = pd.melt(df, id_vars=["product", "uf", "ANO", "unit"], var_name=["month"], value_name="volume")

        months_names={"Jan":"01", "Fev":"02", "Mar":"03", "Abr":"04", "Mai":"05", "Jun":"06",
                      "Jul":"07", "Ago":"08", "Set":"09", "Out":"10", "Nov":"11", "Dez":"12"}

        df["month"] = df.month.replace(months_names)

        #creating 'year_month' column
        df["year_month"] = df["ANO"].astype(str) + "-" + df["month"].astype(str)

        df = df.drop(columns=["ANO", "month"])

        #creating 'created_at' column
        df["created_at"] = datetime.datetime.today().replace(microsecond=0)

        #completing nulls with '0'
        df = df.fillna(0)

        #defining data type of final dataframe
        df["product"] = df["product"].astype(str)
        df["uf"] = df["uf"].astype(str)
        df["unit"] = df["unit"].astype(str)
        df["volume"] = df["volume"].astype(float)
        df["year_month"] = df["year_month"].astype(str)
        df["created_at"] = pd.to_datetime(df["created_at"])

        print("\n")
        logging.info(f"Data of sheet {i} successfully processed")
        logging.info("Visual check: ")
        print(df.head())

        logging.info("Saving processed data into parquet format")
        persist_path = f'{i}'
        df.to_parquet(persist_path, engine="fastparquet", partition_cols=["product", "year_month"])
        logging.info("Data successfully saved")


def create_database_from_parquet():
    """ function which read the parquet file and create a database """
    
    sheets = {"DPCache_m3": "oil_derivative", "DPCache_m3_2": "diesel"}
    for i in sheets:
        logging.info(f"Creating database to insert data into table '{sheets[i]}'")
        df_parquet = pd.read_parquet(f"{i}")

        conn = sqlite3.connect(f"{sheets[i]}.db")
        cursor = conn.cursor()

        #dropping table
        logging.info(f"Dropping table '{sheets[i]}' if exists")
        cursor.execute(f"""DROP TABLE IF EXISTS {sheets[i]}""")
        conn.commit()
        logging.info(f"Table '{sheets[i]}' successfully dropped")

        #creating table
        logging.info(f"Creating table '{sheets[i]}'")
        cursor.execute(f""" CREATE TABLE IF NOT EXISTS {sheets[i]}
                        (uf VARCHAR(25), unit VARCHAR(25), volume DOUBLE, created_at TEXT, product VARCHAR(25), year_month VARCHAR(25))""")
        conn.commit()
        logging.info(f"Table '{sheets[i]}' successfylly created")

        #inserting data into table
        logging.info(f"Inserting data into table '{sheets[i]}'")
        for row in df_parquet.itertuples():
            cursor.execute(f"""INSERT INTO {sheets[i]}(uf, unit, volume, created_at, product, year_month)
                               VALUES("{row[1]}", "{row[2]}", {row[3]}, "{row[4]}", "{row[5]}", "{row[6]}")""")
        conn.commit()
        logging.info(f"Data successfully inserted into table '{sheets[i]}'")
        
        #checking select with pandas
        logging.info(f"Checking if the data has been inserted correctly into table '{sheets[i]}' with pandas")
        logging.info(f"SELECT * FROM {sheets[i]}")
        df_sql = pd.read_sql(f"SELECT * FROM {sheets[i]}", conn)
        logging.info("Visual check:")
        print(df_sql.head())
        logging.info(f"Check completed successfully")


with DAG('ETL_Raízen', start_date = datetime.datetime(2022,8,9),
           schedule_interval = '30 * * * *' , catchup = False) as dag:

    create_raw_data_dir = PythonOperator(
        task_id = 'create_raw_data_dir',
        python_callable = create_raw_data_dir
    )

    download_raw_data = PythonOperator(
        task_id = 'download_raw_data',
        python_callable = download_raw_data
    )

    convert_xls_into_xlsx = PythonOperator(
        task_id = 'convert_xls_into_xlsx',
        python_callable = convert_xls_into_xlsx
    )

    read_transform_and_save = PythonOperator(
        task_id = 'read_transform_and_save',
        python_callable = read_transform_and_save
    )

    create_database_from_parquet = PythonOperator(
        task_id = 'create_database_from_parquet',
        python_callable = create_database_from_parquet
    )

    create_raw_data_dir >> download_raw_data >> convert_xls_into_xlsx >> read_transform_and_save >> create_database_from_parquet
