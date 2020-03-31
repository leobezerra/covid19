import sys
import os
from datetime import datetime

import requests
import pyuca
import pandas as pd
from camelot import read_pdf


def parse(bulletin_url, first_page, last_page, check=None, date=None, coord=False):
    # request to find the documents
    pdf = requests.get(bulletin_url)
    open('bulletin.pdf', 'wb').write(pdf.content)

    # parse DataFrame from pdf
    pages = ', '.join(str(page) for page in range(first_page, last_page + 1))
    tables = read_pdf("bulletin.pdf", pages=pages)

    columns = ['municipio', 'suspeito', 'confirmado']
    data = pd.concat(
        pd.DataFrame(
            table.df.iloc[1:, [0, 1, 4]].replace("-", 0).replace("", 0).values,
            columns=columns,
        )
        for table in tables
        if table.df.shape[1] == 5
    )

    data = data.reset_index(drop=True)

    # checksum the data
    data = data.fillna(0)
    data["suspeito"] = data["suspeito"].astype("int")
    data["confirmado"] = data["confirmado"].astype("int")

    total = data.iloc[-1,] 
    data = data.iloc[:-1,]

    if not all(sum(data[feature]) == total[feature] for feature in ["confirmado", "suspeito"]):
        print("Atenção! O total raspado não condiz com o total informado no boletim!")
        for feature in ["confirmado", "suspeito"]:
            print(f"{feature}: {sum(data[feature])} (raspado), {total[feature]} (boletim)")

    data['municipio'] = data['municipio'].map(lambda x: str(x).replace('\n', ' ')) 

    # fixing city names
    data.loc[data["municipio"] == "Governado Dix-Sep Rosado", "municipio"] = "Governador Dix-Sept Rosado"
    data.loc[data["municipio"] == "Lagoa d’Anta", "municipio"] = "Lagoa d'Anta"
    data.loc[data["municipio"] == "Santana dos Matos", "municipio"] = "Santana do Matos"
    if "Assú" in data["municipio"].unique():
        data.loc[data["municipio"] == "Assú", "municipio"] = "Açu"

    # verifying against manually collected data
    data["confirmado"] = data["confirmado"].astype(int)
    data["suspeito"] = data["suspeito"].astype(int)
    data = data.query("suspeito > 0 or confirmado > 0")
    data = data.reset_index(drop=True)

    # checking against existing CSVs
    base_url = "https://raw.githubusercontent.com/leobezerra/covid19/master/data/rn_covid_19_boletins"
    if check:
        df_old = pd.read_csv(f"{base_url}/{date}.csv").query("suspeito > 0 or confirmado > 0")
        data_mun = set(data["municipio"].unique())
        old_mun = set(df_old["municipio"].unique())
        print("Atenção! Os municípios raspados e de referência não batem")
        if data_mun != old_mun:
            print("Não estão no CSV de referência: ", data_mun - old_mun)
            print("Não estão nos dados raspados: ", old_mun - data_mun)
            exit()
        for m in df_old.municipio:
            if df_old[df_old['municipio'] == m]['confirmado'].iloc[0] != data[data['municipio'] == m]['confirmado'].iloc[0]:
                print(m)
                print(df_old[df_old['municipio'] == m]['confirmado'].iloc[0], data[data['municipio'] == m]['confirmado'].iloc[0])
                exit()
    
    # adding latitude and longitude data
    if coord:
        coord_rn = pd.read_csv(coord)
        data = pd.merge(data, coord_rn, how="right").fillna(0)
        data["confirmado"] = data["confirmado"].astype(int)
        data["suspeito"] = data["suspeito"].astype(int)

    # adding date
    data = data.assign(data=datetime.strptime(date, "%m-%d-%Y").strftime("%Y-%m-%d"))
    data = pd.DataFrame(data, columns="municipio,data,confirmado,suspeito,lat,lon".split(","))

    # Pandas is not pythonic
    coll = pyuca.Collator()
    df_municipios = pd.DataFrame(sorted(data["municipio"], key=coll.sort_key), columns=["mun"])
    data = pd.merge(df_municipios, data, left_on="mun", right_on="municipio").drop("mun", axis=1)

    # persisting
    data.to_csv(f"{date}.csv", index=False)


if __name__ == "__main__":
    data = [
           # {"date": "03-24-2020", "first_page": 9, "last_page": 11, "bulletin": "http://www.adcon.rn.gov.br/ACERVO/sesap/DOC/DOC000000000227775.PDF"},
           # {"date": "03-25-2020", "first_page": 8, "last_page": 10, "bulletin": "http://www.adcon.rn.gov.br/ACERVO/sesap/DOC/DOC000000000227985.PDF"},
           # {"date": "03-27-2020", "first_page": 9, "last_page": 11, "bulletin": "http://www.adcon.rn.gov.br/ACERVO/sesap/DOC/DOC000000000228049.PDF"},
           # {"date": "03-28-2020", "first_page": 8, "last_page": 10, "bulletin": "http://www.adcon.rn.gov.br/ACERVO/sesap/DOC/DOC000000000228113.PDF"},
           {"date": "03-30-2020", "first_page": 8, "last_page": 10, "bulletin": "http://www.adcon.rn.gov.br/ACERVO/sesap/DOC/DOC000000000228113.PDF"},
           ]
            
    base_url = "rn_covid_19_boletins"
    for pair in data:
        date, first_page, last_page, bulletin_url = pair.values()
        parse(bulletin_url, first_page, last_page, check=False, date=f"{date}", coord="coordenadas-rn.csv")
