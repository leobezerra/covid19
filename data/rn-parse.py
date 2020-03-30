import sys
import os
from datetime import datetime

import requests
import pyuca
import pandas as pd
from tabula import read_pdf

def parse(bulletin_url, first_page, last_page, check=None, date=None, coord=False):
    # request to find the documents
    pdf = requests.get(bulletin_url)
    open('bulletin.pdf', 'wb').write(pdf.content)

    # parse DataFrame from pdf
    dfs = read_pdf("bulletin.pdf", stream=True, pages=list(range(first_page, last_page + 1)))
    columns = ['municipio', 'suspeito', 'confirmado']
    data = pd.concat(pd.DataFrame(df.iloc[2:,[0,1,4]].replace("-",0).values, columns=columns) 
                     for df in dfs if df.shape[1] == 5)
    data = data.reset_index(drop=True)

    # checksum the data
    data_checksum = data.fillna(0)
    data_checksum["suspeito"] = data_checksum["suspeito"].astype("int")
    data_checksum["confirmado"] = data_checksum["confirmado"].astype("int")

    total = data_checksum.iloc[-1,]
    data_checksum = data_checksum.iloc[:-1,]
    data = data.iloc[:-1,]

    if not all(sum(data_checksum[feature]) == total[feature] for feature in ["confirmado", "suspeito"]):
        for feature in ["confirmado", "suspeito"]:
            print(sum(data_checksum[feature]), total[feature])
        exit()

    # fix multirow lines
    drop_lines = []
    for index, row in data[data['municipio'].isnull()].iterrows():
        #print(' '.join(data.iloc[[index-1, index+1], 0]))
        data.at[index-1, 'municipio'] = ' '.join(data.iloc[[index-1, index+1], 0])
        data.at[index-1, 'suspeito'] = data.iloc[index, 1]
        data.at[index-1, 'confirmado'] = data.iloc[index, 2]
        drop_lines.extend([index, index+1])

    data = data.drop(drop_lines).fillna(0)

    # fixing city names
    data.loc[data["municipio"] == "Governado Dix-Sep Rosado", "municipio"] = "Governador Dix-Sept Rosado"
    data.loc[data["municipio"] == "Lagoa d’Anta", "municipio"] = "Lagoa d'Anta"
    data.loc[data["municipio"] == "Santana dos Matos", "municipio"] = "Santana do Matos"

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
        if data_mun != old_mun:
            print("Not in the CSV file: ", data_mun - old_mun)
            print("Not in the scraped data: ", old_mun - data_mun)
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
           {"date": "03-24-2020", "first_page": 9, "last_page": 11, "bulletin": "http://www.adcon.rn.gov.br/ACERVO/sesap/DOC/DOC000000000227775.PDF"},
           {"date": "03-25-2020", "first_page": 8, "last_page": 10, "bulletin": "http://www.adcon.rn.gov.br/ACERVO/sesap/DOC/DOC000000000227985.PDF"},
           {"date": "03-27-2020", "first_page": 9, "last_page": 11, "bulletin": "http://www.adcon.rn.gov.br/ACERVO/sesap/DOC/DOC000000000228049.PDF"},
           {"date": "03-28-2020", "first_page": 8, "last_page": 10, "bulletin": "http://www.adcon.rn.gov.br/ACERVO/sesap/DOC/DOC000000000228113.PDF"},
           ]
            
    base_url = "rn_covid_19_boletins"
    for pair in data:
        date, first_page, last_page, bulletin_url = pair.values()
        parse(bulletin_url, first_page, last_page, check=False, date=f"{date}", coord="coordenadas-rn.csv")
