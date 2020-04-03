from datetime import datetime

import requests
import pyuca
import pandas as pd
from tabula import read_pdf

def parse(bulletin_url, first_page, last_page, check=None, date=None, coord=False):
    # request to find the documents
    pdf = requests.get(bulletin_url)
    open('bulletin.pdf', 'wb').write(pdf.content)
    new_format = datetime.strptime(date, "%m-%d-%Y") >= datetime.strptime("04-01-2020", "%m-%d-%Y") 

    # parse DataFrame from pdf
    dfs = read_pdf("bulletin.pdf", stream=True, pages=list(range(first_page, last_page + 1)))
    columns = ['municipio', 'suspeito', 'confirmado']

    skip = 1 if new_format else 2
    data = pd.concat(pd.DataFrame(df.iloc[skip:,[0,1,-1]].replace("-",0).values, columns=columns) 
                     for df in dfs if df.shape[1] in [4,5])
    data = data[data["municipio"] != "RESIDÊNCIA"]
    data = data.reset_index(drop=True)

    # isolating local from imported cases
    total_rn_idx = data[data["municipio"] == "RN"].index.tolist()
    data_rn = data.loc[:total_rn_idx[0],]
    data_importados = data.loc[total_rn_idx[0] + 1:,]

    # fixing multirow lines from imported cases
    for index, row in data_importados[data_importados['municipio'].isna()].iterrows():
        data_importados.at[index-1, 'municipio'] = ' '.join(data_importados.loc[[index-1, index+1], "municipio"])
        data_importados.at[index-1, 'suspeito'] = data_importados.loc[index, "suspeito"]
        data_importados.at[index-1, 'confirmado'] = data_importados.loc[index, "confirmado"]
    data_importados = data_importados[~(data_importados["suspeito"].isna() & data_importados["confirmado"].isna())]
    data_importados = data_importados.dropna(subset=["municipio"]).fillna(0)

    # verifying imported cases match
    data_importados["confirmado"] = data_importados["confirmado"].astype(int)
    data_importados["suspeito"] = data_importados["suspeito"].astype(int)
    total_importados = data_importados.iloc[-1,]
    data_importados = data_importados.iloc[:-1,]
    
    if not all(sum(data_importados[feature]) == total_importados[feature] for feature in ["confirmado", "suspeito"]):
        print("Atenção (casos importados)! O total raspado não condiz com o total informado no boletim!")
        for feature in ["confirmado", "suspeito"]:
            print(f"{feature}: {sum(data_importados[feature])} (raspado), {total_importados[feature]} (boletim)")

    # aggregating all imported cases
    data_importados["municipio"] = "Importados"
    data_importados = data_importados.pivot_table(index="municipio", 
                                                  values=["suspeito", "confirmado"],
                                                  aggfunc="sum").reset_index()
    
    # fixing multirow lines from local cases
    for index, row in data_rn[data_rn['municipio'].isna()].iterrows():
        if not new_format:
            data_rn.at[index-1, 'municipio'] = ' '.join(data_rn.loc[[index-1, index+1], "municipio"])
            data_rn.at[index-1, 'suspeito'] = data_rn.loc[index, "suspeito"]
            data_rn.at[index-1, 'confirmado'] = data_rn.loc[index, "confirmado"]
        if new_format:
            data_rn.at[index, 'municipio'] = data_rn.loc[index + 1, "municipio"]
    data_rn = data_rn[~(data_rn["suspeito"].isna() & data_rn["confirmado"].isna())]
    data_rn = data_rn.dropna(subset=["municipio"]).fillna(0)
    
    # verifying local cases match
    data_rn["confirmado"] = data_rn["confirmado"].astype(int)
    data_rn["suspeito"] = data_rn["suspeito"].astype(int)
    total_rn = data_rn.iloc[-1,]
    data_rn = data_rn.iloc[:-1,]
    
    if not all(sum(data_rn[feature]) == total_rn[feature] for feature in ["confirmado", "suspeito"]):
        print("Atenção (casos locais)! O total raspado não condiz com o total informado no boletim!")
        for feature in ["confirmado", "suspeito"]:
            print(f"{feature}: {sum(data_rn[feature])} (raspado), {total_rn[feature]} (boletim)")
    
    data = data_rn.reset_index(drop=True)
    
    # fixing city names
    data.loc[data["municipio"] == "Augusto Severo", "municipio"] = "Campo Grande"
    data.loc[data["municipio"] == "Governado Dix-Sep Rosado", "municipio"] = "Governador Dix-Sept Rosado"
    data.loc[data["municipio"] == "Governador Dix-Sept", "municipio"] = "Governador Dix-Sept Rosado"
    data.loc[data["municipio"] == "Lagoa d’Anta", "municipio"] = "Lagoa d'Anta"
    data.loc[data["municipio"] == "Santana dos Matos", "municipio"] = "Santana do Matos"
    data.loc[data["municipio"] == "São José do Mipibú", "municipio"] = "São José de Mipibu"
    if "Assú" in data["municipio"].unique():
        data.loc[data["municipio"] == "Assú", "municipio"] = "Açu"

    # verifying against manually collected data
    data = data.query("suspeito > 0 or confirmado > 0")
    data = data.reset_index(drop=True)

    # checking against existing CSVs
    base_url = "https://raw.githubusercontent.com/leobezerra/covid19/master/data/rn_covid_19_boletins"
    if check:
        df_old = pd.read_csv(f"{base_url}/{date}.csv").query("suspeito > 0 or confirmado > 0")
        data_mun = set(data["municipio"].unique())
        old_mun = set(df_old["municipio"].unique())
        if data_mun != old_mun:
            print("Atenção! Os municípios raspados e de referência não batem")
            print("- Não estão no CSV de referência: ", data_mun - old_mun)
            print("- Não estão nos dados raspados: ", old_mun - data_mun)
            exit()
        for m in df_old.municipio:
            if df_old[df_old['municipio'] == m]['confirmado'].iloc[0] != data[data['municipio'] == m]['confirmado'].iloc[0]:
                print(m)
                print(df_old[df_old['municipio'] == m]['confirmado'].iloc[0], data[data['municipio'] == m]['confirmado'].iloc[0])
                exit()
    
    # adding latitude and longitude data
    if coord:
        coord_rn = pd.read_csv(coord)
        data_mun = set(data["municipio"].unique())
        coord_mun = set(coord_rn["municipio"].unique())
        print("Atenção! Os municípios raspados e do arquivo de coordenadas não batem")
        print("- Não estão no CSV de coordenadas: ", data_mun - coord_mun)
        print("- Não estão nos dados raspados: ", coord_mun - data_mun)

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

    # aggregating all cases back
    data = pd.concat([data, data_importados]).reset_index(drop=True)
    data.iloc[-1,1] = datetime.strptime(date, "%m-%d-%Y").strftime("%Y-%m-%d") 

    # persisting
    data.to_csv(f"{date}.csv", index=False)


if __name__ == "__main__":
    data = [
           # {"date": "03-24-2020", "first_page": 9, "last_page": 11, "bulletin": "http://www.adcon.rn.gov.br/ACERVO/sesap/DOC/DOC000000000227775.PDF"},
           # {"date": "03-25-2020", "first_page": 8, "last_page": 11, "bulletin": "http://www.adcon.rn.gov.br/ACERVO/sesap/DOC/DOC000000000227985.PDF"},
           # {"date": "03-27-2020", "first_page": 9, "last_page": 12, "bulletin": "http://www.adcon.rn.gov.br/ACERVO/sesap/DOC/DOC000000000228049.PDF"},
           # {"date": "03-28-2020", "first_page": 8, "last_page": 11, "bulletin": "http://www.adcon.rn.gov.br/ACERVO/sesap/DOC/DOC000000000228113.PDF"},
           # {"date": "03-30-2020", "first_page": 8, "last_page": 10, "bulletin": "http://www.adcon.rn.gov.br/ACERVO/sesap/DOC/DOC000000000228171.PDF"},
           # {"date": "04-01-2020", "first_page": 8, "last_page": 13, "bulletin": "http://www.adcon.rn.gov.br/ACERVO/sesap/DOC/DOC000000000228342.PDF"},        
    ]

    base_url = "rn_covid_19_boletins"
    for pair in data:
        date, first_page, last_page, bulletin_url = pair.values()
        parse(bulletin_url, first_page, last_page, check=False, date=f"{date}", coord="coordenadas-rn.csv")
