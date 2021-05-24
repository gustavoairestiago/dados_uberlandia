import pandas as pd
import requests
import zipfile as os
from tqdm import tqdm

#função baixar arquivo

def baixar_arquivo(url, endereco):
    resposta = requests.get(url, stream=True)
    total_size_in_bytes= int(resposta.headers.get('content-length', 0))
    block_size = 1024 #1 Kibibyte
    progress_bar = tqdm(total=total_size_in_bytes, unit='iB', unit_scale=True)
    if resposta.status_code == requests.codes.OK:
        with open(endereco, 'wb') as novo_arquivo:
            for parte in resposta.iter_content(chunk_size=2000):
                progress_bar.update(len(parte))
                novo_arquivo.write(parte)
        print("Download finalizado. Arquivo salvo em: {}".format(endereco))
    else:
        resposta.raise_for_status()
    progress_bar.close()


#Download das bases
#https://www.gstatic.com/covid19/mobility/Region_Mobility_Report_CSVs.zip
baixar_arquivo("https://www.gstatic.com/covid19/mobility/Region_Mobility_Report_CSVs.zip","./Region_Mobility_Report_CSVs.zip")
mob_zip = os.ZipFile ("./Region_Mobility_Report_CSVs.zip")
mob_zip.extract("2020_BR_Region_Mobility_Report.csv",'./')
mob_zip.extract("2021_BR_Region_Mobility_Report.csv",'./')
mob_zip.close()

# Dados 2020
mob = pd.read_csv('./2020_BR_Region_Mobility_Report.csv',sep=',',infer_datetime_format=True,dayfirst=True,)
place_id= 'ChIJXWuNq7NFpJQRhR-c3jhmXZQ' 
mob_filtro_mun_2 = mob['place_id']==place_id
mob_udi_2=mob[mob_filtro_mun_2]
mob_udi_2['date'] = mob_udi_2['date'].astype("datetime64")
mob_udi_2.to_excel(excel_writer="./mob_udi_2020.xlsx",index_label="id")
    
#Dados 2021
mob2 = pd.read_csv('./2021_BR_Region_Mobility_Report.csv',sep=','
,infer_datetime_format=True,dayfirst=True,)
place_id= 'ChIJXWuNq7NFpJQRhR-c3jhmXZQ' 
mob_filtro_mun2 = mob2['place_id']==place_id
mob_udi2=mob2[mob_filtro_mun2]
mob_udi2['date'] = mob_udi2['date'].astype("datetime64")
mob_udi2.to_excel(excel_writer="./mob_udi_2021.xlsx",index_label="id")
    
#Junção das bases
mob_udi_uni = mob_udi2.append(mob_udi_2,ignore_index=True)
mob_udi_uni.to_excel(excel_writer="./mob_udi_2020_2021.xlsx",index_label="id")