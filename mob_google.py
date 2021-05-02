import pandas as pd
import requests
import zipfile as os


#função baixar arquivo
def baixar_arquivo(url, endereco):
    resposta = requests.get(url, stream=True)
    if resposta.status_code == requests.codes.OK:
        with open(endereco, 'wb') as novo_arquivo:
                for parte in resposta.iter_content(chunk_size=10000):
                    novo_arquivo.write(parte)
        print("Download finalizado. Arquivo salvo em: {}".format(endereco))
    else:
        resposta.raise_for_status()
#Download das bases
#https://www.gstatic.com/covid19/mobility/Region_Mobility_Report_CSVs.zip
baixar_arquivo("https://www.gstatic.com/covid19/mobility/Region_Mobility_Report_CSVs.zip","D:/Gustavo/Drive/Uberlândia/Dados_udi/Region_Mobility_Report_CSVs.zip")
mob_zip = os.ZipFile ("D:/Gustavo/Drive/Uberlândia/Dados_udi/Region_Mobility_Report_CSVs.zip")
mob_zip.extract("2020_BR_Region_Mobility_Report.csv",'D:/Gustavo/Drive/Uberlândia/Dados_udi')
mob_zip.extract("2021_BR_Region_Mobility_Report.csv",'D:/Gustavo/Drive/Uberlândia/Dados_udi')
mob_zip.close()

# Dados 2020
mob = pd.read_csv('D:/Gustavo/Drive/Uberlândia/Dados_udi/2020_BR_Region_Mobility_Report.csv',sep=',',infer_datetime_format=True,dayfirst=True,)
place_id= 'ChIJXWuNq7NFpJQRhR-c3jhmXZQ' 
mob_filtro_mun_2 = mob['place_id']==place_id
mob_udi_2=mob[mob_filtro_mun_2]
mob_udi_2['date'] = mob_udi_2['date'].astype("datetime64")
mob_udi_2.to_excel(excel_writer="D:/Gustavo/Drive/Uberlândia/Dados_udi/mob_udi_2020.xlsx",index_label="id")
    
#Dados 2021
mob2 = pd.read_csv('D:/Gustavo/Drive/Uberlândia/Dados_udi/2021_BR_Region_Mobility_Report.csv',sep=','
,infer_datetime_format=True,dayfirst=True,)
place_id= 'ChIJXWuNq7NFpJQRhR-c3jhmXZQ' 
mob_filtro_mun2 = mob2['place_id']==place_id
mob_udi2=mob2[mob_filtro_mun2]
mob_udi2['date'] = mob_udi2['date'].astype("datetime64")
mob_udi2.to_excel(excel_writer="D:/Gustavo/Drive/Uberlândia/Dados_udi/mob_udi_2021.xlsx",index_label="id")
    
#Junção das bases
mob_udi_uni = mob_udi2.append(mob_udi_2,ignore_index=True)
mob_udi_uni.to_excel(excel_writer="D:/Gustavo/Drive/Uberlândia/Dados_udi/mob_udi_2020_2021.xlsx",index_label="id")