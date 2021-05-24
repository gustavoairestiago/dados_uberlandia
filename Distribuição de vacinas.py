import pandas as pd
import requests
import bs4
from tqdm import tqdm

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
        
URL = 'https://coronavirus.saude.mg.gov.br/vacinometro'
requests.get(URL)

web_page = bs4.BeautifulSoup(requests.get(URL, {}).text, "lxml")
web_page

#Busca e Seleciona o node 
sub_web_page = web_page.find(name="div", attrs={"itemprop":"articleBody"})
links = sub_web_page.find_all(name='a') #não utilizado 

#coleta o nome e o link referenciado 
nome=[str(wp.text.strip("\n").replace(",", "")) 
    for wp in sub_web_page.find_all("a")]
link = [link.get('href') for link in sub_web_page.find_all('a')]
link[0] = 'https://coronavirus.saude.mg.gov.br' + link[0]
link[1] = 'https://coronavirus.saude.mg.gov.br' + link[1]

#coloca em dicionário
dict_links = dict(zip(nome, link))
baixar_arquivo(dict_links['XLSX Distribuição'], "./Vacinacao_distr.csv")

df =pd.read_excel('./Vacinacao_distr.csv', sheet_name= 'SIES_MUN')
filt = df['MUNICIPIO']=='UBERLANDIA'
df= df[filt]
df['DATA'] = pd.to_datetime(df['DATA'],format="%d/%m/%Y",dayfirst=True)
df['VALIDADE'] = pd.to_datetime(df['VALIDADE'],format="%d/%m/%Y",dayfirst=True)
df.to_excel(excel_writer = "./Vacinacao_distr_mun.xlsx") 
