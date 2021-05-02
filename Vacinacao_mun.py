import pandas as pd
import requests
import bs4

#função baixar arquivo
def baixar_arquivo(url, endereco):
    resposta = requests.get(url, stream=True)
    if resposta.status_code == requests.codes.OK:
        with open(endereco, 'wb') as novo_arquivo:
                for parte in resposta.iter_content(chunk_size=2000):
                    novo_arquivo.write(parte)
        print("Download finalizado. Arquivo salvo em: {}".format(endereco))
    else:
        resposta.raise_for_status()

#função agrupar idades       
def agrupar_idade(maior,menor,coluna,novacol):
    if maior >= 80:
        df_mun_ref.loc[(df_mun_ref['paciente_idade'] >= 80) ,"grupo_idade"] = "80 anos ou mais"
    else:
        df_mun_ref.loc[(maior <= df_mun_ref[coluna])&(df_mun_ref[coluna] <= menor) ,novacol] = "{} a {} anos".format(maior,menor)
        
URL = 'https://opendatasus.saude.gov.br/dataset/covid-19-vacinacao/resource/ef3bd0b8-b605-474b-9ae5-c97390c197a8'
requests.get(URL)

#Transforma o html em texto
web_page = bs4.BeautifulSoup(requests.get(URL, {}).text, "lxml")
web_page

#Busca e Seleciona o node 
sub_web_page = web_page.find(name="div", attrs={"class":"prose notes"})
links = sub_web_page.find_all(name='a') #não utilizado 

#coleta o nome e o link referenciado 
nome=[str(wp.text.strip("\n").replace(",", "")) 
    for wp in sub_web_page.find_all("a")]
link = [link.get('href') for link in sub_web_page.find_all('a')]

#coloca em dicionário
dict_links = dict(zip(nome, link))
dict_links
#Download da planilha do Estado selecionado
UF = "MG" #Colocar sigla da UF desejada
UF2='Dados '+UF
cod_ibge=317020 # Colocar cod_ibge cidade desejada
baixar_arquivo(dict_links[UF2], "D:/Gustavo/Drive/Uberlândia/Dados_udi/Vacinação {}.csv".format(UF2))

# Filtrando por Município - Insira o Código IBGE do município
df_mun_ret = pd.DataFrame()

for chunk in pd.read_csv("D:/Gustavo/Drive/Uberlândia/Dados_udi/Vacinação {}.csv".format(UF2),sep=';',chunksize = 100000,infer_datetime_format=True,dayfirst=True):
  df_filtro_mun = chunk['estabelecimento_municipio_codigo']==cod_ibge
  df_mun=chunk[df_filtro_mun]
  df_mun_ret = df_mun_ret.append(df_mun)

df_mun_ref = df_mun_ret #quando quiser voltar as alterações

# Nova Data com Split das coluna vacina_dataAplicacao separado por T
data1 = df_mun_ref["vacina_dataAplicacao"].str.split("T", n = 1, expand = True)

# Criando a Nova Coluna "nome" com o new[0]
df_mun_ref["vacina_data_aplic"]= data1[0] 

# Retirando a antiga coluna "Name" 
df_mun_ref.drop(columns =["vacina_dataAplicacao"], inplace = True)

#convertendo datas
def transformar_em_data(col, formato):
    df_mun_ref['vacina_data_aplic'] = df_mun_ref['vacina_data_aplic'].astype("datetime64")
df_mun_ref['vacina_data_aplic'] = df_mun_ref['vacina_data_aplic'].astype("datetime64")
df_mun_ref['data_importacao_rnds'] = df_mun_ref['data_importacao_rnds'].astype("datetime64")
df_mun_ref['paciente_dataNascimento'] = df_mun_ref['paciente_dataNascimento'].astype("datetime64")

#alterar idades de acordo com faixas
df_mun_ref.loc[(df_mun_ref['paciente_idade'] < 80) & (df_mun_ref['vacina_grupoAtendimento_nome'] == 'Pessoas de 80 anos ou mais'),"paciente_idade"] = 80
df_mun_ref.loc[(df_mun_ref['paciente_idade'] < 75) & (df_mun_ref['vacina_grupoAtendimento_nome'] == 'Pessoas de 75 a 79 anos'),"paciente_idade"] = 75
df_mun_ref.loc[(df_mun_ref['paciente_idade'] < 70) & (df_mun_ref['vacina_grupoAtendimento_nome'] == 'Pessoas de 70 a 74 anos'),"paciente_idade"] = 70
df_mun_ref.loc[(df_mun_ref['paciente_idade'] < 65) & (df_mun_ref['vacina_grupoAtendimento_nome'] == 'Pessoas de 65 a 69 anos'),"paciente_idade"] = 65
df_mun_ref.loc[(df_mun_ref['paciente_idade'] < 60) & (df_mun_ref['vacina_grupoAtendimento_nome'] == 'Pessoas de 60 a 64 anos'),"paciente_idade"] = 60
df_mun_ref.loc[(df_mun_ref['paciente_idade'] < 60) & (df_mun_ref['vacina_grupoAtendimento_nome'] == 'Pessoas de 60 nos ou mais Institucionalizadas'),"paciente_idade"] = 60
df_mun_ref.loc[(df_mun_ref['paciente_idade'] < 18) ,"paciente_idade"] = 18

#cria listas com o range da idade
r1 = list(range(0,max(df_mun_ref['paciente_idade']),5))
r2 = list(range(4,max(df_mun_ref['paciente_idade']),5))
tam=min(len(r1),len(r2)) #tamanho da lista de idades
for i in range(tam): # percorre cada range entre as idades r1 e r2
    agrupar_idade(r1[i], r2[i],'paciente_idade',"grupo_idade")
df_mun_ref.loc[(df_mun_ref['paciente_idade'] >= 80) ,"grupo_idade"] = "80 anos ou mais"

#Retira as duplicadas (controlando por id paciente, dia de vacinação dose e lote)
df_mun_ref = df_mun_ref.drop_duplicates(['vacina_lote','paciente_id','paciente_dataNascimento','paciente_endereco_cep','vacina_descricao_dose','vacina_data_aplic'])
#salvar nesse caminho
df_mun_ref.to_excel(excel_writer="D:/Gustavo/Drive/Uberlândia/Dados_udi/mun_vacinacao.xlsx", index_label="id") 