# Origem dos dados: https://dados.ufpe.br/dataset/inscricoes-sisu/resource/35934b52-6278-46c8-b055-743626d9e8f5

## Importando biblioteca
#%%
import pandas as pd
import numpy as np
import urllib.request as req
import json

# Extraindo os dados da API de dados abertos da UFPE
#%%
url = 'https://dados.ufpe.br/api/3/action/datastore_search?limit=100000&resource_id=35934b52-6278-46c8-b055-743626d9e8f5'
response = req.urlopen(url)
registros_api = response.read()
registros_api_json = json.loads(registros_api)
dados = pd.json_normalize(registros_api_json["result"]["records"])
dados.head()

# Conhecendo o conjunto de dados

#%%
dados.info()

#%%
dados.columns

## Observando os 5 primeiros valores únicos de cada variável
#%%
for var in dados.columns:
    print(f"{var}: \n",dados[var].unique()[:5],"\n")

## Seleção de variáveis de interesse
#%%
dados = dados.drop(["COD_CURSO","BAIRRO_ENDERECO","TIPO_COTA","ESCOLARIDADE_PAI","ESCOLARIDADE_MAE","DEFICIENCIA"],axis=1)
dados.head()

# Transformações e Feature Engineering

## Padronizando a escrita dos dados de curso
# %%
dados["CURSO"] = dados["CURSO"].str.replace(" -CAA","")
dados["CURSO"].head()

# %%
dados.head()

## Criação da feature idade_ingresso, representando a idade do individuo ao ingressar
#%%
dados["IDADE_INGRESSO"] = dados["ANO_INGRESSO"] - dados["ANO_NASC"]
dados.head()
dados = dados.drop("ANO_NASC", axis=1)

## Criação da feature ingresso, representando uma concatenação das informações das variáveis de período
# %%
variaveis_periodo = ["TIPO_INGRESSO", "ANO_INGRESSO","SEMESTRE_INGRESSO"]

for var in variaveis_periodo:
    dados[var] = dados[var].astype("str")

dados.dtypes
# %%
dados["INGRESSO"] = dados[variaveis_periodo].apply("-".join, axis=1)
dados.head()

## Retirada das variáveis de período
# %%
dados = dados.drop(variaveis_periodo, axis=1)
dados.head()

## Reordenação das colunas
#%%
colunas = ['INGRESSO', "ID_DISCENTE",'IDADE_INGRESSO', 'SEXO','COTA','PAIS_NATURALIDADE','UF_NATURALIDADE','UF_ENDERECO', 'NATURALIDADE','CIDADE_ENDERECO','CAMPUS', 'CURSO']
dados = dados[colunas]
dados.head()

## Verificando e tratando valores nulos e valores únicos
#%%
dados.isna().sum()

#%%
for col in dados.columns:
    if col != "ID_DISCENTE":
        print(f"{col}:\n",dados[col].unique()[:10],"\n")

## Tratamento das variáveis regionais com valores nulos. Acrescentando o valor das informações de endereço nas informações de naturalidade que estão vazias
#%%
variaveis_regionais = ['PAIS_NATURALIDADE','UF_NATURALIDADE','UF_ENDERECO', 'NATURALIDADE','CIDADE_ENDERECO']

for col in variaveis_regionais:
    print(f"{col}:\n",dados[col].unique(),"\n")
#%%
sem_regional = dados[dados["PAIS_NATURALIDADE"]==""]

discentes_sem_regional = []

for discente in sem_regional["ID_DISCENTE"]:
    discentes_sem_regional.append(discente)
    
print(discentes_sem_regional)

#%%
dados = dados.replace('',np.nan,regex = True)
dados.isna().sum()

dados["PAIS_NATURALIDADE"] = dados["PAIS_NATURALIDADE"].fillna("BRA")
dados["UF_NATURALIDADE"] = dados["UF_NATURALIDADE"].fillna(dados["UF_ENDERECO"])
dados["NATURALIDADE"] = dados["NATURALIDADE"].fillna(dados['CIDADE_ENDERECO'])

dados.isna().sum()

## Validação do tratamento das variáveis regionais
#%%
dados_discentes_sem_regional_tratado = dados[dados["ID_DISCENTE"].isin(discentes_sem_regional)]
dados_discentes_sem_regional_tratado
#%%
for col in variaveis_regionais:
    print(f"{col}:\n",dados[col].unique(),"\n")

## Retirada das variáveis de suporte
#%%
dados = dados.drop(["UF_ENDERECO","CIDADE_ENDERECO","ID_DISCENTE"], axis=1)
dados.head()

## Exportação do arquivo parquet, disponibilizado para análise dos dados
#%%
dados.to_parquet("base_tratada_discentes_ingressos_sisu_ufpe_2023.parquet")
#%%
