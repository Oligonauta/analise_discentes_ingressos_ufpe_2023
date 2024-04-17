# Conhecendo os dados

## Importando bibliotecas
#%%
import pandas as pd
import seaborn as sns

## Extraindo os dados
#%%
dados = pd.read_csv("discentes-ingressos-sisu-2023-ufpe.csv")
dados.head()

## Conhecendo o conjunto de dados
#%%
dados.info()

#%%
dados.columns

### Observando os 5 primeiros valores únicos de cada variável
#%%
for var in dados.columns:
    print(f"{var}: \n",dados[var].unique()[:5],"\n")

## Seleção de variáveis e pré-processamento de dados
#%%
dados = dados.drop(["COD_CURSO","UF_ENDERECO","BAIRRO_ENDERECO","CIDADE_ENDERECO","TIPO_COTA","ESCOLARIDADE_PAI","ESCOLARIDADE_MAE","DEFICIENCIA"],axis=1)
dados.head()

# %%
dados["CURSO"] = dados["CURSO"].str.replace(" -CAA","")
dados["CURSO"].head()
# %%
dados.head()

#%%
dados["IDADE_INGRESSO"] = dados["ANO_INGRESSO"] - dados["ANO_NASC"]
dados.head()
dados = dados.drop("ANO_NASC", axis=1)
# %%
variaveis_periodo = ["TIPO_INGRESSO", "ANO_INGRESSO","SEMESTRE_INGRESSO"]

for var in variaveis_periodo:
    dados[var] = dados[var].astype("str")

dados.dtypes
# %%
dados["INGRESSO"] = dados[variaveis_periodo].apply("-".join, axis=1)
dados.head()
# %%
dados = dados.drop(variaveis_periodo, axis=1)
dados.head()
#%%
colunas = ['INGRESSO','ID_DISCENTE', 'IDADE_INGRESSO', 'SEXO','COTA','PAIS_NATURALIDADE','UF_NATURALIDADE','NATURALIDADE','CAMPUS', 'CURSO']
dados = dados[colunas]
dados.head()
# %%
dados.to_parquet("base_tratada_discentes_ingressos_sisu_ufpe_2023.parquet")
#%%