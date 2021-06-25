import ipdb
import requests
import pandas as pd
import zipfile
import os
import shutil
import pathlib

from . import misc

URL = 'http://agencia.tse.jus.br/estatistica/sead/odsele/consulta_cand'
FILE = 'consulta_cand'

BASE_HEADER = [
    'DATA_GERACAO', 'HORA_GERACAO', 'ANO_ELEICAO', 'NUM_TURNO', 'DESCRICAO_ELEICAO',
    'SIGLA_UF', 'SIGLA_UE', 'DESCRICAO_UE', 'CODIGO_CARGO', 'DESCRICAO_CARGO', 'NOME_CANDIDATO',
    'SEQUENCIAL_CANDIDATO', 'NUMERO_CANDIDATO', 'CPF_CANDIDATO', 'NOME_URNA_CANDIDATO',
    'COD_SITUACAO_CANDIDATURA', 'DES_SITUACAO_CANDIDATURA', 'NUMERO_PARTIDO', 'SIGLA_PARTIDO',
    'NOME_PARTIDO', 'CODIGO_LEGENDA', 'SIGLA_LEGENDA', 'COMPOSICAO_LEGENDA', 'NOME_LEGENDA',
    'CODIGO_OCUPACAO', 'DESCRICAO_OCUPACAO', 'DATA_NASCIMENTO', 'NUM_TITULO_ELEITORAL_CANDIDATO',
    'IDADE_DATA_ELEICAO', 'CODIGO_SEXO', 'DESCRICAO_SEXO', 'COD_GRAU_INSTRUCAO', 'DESCRICAO_GRAU_INSTRUCAO',
    'CODIGO_ESTADO_CIVIL', 'DESCRICAO_ESTADO_CIVIL'
]

FIM_HEADER = [
    'CODIGO_NACIONALIDADE', 'DESCRICAO_NACIONALIDADE',
    'SIGLA_UF_NASCIMENTO', 'CODIGO_MUNICIPIO_NASCIMENTO', 'NOME_MUNICIPIO_NASCIMENTO',
    'DESPESA_MAX_CAMPANHA', 'COD_SIT_TOT_TURNO', 'DESC_SIT_TOT_TURNO'
]

RECURSO = 'candidatos'

def getCandidatos(ano_eleicao, download_path, out_path= './data'):
    lista = []

    print(f'# Processando candidatos das eleições de {ano_eleicao}')

    url = misc.gera_url(URL, FILE, ano_eleicao)
    filename = url.split('/')[-1]

    misc.download_and_retry(url, download_path, filename)
    
    if os.path.isfile(download_path + filename):
        prefix = filename.split('.zip')[0]

        with zipfile.ZipFile(download_path + filename, 'r') as zip_ref:
            print(f'\t# Extraindo {filename}')
            zip_ref.extractall(path=f'{download_path}/extracted/{RECURSO}/{ano_eleicao}')

        cabecalho = BASE_HEADER + FIM_HEADER

        if ano_eleicao == 2012:
            cabecalho = cabecalho + ['NM_EMAIL']

        if ano_eleicao >= 2014:
            cabecalho = BASE_HEADER + ['CODIGO_COR_RACA', 'DESCRICAO_COR_RACA'] + FIM_HEADER + ['NM_EMAIL']

        currentDirectory = pathlib.Path(f'{download_path}/extracted/{RECURSO}/{ano_eleicao}')
        patterns = ['*.txt', '*.csv']

        files = []
        for p in patterns:
            for file in currentDirectory.glob(p):
                files.append(file)

        for file in sorted(files):
            print(f'\t\t# Carregando {file}')
            if ano_eleicao >= 2014:
                _resultado = pd.read_csv(file, sep=';', encoding='latin1', na_values=['#NULO#', '#NULO', '#NE#', '#NE'], dtype='object')
            else:
                _resultado = pd.read_csv(file, sep=';', encoding='latin1', na_values=['#NULO#', '#NULO', '#NE#', '#NE'], names=cabecalho, dtype='object')
            _resultado = _resultado.iloc[:-1, :]

            lista.append(_resultado)

        # Deleta os arquivos extraidos
        print(f'\t# Deletando diretorio {download_path}/extracted/{RECURSO}/{ano_eleicao}')
        shutil.rmtree(f'{download_path}/extracted/{RECURSO}/{ano_eleicao}')

        resultado_eleicoes = pd.concat(lista, ignore_index=True)

        # Padroniza cabecalhos
        _ccompleto = BASE_HEADER + ['CODIGO_COR_RACA', 'DESCRICAO_COR_RACA'] + FIM_HEADER + ['NM_EMAIL']
        cabecalho_restantes = list(set(_ccompleto).difference(set(cabecalho)))
        resultado_eleicoes = pd.concat([resultado_eleicoes, pd.DataFrame(columns=cabecalho_restantes)], sort=False)

        misc.mkdir(f'{out_path}/{ano_eleicao}')
        
        print(f'\t# Escrevendo {out_path}/{ano_eleicao}/candidatos.csv\n')
        resultado_eleicoes.to_csv(f'{out_path}/{ano_eleicao}/candidatos.csv', index=False, encoding='utf-8', sep='|')
