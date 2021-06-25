import requests
import pandas as pd
import zipfile
import os.path
import shutil

from . import misc

URL  = 'http://agencia.tse.jus.br/estatistica/sead/odsele/votacao_candidato_munzona'
FILE = 'votacao_candidato_munzona'

CABECALHO_LEGADO = [
    'DT_GERACAO', 'HH_GERACAO', 'ANO_ELEICAO', 'NR_TURNO', 'DS_ELEICAO',
    'SG_UF', 'SG_UE', 'CD_MUNICIPIO', 'NM_MUNICIPIO', 'NR_ZONA',
    'CD_CARGO', 'NR_CANDIDATO', 'SQ_CANDIDATO', 'NM_CANDIDATO', 'NM_URNA_CANDIDATO',
    'DS_CARGO', 'COD_SIT_CAND_SUPERIOR', 'DESC_SIT_CAND_SUPERIOR', 'CD_SITUACAO_CANDIDATURA', 'DS_SITUACAO_CANDIDATURA',
    'CD_SIT_TOT_TURNO', 'DS_SIT_TOT_TURNO', 'NR_PARTIDO', 'SG_PARTIDO', 'NM_PARTIDO',
    'SQ_COLIGACAO', 'NM_COLIGACAO', 'DS_COMPOSICAO_COLIGACAO', 'QT_VOTOS_NOMINAIS'
]

def getResultadoEleicoes(ano_eleicao, download_path, out_path= './data'):
    lista = []

    print(f'# Processando resultado das eleições de {ano_eleicao}')

    url = misc.gera_url(URL, FILE, ano_eleicao)
    filename = url.split('/')[-1]

    if misc.download_and_retry(url, download_path, filename):
        prefix = filename.split('.zip')[0]

        with zipfile.ZipFile(download_path + filename, 'r') as zip_ref:
            print(f'\t# Extraindo {filename} para {download_path + prefix}')
            zip_ref.extractall(download_path + prefix)
        
        files = os.listdir(download_path + prefix)
        
         # Arquivo nacional que agrupa todos os dados
        files = list(filter(lambda x: 'BRASIL' in x or 'brasil' in x, files))
        if len(files) == 0:
            files = os.listdir(download_path + prefix)

        files = list(filter(lambda x: x.lower().endswith('.csv') or x.lower().endswith('.txt'), files))

        for file in sorted(files):
            
            filepath = download_path + prefix + '/' + file

            print(f'\t\t# Carregando {file}')
            if ano_eleicao >= 2014:
                _resultado = pd.read_csv(filepath, sep=';', encoding='latin1', na_values=['#NULO#', '#NULO', '#NE#', '#NE'], dtype='object')
            else:
                _resultado = pd.read_csv(filepath, sep=';', encoding='latin1', na_values=['#NULO#', '#NULO', '#NE#', '#NE'], dtype='object', names=CABECALHO_LEGADO)
            
            # Padronizando os resultados com abrangencia estadual para conseguir
            # valores unicos
            _resultado.loc[
                (_resultado.DS_CARGO == 'GOVERNADOR') |
                (_resultado.DS_CARGO == 'SENADOR') |
                (_resultado.DS_CARGO == 'DEPUTADO FEDERAL') |
                (_resultado.DS_CARGO == 'DEPUTADO ESTADUAL'), 'CD_MUNICIPIO'] = -1
            
            _resultado.loc[
                (_resultado.DS_CARGO == 'GOVERNADOR') |
                (_resultado.DS_CARGO == 'SENADOR') |
                (_resultado.DS_CARGO == 'DEPUTADO FEDERAL') |
                (_resultado.DS_CARGO == 'DEPUTADO ESTADUAL'), 'NM_MUNICIPIO'] = 'ESTADUAL'

            # Filtrando os resultados nao eleitos
            _resultado = _resultado[
                (_resultado.DS_SIT_TOT_TURNO == 'ELEITO') |
                (_resultado.DS_SIT_TOT_TURNO == 'ELEITO POR QP') |
                (_resultado.DS_SIT_TOT_TURNO == 'ELEITO POR MÉDIA')][
                ['DT_GERACAO', 'ANO_ELEICAO', 'SG_UF', 'CD_MUNICIPIO', 'NM_MUNICIPIO',
                'CD_CARGO', 'NR_CANDIDATO', 'SQ_CANDIDATO', 'NM_CANDIDATO', 'NM_URNA_CANDIDATO', 
                'DS_CARGO', 'DS_SIT_TOT_TURNO', 'NR_PARTIDO', 'SG_PARTIDO', 'NM_PARTIDO', 
                'SQ_COLIGACAO', 'NM_COLIGACAO', 'DS_COMPOSICAO_COLIGACAO']
            ].drop_duplicates()

            lista.append(_resultado)
        
        # Deleta os arquivos extraidos
        print(f'\t# Deletando diretorio {download_path + prefix}')
        shutil.rmtree(download_path + prefix)

        resultado = pd.concat(lista, ignore_index=True)

        colunas_removidas = ['HH_GERACAO', 'DT_ELEICAO']
        for col in colunas_removidas:
            if col in resultado.columns:
                resultado.drop(col, axis=1, inplace=True)

        misc.mkdir(f'{out_path}/{ano_eleicao}')

        print(f'\t# Escrevendo {out_path}/{ano_eleicao}/resultado_eleicoes.csv\n')
        resultado.to_csv(f'{out_path}/{ano_eleicao}/resultado_eleicoes.csv', index=False, encoding='utf-8', sep='|')
