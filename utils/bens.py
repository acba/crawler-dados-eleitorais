import pandas as pd
import zipfile
import os.path
import shutil

from . import misc

URL  = 'http://agencia.tse.jus.br/estatistica/sead/odsele/bem_candidato'
FILE = 'bem_candidato'

CABECALHO_LEGADO = [
    'DT_GERACAO', 'HH_GERACAO', 'ANO_ELEICAO', 'DS_ELEICAO',
    'SG_UF', 'SQ_CANDIDATO', 'CD_TIPO_BEM_CANDIDATO', 'DS_TIPO_BEM_CANDIDATO',
    'DS_BEM_CANDIDATO', 'VR_BEM_CANDIDATO', 'DT_ULTIMA_ATUALIZACAO', 'HH_ULTIMA_ATUALIZACAO'
]

def getBens(ano_eleicao, download_path, out_path= './data'):
    lista = []

    print(f'# Processando bens das eleições de {ano_eleicao}')

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
            if ano_eleicao >= 2012:
                _resultado = pd.read_csv(filepath, sep=';', encoding='latin1', na_values=['#NULO#', '#NULO', '#NE#', '#NE'], dtype='object')
            else:
                _resultado = pd.read_csv(filepath, sep=';', encoding='latin1', na_values=['#NULO#', '#NULO', '#NE#', '#NE'], names=CABECALHO_LEGADO, dtype='object')

            _resultado = _resultado.iloc[:-1, :]
            lista.append(_resultado)
        
        # Deleta os arquivos extraidos
        print(f'\t# Deletando diretorio {download_path + prefix}')
        shutil.rmtree(download_path + prefix)

        resultado = pd.concat(lista, ignore_index=True)

        colunas_removidas = ['HH_GERACAO', 'HH_ULTIMA_ATUALIZACAO', 'DT_ELEICAO', 'NR_ORDEM_CANDIDATO',]
        for col in colunas_removidas:
            if col in resultado.columns:
                resultado.drop(col, axis=1, inplace=True)

        misc.mkdir(f'{out_path}/{ano_eleicao}')

        print(f'\t# Escrevendo {out_path}/{ano_eleicao}/bens.csv\n')
        resultado.to_csv(f'{out_path}/{ano_eleicao}/bens.csv', index=False, encoding='utf-8', sep='|')