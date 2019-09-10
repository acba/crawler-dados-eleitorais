import requests
import pandas as pd
import zipfile
import os.path
import shutil
import utils

def getBens(eleicoes, download_path, out_path= './data'):
    URL  = 'http://agencia.tse.jus.br/estatistica/sead/odsele/bem_candidato'
    FILE = 'bem_candidato'
    cabecalho = ['DATA_GERACAO', 'HORA_GERACAO', 'ANO_ELEICAO', 'DESCRICAO_ELEICAO',
                 'SIGLA_UF', 'SQ_CANDIDATO', 'CD_TIPO_BEM_CANDIDATO', 'DS_TIPO_BEM_CANDIDATO',
                 'DETALHE_BEM', 'VALOR_BEM', 'DATA_ULTIMA_ATUALIZACAO', 'HORA_ULTIMA_ATUALIZACAO']
    
    print('### Bens')
    bens = utils.get(eleicoes, download_path, URL, FILE, cabecalho, out_path)
    bens = bens[['DATA_GERACAO', 'ANO_ELEICAO', 'SIGLA_UF', 'SQ_CANDIDATO',
                             'CD_TIPO_BEM_CANDIDATO', 'DS_TIPO_BEM_CANDIDATO', 'DETALHE_BEM',
                             'VALOR_BEM', 'DATA_ULTIMA_ATUALIZACAO']]

    bens.to_csv('{}/bens.csv'.format(out_path), index=False, encoding='utf-8', sep='|')

# def getBens(eleicoes, download_path, out_path= './data'):
#     URL  = 'http://agencia.tse.jus.br/estatistica/sead/odsele/bem_candidato'
#     FILE = 'bem_candidato'

#     lista = []

#     for url, ano in utils.gera_url(URL, FILE, eleicoes):
#         print('Bens')
#         print('Processando eleicoes de {}.'.format(ano))
#         local_filename = url.split('/')[-1]

#         # Realiza o download se o arquivo nao existir
#         if not os.path.isfile(download_path + local_filename):
#             print('\t' + '* Baixando ' + local_filename)
#             status, filename = utils.download_file(url, download_path)

#         if os.path.isfile(download_path + local_filename):
#             print('\t' + '* ' + local_filename + ' existe.')

#             prefix   = local_filename.split('.zip')[0]
#             filename = prefix + '_PB.txt'

#             with zipfile.ZipFile(download_path + local_filename, 'r') as zip_ref:
#                 print('\t' + '* Extraindo {}'.format(local_filename))
#                 zip_ref.extractall(download_path + prefix)

#             _resultado = pd.read_csv(download_path + prefix + '/' + filename,
#                                     sep=';', encoding='ansi',
#                                      names=['DATA_GERACAO', 'HORA_GERACAO', 'ANO_ELEICAO', 'DESCRICAO_ELEICAO',
#                                             'SIGLA_UF', 'SQ_CANDIDATO', 'CD_TIPO_BEM_CANDIDATO', 'DS_TIPO_BEM_CANDIDATO',
#                                             'DETALHE_BEM', 'VALOR_BEM', 'DATA_ULTIMA_ATUALIZACAO', 'HORA_ULTIMA_ATUALIZACAO'])

#             _resultado = _resultado[['DATA_GERACAO', 'ANO_ELEICAO', 'SIGLA_UF', 'SQ_CANDIDATO',
#                                      'CD_TIPO_BEM_CANDIDATO', 'DS_TIPO_BEM_CANDIDATO', 'DETALHE_BEM',
#                                      'VALOR_BEM', 'DATA_ULTIMA_ATUALIZACAO']]

#             lista.append(_resultado)

#             # Deleta os arquivos extraidos
#             print('\t' + '* Deletando diretorio {}\n'.format(download_path + prefix))
#             shutil.rmtree(download_path + prefix)

#     resultado_eleicoes = pd.concat(lista, ignore_index=True)
#     resultado_eleicoes.to_csv('{}/bens.csv'.format(out_path), index=False, encoding='utf-8', sep='|')
