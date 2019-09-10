import requests
import pandas as pd
import zipfile
import os.path
import shutil
import utils

def getResultadoEleicoes(eleicoes, download_path, out_path= './data'):
    URL  = 'http://agencia.tse.jus.br/estatistica/sead/odsele/votacao_candidato_munzona'
    FILE = 'votacao_candidato_munzona'

    lista = []


    print('### Resultado Eleicoes')
    for url, ano in utils.gera_url(URL, FILE, eleicoes):
        print('Processando eleicoes de {}.'.format(ano))
        local_filename = url.split('/')[-1]

        # Realiza o download se o arquivo nao existir
        if not os.path.isfile(download_path + local_filename):
            print('\t' + '* Baixando ' + local_filename)
            status, filename = utils.download_file(url, download_path)

        if os.path.isfile(download_path + local_filename):
            print('\t' + '* ' + local_filename + ' existe.')

            prefix   = local_filename.split('.zip')[0]
            filename = prefix + '_PB.txt'

            with zipfile.ZipFile(download_path + local_filename, 'r') as zip_ref:
                print('\t' + '* Extraindo {}'.format(local_filename))
                zip_ref.extractall(download_path + prefix)

            if ano >= 2014:
                _resultado = pd.read_csv(download_path + prefix + '/' + filename,
                                        sep=';', encoding='ansi',
                                        names=['DATA_GERACAO', 'HORA_GERACAO', 'ANO_ELEICAO', 'NUM_TURNO', 'DESCRICAO_ELEICAO',
                                                'SIGLA_UF', 'SIGLA_UE', 'CODIGO_MUNICIPIO', 'NOME_MUNICIPIO', 'NUMERO_ZONA',
                                                'CODIGO_CARGO', 'NUMERO_CAND', 'SQ_CANDIDATO', 'NOME_CANDIDATO', 'NOME_URNA_CANDIDATO',
                                                'DESCRICAO_CARGO', 'COD_SIT_CAND_SUPERIOR', 'DESC_SIT_CAND_SUPERIOR', 'CODIGO_SIT_CANDIDATO', 'DESC_SIT_CANDIDATO',
                                                'CODIGO_SIT_CAND_TOT', 'DESC_SIT_CAND_TOT', 'NUMERO_PARTIDO', 'SIGLA_PARTIDO', 'NOME_PARTIDO',
                                                'SEQUENCIAL_LEGENDA', 'NOME_COLIGACAO', 'COMPOSICAO_LEGENDA', 'TOTAL_VOTOS', 'TRANSITO'])
            else :
                _resultado = pd.read_csv(download_path + prefix + '/' + filename,
                                         sep=';', encoding='ansi',
                                         names=['DATA_GERACAO', 'HORA_GERACAO', 'ANO_ELEICAO', 'NUM_TURNO', 'DESCRICAO_ELEICAO',
                                                'SIGLA_UF', 'SIGLA_UE', 'CODIGO_MUNICIPIO', 'NOME_MUNICIPIO', 'NUMERO_ZONA',
                                                'CODIGO_CARGO', 'NUMERO_CAND', 'SQ_CANDIDATO', 'NOME_CANDIDATO', 'NOME_URNA_CANDIDATO',
                                                'DESCRICAO_CARGO', 'COD_SIT_CAND_SUPERIOR', 'DESC_SIT_CAND_SUPERIOR', 'CODIGO_SIT_CANDIDATO', 'DESC_SIT_CANDIDATO',
                                                'CODIGO_SIT_CAND_TOT', 'DESC_SIT_CAND_TOT', 'NUMERO_PARTIDO', 'SIGLA_PARTIDO', 'NOME_PARTIDO',
                                                'SEQUENCIAL_LEGENDA', 'NOME_COLIGACAO', 'COMPOSICAO_LEGENDA', 'TOTAL_VOTOS'])

            # Padronizando os resultados com abrangencia estadual para conseguir
            # valores unicos
            _resultado.loc[(_resultado.DESCRICAO_CARGO == 'GOVERNADOR') |
                        (_resultado.DESCRICAO_CARGO == 'SENADOR') |
                        (_resultado.DESCRICAO_CARGO == 'DEPUTADO FEDERAL') |
                        (_resultado.DESCRICAO_CARGO == 'DEPUTADO ESTADUAL'), 'CODIGO_MUNICIPIO'] = -1
            
            _resultado.loc[(_resultado.DESCRICAO_CARGO == 'GOVERNADOR') |
                           (_resultado.DESCRICAO_CARGO == 'SENADOR') |
                           (_resultado.DESCRICAO_CARGO == 'DEPUTADO FEDERAL') |
                           (_resultado.DESCRICAO_CARGO == 'DEPUTADO ESTADUAL'), 'NOME_MUNICIPIO'] = 'ESTADUAL'

            # Filtrando os resultados nao eleitos
            _resultado = _resultado[(_resultado.DESC_SIT_CAND_TOT == 'ELEITO') |
                                    (_resultado.DESC_SIT_CAND_TOT == 'ELEITO POR QP') |
                                    (_resultado.DESC_SIT_CAND_TOT == 'ELEITO POR MÉDIA')][
                                        ['DATA_GERACAO', 'ANO_ELEICAO', 'SIGLA_UF', 'CODIGO_MUNICIPIO', 'NOME_MUNICIPIO',
                                         'CODIGO_CARGO', 'NUMERO_CAND', 'SQ_CANDIDATO', 'NOME_CANDIDATO', 'NOME_URNA_CANDIDATO', 'DESCRICAO_CARGO',
                                         'DESC_SIT_CAND_TOT', 'NUMERO_PARTIDO', 'SIGLA_PARTIDO', 'NOME_PARTIDO', 'SEQUENCIAL_LEGENDA', 'NOME_COLIGACAO', 
                                         'COMPOSICAO_LEGENDA'
                                        ]].drop_duplicates()
            lista.append(_resultado)

            # Deleta os arquivos extraidos
            print('\t' + '* Deletando diretorio {}\n'.format(download_path + prefix))
            shutil.rmtree(download_path + prefix)

    resultado_eleicoes = pd.concat(lista, ignore_index=True)
    resultado_eleicoes.to_csv('{}/resultado_eleicoes.csv'.format(out_path), index=False, encoding='utf-8', sep='|')
