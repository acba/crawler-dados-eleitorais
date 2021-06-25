import requests
import pandas as pd
import zipfile
import os.path
import shutil

from . import misc

URL  = 'http://agencia.tse.jus.br/estatistica/sead/odsele/prestacao_contas'
FILE = 'prestacao_contas_final'

"Entrega em conjunto?";"N�mero Recibo Eleitoral";"N�mero do documento";"CPF/CNPJ do doador";"Nome do doador";"Data da receita";"Valor receita";"Tipo receita";"Fonte recurso";"Esp�cie recurso";"Descri��o da receita"                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        


def processaPrestacaoFinalReceitaCandidato(file_path, ano_eleicao):
    lista = []
    files = misc.listdirrecursive(file_path + '/candidato', 'ReceitasCandidatos')

    cabecalho = [
        'COD_ELEICAO', 'DESC_ELEICAO', 'DATA_GERACAO', 'CNPJ_PRESTADOR_CONTA', 'SQ_CANDIDATO',
        'UF', 'SIGLA_UE', 'NOME_UE', 'SIGLA_PARTIDO', 'NUM_CANDIDATO', 'CARGO', 'NOME_CANDIDATO',
        'CPF_CANDIDATO', 'CPF_VICE_SUPLENTE', 'NUM_RECIBO_ELEITORAL', 'NUM_DOC', 'CPF_CNPJ_DOADOR',
        'NOME_DOADOR', 'NOME_DOADOR_RFB', 'SIGLA_UE_DOADOR', 'NUM_PARTIDO_DOADOR', 'NUM_CANDIDATO_DOADOR',
        'CD_CNAE_DOADOR', 'DESC_CNAE_DOADOR', 'DATA_RECEITA', 'VALOR_RECEITA', 'TIPO_RECEITA',
        'FONTE_RECURSO', 'ESPECIE_RECURSO', 'DESC_RECEITA', 'CPF_CNPJ_DOADOR_ORIGINARIO', 'NOME_DOADOR_ORIGINARIO',
        'TIPO_DOADOR_ORIGINARIO', 'CD_CNAE_DOADOR_ORIGINARIO', 'NOME_DOADOR_ORIGINARIO_RFB'
    ]

    CABECALHO_2010 = [
        'DATA_GERACAO', 'SQ_CANDIDATO', 'SG_UF', 'SG_PARTIDO', 'NR_CANDIDATO', 'DS_CARGO', 'NM_CANDIDATO', 'CPF_CANDIDATO'
    ]
    PULA_PRIMEIRA_LINHA = True

    # if 

    for filename in files:
        df = pd.read_csv(filename, sep=';', encoding='latin1', names=cabecalho)
        lista.append(df)

    return pd.concat(lista, ignore_index=True)


def processaPrestacaoFinalDespesaCandidato(file_path, ano):
    filename  = 'despesas_candidatos_prestacao_contas_final_{}_brasil.txt'.format(ano)
    cabecalho = ['COD_ELEICAO', 'DESC_ELEICAO', 'DATA_GERACAO', 'CNPJ_PRESTADOR_CONTA', 'SQ_CANDIDATO',
                'UF', 'SIGLA_UE', 'NOME_UE', 'SIGLA_PARTIDO', 'NUM_CANDIDATO', 'CARGO', 'NOME_CANDIDATO',
                'CPF_CANDIDATO', 'CPF_VICE_SUPLENTE', 'TIPO_DOC', 'NUM_DOC', 'CPF_CNPJ_FORNECEDOR',
                'NOME_FORNECEDOR', 'NOME_FORNECEDOR_RFB', 'CD_CNAE_FORNECEDOR', 'DESC_CNAE_FORNECEDOR', 
                'DATA_DESPESA', 'VALOR_DESPESA', 'TIPO_DESPESA', 'DESC_DESPESA']

    _resultado = pd.read_csv(file_path + '/' + filename,sep=';', encoding='ansi', names=cabecalho)
    return _resultado[_resultado['SIGLA_UE'] == 'PB']

#
#

def processaPrestacaoFinalReceitaPartido(file_path, ano):
    filename  = 'receitas_partidos_prestacao_contas_final_{}_PB.txt'.format(ano)
    cabecalho = ['COD_ELEICAO', 'DESC_ELEICAO', 'DATA_GERACAO', 'CNPJ_PRESTADOR_CONTA', 'SQ_DIRECAO_PARTIDARIA',
                'UF', 'SIGLA_UE', 'NOME_UE', 'TIPO_DIRETORIO', 'SIGLA_PARTIDO', 'NUM_RECIBO_ELEITORAL', 
                'NUM_DOC', 'CPF_CNPJ_DOADOR', 'NOME_DOADOR', 'NOME_DOADOR_RFB', 'SIGLA_UE_DOADOR', 
                'NUM_PARTIDO_DOADOR', 'NUM_CANDIDATO_DOADOR', 'CD_CNAE_DOADOR', 'DESC_CNAE_DOADOR', 
                'DATA_RECEITA', 'VALOR_RECEITA', 'TIPO_RECEITA', 'FONTE_RECURSO', 'ESPECIE_RECURSO', 
                'DESC_RECEITA', 'CPF_CNPJ_DOADOR_ORIGINARIO', 'NOME_DOADOR_ORIGINARIO',
                'TIPO_DOADOR_ORIGINARIO', 'CD_CNAE_DOADOR_ORIGINARIO', 'NOME_DOADOR_ORIGINARIO_RFB']

    return pd.read_csv(file_path + '/' + filename,sep=';', encoding='ansi', names=cabecalho)

def processaPrestacaoFinalDespesaPartido(file_path, ano):
    filename  = 'despesas_partidos_prestacao_contas_final_{}_PB.txt'.format(ano)
    cabecalho = ['COD_ELEICAO', 'DESC_ELEICAO', 'DATA_GERACAO', 'CNPJ_PRESTADOR_CONTA', 'SQ_DIRECAO_PARTIDARIA',
                'UF', 'SIGLA_UE', 'NOME_UE', 'TIPO_DIRETORIO', 'SIGLA_PARTIDO', 'TIPO_DOC', 'NUM_DOC', 
                'CPF_CNPJ_FORNECEDOR', 'NOME_FORNECEDOR', 'NOME_FORNECEDOR_RFB', 'CD_CNAE_FORNECEDOR', 'DESC_CNAE_FORNECEDOR', 
                'DATA_DESPESA', 'VALOR_DESPESA', 'TIPO_DESPESA', 'DESC_DESPESA']

    return pd.read_csv(file_path + '/' + filename,sep=';', encoding='ansi', names=cabecalho)

#

def getPrestacaoFinal(ano_eleicao, download_path, out_path= './data'):

    receita_candidato = []
    despesa_candidato = []

    receita_partido = []
    despesa_partido = []

    print(f'# Processando prestacao de contas final das eleições de {ano_eleicao}')

    if ano_eleicao < 2012:
        url = f'{URL}/prestacao_contas_{ano_eleicao}.zip'
    elif ano_eleicao >= 2012 and ano_eleicao <= 2016:
        url = f'{URL}/prestacao_final_{ano_eleicao}.zip'
    elif ano_eleicao == 2018 or ano_eleicao == 2020:
        url = f'{URL}/prestacao_de_contas_eleitorais_candidatos_{ano_eleicao}.zip'
    else:
        pass

    filename = url.split('/')[-1]

    misc.download_and_retry(url, download_path, filename)

    if os.path.isfile(download_path + filename):
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


        r_candidato = processaPrestacaoFinalReceitaCandidato(download_path + prefix, ano_eleicao)

        # receita_candidato.append(processaPrestacaoFinalReceitaCandidato(download_path + prefix))
        # despesa_candidato.append(processaPrestacaoFinalDespesaCandidato(download_path + prefix, ano_eleicao))

        # receita_partido.append(processaPrestacaoFinalReceitaPartido(download_path + prefix, ano_eleicao))
        # despesa_partido.append(processaPrestacaoFinalDespesaPartido(download_path + prefix, ano_eleicao))

        # Deleta os arquivos extraidos
        print(f'\t# Deletando diretorio {download_path + prefix}')
        shutil.rmtree(download_path + prefix)

        # r_candidato = pd.concat(receita_candidato, ignore_index=True)
        # d_candidato = pd.concat(despesa_candidato, ignore_index=True)

        # r_partido = pd.concat(receita_partido, ignore_index=True)
        # d_partido = pd.concat(despesa_partido, ignore_index=True)

        print(f'\t# Escrevendo {out_path}/{ano_eleicao}/receita_candidatos.csv\n')
        r_candidato.to_csv(f'{out_path}/{ano_eleicao}/receita_candidatos.csv', index=False, encoding='utf-8', sep='|')

        # d_candidato.to_csv('{}/despesa_candidatos.csv'.format(out_path), index=False, encoding='utf-8', sep='|')
        # r_partido.to_csv('{}/receita_partidos.csv'.format(out_path), index=False, encoding='utf-8', sep='|')
        # d_partido.to_csv('{}/despesa_partidos.csv'.format(out_path), index=False, encoding='utf-8', sep='|')


    # print('### Prestacao de Contas Final')
    # for url, ano in misc.gera_url(URL, FILE, eleicoes):
    #     print('Processando eleicoes de {}.'.format(ano))
    #     if ano < 2012:
    #         url = '{}/prestacao_contas_{}.zip'.format(URL, ano)
    #     elif ano >= 2012 and ano < 2016:
    #         url = '{}/prestacao_final_{}.zip'.format(URL, ano)
    #     else:
    #         pass

    #     local_filename = url.split('/')[-1]

    #     # Realiza o download se o arquivo nao existir
    #     if not os.path.isfile(download_path + local_filename):
    #         print('\t' + '* Baixando ' + local_filename)
    #         status, filename = misc.download_file(url, download_path)

    #     if os.path.isfile(download_path + local_filename):
    #         print('\t' + '* ' + local_filename + ' existe.')
    #         prefix   = local_filename.split('.zip')[0]

    #         with zipfile.ZipFile(download_path + local_filename, 'r') as zip_ref:
    #             print('\t' + '* Extraindo {}'.format(local_filename))
    #             zip_ref.extractall(download_path + prefix)

            # receita_candidato.append(processaPrestacaoFinalReceitaCandidato(download_path + prefix, ano))
            # despesa_candidato.append(processaPrestacaoFinalDespesaCandidato(download_path + prefix, ano))

            # receita_partido.append(processaPrestacaoFinalReceitaPartido(download_path + prefix, ano))
            # despesa_partido.append(processaPrestacaoFinalDespesaPartido(download_path + prefix, ano))

    #         # Deleta os arquivos extraidos
    #         print('\t' + '* Deletando diretorio {}\n'.format(download_path + prefix))
    #         shutil.rmtree(download_path + prefix)

    # r_candidato = pd.concat(receita_candidato, ignore_index=True)
    # d_candidato = pd.concat(despesa_candidato, ignore_index=True)

    # r_partido = pd.concat(receita_partido, ignore_index=True)
    # d_partido = pd.concat(despesa_partido, ignore_index=True)

    # r_candidato.to_csv('{}/receita_candidatos.csv'.format(out_path), index=False, encoding='utf-8', sep='|')
    # d_candidato.to_csv('{}/despesa_candidatos.csv'.format(out_path), index=False, encoding='utf-8', sep='|')
    # r_partido.to_csv('{}/receita_partidos.csv'.format(out_path), index=False, encoding='utf-8', sep='|')
    # d_partido.to_csv('{}/despesa_partidos.csv'.format(out_path), index=False, encoding='utf-8', sep='|')

def getPrestacaoFinalSuplementar(eleicoes, download_path, out_path= './data'):
    lista = []

    print('### Prestacao de Contas Final Suplementar')
    for url, ano in misc.gera_url(URL, FILE, eleicoes):
        print('Processando eleicoes de {}.'.format(ano))
        local_filename = url.split('/')[-1]

        # Realiza o download se o arquivo nao existir
        if not os.path.isfile(download_path + local_filename):
            print('\t' + '* Baixando ' + local_filename)
            status, filename = misc.download_file(url, download_path)

        if os.path.isfile(download_path + local_filename):
            print('\t' + '* ' + local_filename + ' existe.')

            prefix   = local_filename.split('.zip')[0]

            with zipfile.ZipFile(download_path + local_filename, 'r') as zip_ref:
                print('\t' + '* Extraindo {}'.format(local_filename))
                zip_ref.extractall(download_path + prefix)

            arquivos = ['despesas_partidos_prestacao_contas_final_sup',  # _2016
                        'receitas_partidos_prestacao_contas_final_sup',  # _2016
                        'despesas_candidatos_prestacao_contas_final_sup',  # _2016
                        'receitas_candidatos_prestacao_contas_final_sup'  # _2016
                        ]
            filename = prefix + '_PB.txt'

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
