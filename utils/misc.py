import requests
import pandas as pd
import zipfile
import os.path
import shutil
import sys

def download(url, download_path = './'):
    local_filename = url.split('/')[-1]
    response = requests.get(url, stream=True)
    total = response.headers.get('content-length')

    if response.status_code == 200:
        if not os.path.exists(download_path):
            os.makedirs(download_path)

        with open(download_path + local_filename, 'wb') as f:
            if total is None:
                f.write(response.content)
            else:
                downloaded = 0
                total = int(total)

                for data in response.iter_content(chunk_size=max(int(total/1000), 1024*1024)):
                    if data:
                        downloaded += len(data)
                        f.write(data)
                        done = int(50*downloaded/total)
                        sys.stdout.write('\r[{}{}]'.format('█' * done, '.' * (50-done)))
                        sys.stdout.flush()
        sys.stdout.write('\n')
    else:
        print(f'ERROR: Erro no download do arquivo {local_filename}')
    
    return response.status_code, local_filename

def gera_url(prefixo_url, prefixo_file, eleicao):
    filename = f'{prefixo_file}_{eleicao}.zip'
    url = f'{prefixo_url}/{filename}'

    return url

def get(eleicoes, download_path, p_url, p_file, cabecalho, out_path='./data'):
    URL  = p_url
    FILE = p_file

    lista = []

    for url, ano in gera_url(URL, FILE, eleicoes):
        print('Processando eleicoes de {}.'.format(ano))
        local_filename = url.split('/')[-1]

        # Realiza o download se o arquivo nao existir
        if not os.path.isfile(download_path + local_filename):
            print('\t' + '* Baixando ' + local_filename)
            status, filename = download(url, download_path)

        if os.path.isfile(download_path + local_filename):
            print('\t' + '* ' + local_filename + ' existe.')

            prefix   = local_filename.split('.zip')[0]
            filename = prefix + '_PB.txt'

            with zipfile.ZipFile(download_path + local_filename, 'r') as zip_ref:
                print('\t' + '* Extraindo {}'.format(local_filename))
                zip_ref.extractall(download_path + prefix)

            _resultado = pd.read_csv(download_path + prefix + '/' + filename,
                                     sep=';', encoding='ansi',
                                     names=cabecalho)

            lista.append(_resultado)

            # Deleta os arquivos extraidos
            print('\t' + '* Deletando diretorio {}\n'.format(download_path + prefix))
            shutil.rmtree(download_path + prefix)

    return pd.concat(lista, ignore_index=True)



def download_and_retry(url, download_path, filename, retries = 3):

    if not os.path.isfile(download_path + filename):
            status   = None
            contador = 0

            while status != 200:
                print('\t' + '# Baixando ' + filename)
                status, filename = download(url, download_path)
                contador = contador + 1
                if contador > retries:
                    break