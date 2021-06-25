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

def get(eleicoes, download_path, p_url, p_file, out_path='./data'):
    URL  = p_url
    FILE = p_file

    lista = []

    for ano_eleicao in eleicoes:
        url = gera_url(URL, FILE, ano_eleicao)

        print('Processando eleicoes de {}.'.format(ano_eleicao))
        print('url: ', url)
        local_filename = url.split('/')[-1]

        # Realiza o download se o arquivo nao existir
        if not os.path.isfile(download_path + local_filename):
            print('\t' + '* Baixando ' + local_filename)
            status, filename = download(url, download_path)

        if os.path.isfile(download_path + local_filename):
            print('\t' + '* ' + local_filename + ' existe.')

            prefix = local_filename.split('.zip')[0]

            with zipfile.ZipFile(download_path + local_filename, 'r') as zip_ref:
                print('\t' + '* Extraindo {}'.format(local_filename))
                zip_ref.extractall(download_path + prefix)
            
            # Arquivo nacional que agrupa todos os dados
            arquivos_br = list(filter(lambda x: 'BRASIL' in x or 'brasil' in x, os.listdir(download_path + prefix)))

            if len(arquivos_br) > 0:
                filename = arquivos_br[0]
                _resultado = pd.read_csv(download_path + prefix + '/' + filename, sep=';', encoding='ISO-8859-1')
            else:
                arquivos_estados = os.listdir(download_path + prefix)
                dados_estados = []

                for arquivo_uf in arquivos_estados:
                    _resultado = pd.read_csv(download_path + prefix + '/' + arquivo_uf, sep=';', encoding='ISO-8859-1')
                    dados_estados.append(_resultado)
                
                pd.concat(dados_estados, ignore_index=True)


            import ipdb; ipdb.set_trace()

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
            print(f'\t# Baixando {filename}')
            status, filename = download(url, download_path)
            contador = contador + 1
            if contador > retries:
                return False

    return True

def mkdir(path):
    if not os.path.exists(path):
            os.makedirs(path)

def listdir(path):
    files = os.listdir(path)

    # Arquivo nacional que agrupa todos os dados
    files = list(filter(lambda x: 'brasil' in x.lower(), files))
    if len(files) == 0:
        files = os.listdir(path)
    files = list(filter(lambda x: x.lower().endswith('.csv') or x.lower().endswith('.txt'), files))

    return files

def listdirrecursive(root, fil):
    lista = []

    for path, subdirs, files in os.walk(root):
        for name in files:
            if fil.lower() in name.lower():
                lista.append(os.path.join(path, name))
    
    return lista
