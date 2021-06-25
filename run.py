import sys
from utils import resultado, bens, candidatos, prestacao_contas

ANO_ELEICOES = [2002, 2004, 2006, 2008, 2010, 2012, 2014, 2016, 2018, 2020]
DOWNLOAD_PATH = './data/download/'

def crawl(periodo):
    for ano in periodo:
        ano = int(ano)

        candidatos.getCandidatos(ano, download_path=DOWNLOAD_PATH)
        bens.getBens(ano, download_path=DOWNLOAD_PATH)
        resultado.getResultadoEleicoes(ano, download_path=DOWNLOAD_PATH)
        # prestacao_contas.getPrestacaoFinal(ano, download_path=DOWNLOAD_PATH)

periodo = sys.argv[1].split(',') if len(sys.argv) >= 2 and sys.argv[1] is not None else ANO_ELEICOES

crawl(periodo)
