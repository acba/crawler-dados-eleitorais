from utils import resultado, legendas, bens, candidatos, prestacao_contas

#ANO_ELEICOES = [2016]
ANO_ELEICOES = [2000, 2002, 2004, 2006, 2008, 2010, 2012, 2014, 2016, 2018]
DOWNLOAD_PATH = './data/download/'

def run():
    for ano in ANO_ELEICOES:
        candidatos.getCandidatos(ano, download_path=DOWNLOAD_PATH)
    # bens.getBens(ANO_ELEICOES, DOWNLOAD_PATH)
    # legendas.getLegendas(ANO_ELEICOES, DOWNLOAD_PATH)
    # resultado.getResultadoEleicoes(ANO_ELEICOES, DOWNLOAD_PATH)
    # prestacao_contas.getPrestacaoFinal(ANO_ELEICOES, DOWNLOAD_PATH)


run()
