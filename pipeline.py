import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText
from apache_beam.io.textio import WriteToText
import re

pipeline_options = PipelineOptions(argv=None) # Argumentos é basicamente maquina e etc (Produção)
pipeline = beam.Pipeline(options=pipeline_options)

colunas_dengue = [
                'id',
                'data_iniSE',
                'casos',
                'ibge_code',
                'cidade',
                'uf',
                'cep',
                'latitude',
                'longitude' ]

def list_to_dict(elemento, colunas):
    return dict(zip(colunas,elemento))


def text_to_list(elemento, delimitador='|'):
    '''
    Recebe um texto e um delimitador
    Retorna uma lista de elementos pelo delimitador
    '''
    return elemento.split(delimitador)

def trata_data(elemento):

    elemento['ano_mes'] = elemento['data_iniSE'][:-3]

    return elemento

def chave_uf(elemento):
    chave = elemento['uf']

    return(chave, elemento)

def casos_dengue(elemento):

    uf, registros = elemento

    for registro in registros:
        if bool(re.search(r'\d',registro['casos'])):
            yield (f"{uf}-{registro['ano_mes']}", float(registro['casos']))
        else:
            yield (f"{uf}-{registro['ano_mes']}", 0.0)

def chave_chuva(elemento):
    uf = elemento[-1]
    ano_mes = elemento[0][:-3]

    if(float(elemento[1]) > 0): 
        return (uf+"-"+ano_mes,float(elemento[1]))
    else:
        return (uf+"-"+ano_mes,0.0)

def arrendondar_chuvas(elemento):
    chave , mm = elemento

    return (chave, round(mm,1))


    

def filtra_compos_vazios(elemento): 
    chave, dados = elemento
    if all([
        dados['chuvas'],
        dados['dengue']
    ]):
        return True
    return False

def descompacta_elementos(elemento):
    chave, dados = elemento

    uf, ano , mes = chave.split('-')
    chuva = dados['chuvas'][0]
    dengue = dados['dengue'][0]

    return (uf, (ano), (mes), str(chuva), str(dengue))

def prepara_csv(element, delimiter=';'):
    """
    Transform a tuple in a csv
    """
    return f"{delimiter}".join(map(str, element))



dengue = (
    pipeline
    | "Leitura do dataset de dengue"
      >> ReadFromText('casos_dengue.txt', skip_header_lines=1)
    | "De texto para lista" >> beam.Map(text_to_list)
    | "De lista para dicionario" >> beam.Map(list_to_dict,colunas_dengue)
    | "Adicionar ano_mes" >> beam.Map(trata_data)
    | "Retorna Tupla" >> beam.Map(chave_uf)
    | "Agrupar pelo estado" >> beam.GroupByKey()
    | "Descompacta casos de dengue" >> beam.FlatMap(casos_dengue)
    | "Soma dos casos pela chave" >> beam.CombinePerKey(sum)
    #| "Mostrar resultados" >> beam.Map(print)

)


chuva = (
    pipeline
    | "Leitura dataset chuva" >>
        ReadFromText('chuvas.csv',skip_header_lines=1)
    | "De texto para lista - chuva" >> beam.Map(text_to_list, delimitador = ',') 
    | "Converte a lista para chave">> beam.Map(chave_chuva)
    | "Soma das chuvas pela chave" >> beam.CombinePerKey(sum)
    | "Arredondar resultados de chuva" >> beam.Map(arrendondar_chuvas)
    #| "Mostrar resultados chuva" >> beam.Map(print)
)

resultado = (
    # (chuva, dengue)
    # | "Empilhar resultados" >> beam.Flatten()
    # | "Agrupando as pcols" >> beam.GroupByKey()
    ({'chuvas': chuva, 'dengue': dengue})
    | "Mesclar pcols" >> beam.CoGroupByKey()
    | "Filtrar dados vazios" >> beam.Filter(filtra_compos_vazios)
    | "Descompactar elementos resultados" >> beam.Map(descompacta_elementos)
    | "Perara para csv" >> beam.Map(prepara_csv)
    #| "Resultados" >> beam.Map(print)
)

header = 'UF;ANO;MES;CHUVA;DENGUE'

resultado | 'Criar arquivo CSV' >> WriteToText('resultadoPipeline',file_name_suffix=".csv", header=header)


pipeline.run()