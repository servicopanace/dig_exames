import azure.functions as func
import logging
from azure.storage.blob import BlobServiceClient
from PyPDF2 import PdfFileWriter, PdfFileReader, PdfFileMerger
from msrest.authentication import CognitiveServicesCredentials
from azure.cognitiveservices.vision.computervision import ComputerVisionClient
from azure.cognitiveservices.vision.computervision.models import OperationStatusCodes
from io import BytesIO
import io
import openai
import pandas as pd
from datetime import datetime
import sys
import pyodbc
import requests


app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="http_trigger1")
def http_trigger1(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )

def download_blob(storage_connection_string, container_name, blob_name):
    blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    blob_data = blob_client.download_blob()
    return blob_data.readall()

def split_pdf_pages(pdf_bytes):
    input_pdf = PdfFileReader(io.BytesIO(pdf_bytes))
    output_pdfs = []
    for page_num in range(input_pdf.numPages):
        output_pdf = PdfFileWriter()
        output_pdf.addPage(input_pdf.getPage(page_num))
        output_stream = io.BytesIO()
        output_pdf.write(output_stream)
        output_pdfs.append(output_stream.getvalue())
        output_stream.close()
    return output_pdfs

def upload_blob(storage_connection_string, container_name, blob_name, data):
    blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    blob_client.upload_blob(data, overwrite=True)

def excluir_blob(storage_connection_string, container_name, blob_name):
    blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    blob_client.delete_blob()

def pegar_ultimo_nome(texto, delimitador1, delimitador2):
    # Encontra a última ocorrência do primeiro delimitador
    pos_delimitador1 = texto.rfind(delimitador1)
    if pos_delimitador1 == -1:  # Se o primeiro delimitador não for encontrado, retorna None
        return None

    # Encontra a última ocorrência do segundo delimitador a partir da posição do primeiro delimitador
    pos_delimitador2 = texto.rfind(delimitador2, pos_delimitador1)
    if pos_delimitador2 == -1:  # Se o segundo delimitador não for encontrado após o primeiro delimitador, retorna None
        return None

    # Retorna o texto entre os dois delimitadores
    return texto[pos_delimitador1 + 1: pos_delimitador2]

def ocr_image(image_content):
    # Cria o cliente ComputerVisionClient com as credenciais
    credentials = CognitiveServicesCredentials("acfb2b6bae1f4a0089ce150397af5785")
    client = ComputerVisionClient(
        endpoint="https://panacea-computer-vision-ocr.cognitiveservices.azure.com/",
        credentials=credentials
    )

    # Realiza OCR na imagem
    result = client.read_in_stream(BytesIO(image_content), raw=True)

    # Obtém a operação ID para verificar o status do reconhecimento
    operation_id = result.headers["Operation-Location"].split("/")[-1]

    # Verifica o status da operação até que o reconhecimento seja concluído
    while True:
        get_operation = client.get_read_result(operation_id)
        if get_operation.status not in [OperationStatusCodes.running, OperationStatusCodes.not_started]:
            break

    # Obtém o resultado do OCR
    if get_operation.status == OperationStatusCodes.succeeded:
        ocr_result = get_operation.analyze_result
        # Extrai o texto da análise
        extracted_text = ""
        for line in ocr_result.read_results:
            for word in line.lines:
                extracted_text += word.text + " "
        return extracted_text.strip()  # Remove espaços em branco extras do início e do fim do texto
    else:
        return None

def openAI(pergunta):
    # Definições de acesso API
    openai.api_type = "azure"
    openai.api_base = "https://pocaioc.openai.azure.com/"
    openai.api_version = "2024-02-01"
    openai.api_key = '6be695f78c484c94a7847316168a505c'

    response = openai.ChatCompletion.create(
        engine="prontuario",
        messages=[
            {"role": "user", "content": pergunta}
        ],
        temperature=0,
        # top_p=0.97,
        frequency_penalty=0,
        presence_penalty=0,
        stop=None)
    return response

def pegar_extensao_arquivo(palavra):
    partes = palavra.rsplit(".", 1)

    if len(partes) > 1:
        nome_arquivo, extensao = partes
        ultima_palavra = extensao.split('.')[-1] if '.' in extensao else extensao
    else:
        ultima_palavra = ""

    return ultima_palavra

def gera_categoria(df, connection_string, container_name, inf_mais_freq_pasta, extensao_arq):
    informacoes_paginas = []
    patientCode = df[df['patientCode'] != 'NI']['patientCode'].value_counts().idxmax() if not df[
        df['Pasta'] != 'NI'].empty else ""

    nom_arq_laboratorial = 'laboratorial'
    nom_arq_cardiologicos = 'cardiologico'
    nom_arq_anatomopa = 'anatomopatologico'
    nom_arq_endoscopicos = 'endosccpicos.'
    nom_arq_imagem = 'imagem'
    nom_arq_patologicomolec = 'patologia_molecular'
    nom_arq_outros = 'outros'

    if 'pdf' in extensao_arq:

        merge_laboratorial = PdfFileMerger()
        merge_cardiologicos = PdfFileMerger()
        merge_anatomopa = PdfFileMerger()
        merge_endoscopicos = PdfFileMerger()
        merge_imagem = PdfFileMerger()
        merge_patologicomolec = PdfFileMerger()
        merge_outros = PdfFileMerger()

        for index, row in df.iterrows():
            bytes_arquivo = row['Arquivo_Memoria']
            texto = row['Cat_Exame'].lower()
            pag = row['Pagina']

            if "n/a" == texto or "ni" == texto or "não" in texto or "-" in texto or "menção" in texto:
                texto = valor_anterior

            if texto is not None and "lab" in texto:
                merge_laboratorial.append(io.BytesIO(bytes_arquivo))
                informacoes_paginas.append({'Nome_Pasta': inf_mais_freq_pasta,
                                            'Pagina': pag,
                                            'Cod_Caminho_Cat': 6,
                                            'Caminho_Cat': inf_mais_freq_pasta + patientCode + "_" + nom_arq_laboratorial + "." + extensao_arq})

            elif texto is not None and "imag" in texto:
                merge_imagem.append(io.BytesIO(bytes_arquivo))
                informacoes_paginas.append({'Nome_Pasta': inf_mais_freq_pasta,
                                            'Pagina': pag,
                                            'Cod_Caminho_Cat': 12,
                                            'Caminho_Cat': inf_mais_freq_pasta + patientCode + "_" + nom_arq_imagem + "." + extensao_arq})

            elif texto is not None and "cardio" in texto:
                merge_cardiologicos.append(io.BytesIO(bytes_arquivo))
                informacoes_paginas.append({'Nome_Pasta': inf_mais_freq_pasta,
                                            'Pagina': pag,
                                            'Cod_Caminho_Cat': 7,
                                            'Caminho_Cat': inf_mais_freq_pasta + patientCode + "_" + nom_arq_cardiologicos + "." + extensao_arq})

            elif texto is not None and (
                    "anatomo" in texto or "anátomo" in texto or "patologico" in texto or "patológico" in texto):
                merge_anatomopa.append(io.BytesIO(bytes_arquivo))
                informacoes_paginas.append({'Nome_Pasta': inf_mais_freq_pasta,
                                            'Pagina': pag,
                                            'Cod_Caminho_Cat': 8,
                                            'Caminho_Cat': inf_mais_freq_pasta + patientCode + "_" + nom_arq_anatomopa + "." + extensao_arq})

            elif texto is not None and "endos" in texto:
                merge_endoscopicos.append(io.BytesIO(bytes_arquivo))
                informacoes_paginas.append({'Nome_Pasta': inf_mais_freq_pasta,
                                            'Pagina': pag,
                                            'Cod_Caminho_Cat': 9,
                                            'Caminho_Cat': inf_mais_freq_pasta + patientCode + "_" + nom_arq_endoscopicos + "." + extensao_arq})

            elif texto is not None and "outro" in texto:
                merge_outros.append(io.BytesIO(bytes_arquivo))
                informacoes_paginas.append({'Nome_Pasta': inf_mais_freq_pasta,
                                            'Pagina': pag,
                                            'Cod_Caminho_Cat': 10,
                                            'Caminho_Cat': inf_mais_freq_pasta + patientCode + "_" + nom_arq_outros + "." + extensao_arq})

            elif texto is not None and ("pato" in texto or "mole" in texto):
                merge_patologicomolec.append(io.BytesIO(bytes_arquivo))
                informacoes_paginas.append({'Nome_Pasta': inf_mais_freq_pasta,
                                            'Pagina': pag,
                                            'Cod_Caminho_Cat': 14,
                                            'Caminho_Cat': inf_mais_freq_pasta + patientCode + "_" + nom_arq_patologicomolec + "." + extensao_arq})

            elif texto is not None:
                merge_outros.append(io.BytesIO(bytes_arquivo))
                informacoes_paginas.append({'Nome_Pasta': inf_mais_freq_pasta,
                                            'Pagina': pag,
                                            'Cod_Caminho_Cat': 10,
                                            'Caminho_Cat': inf_mais_freq_pasta + patientCode + "_" + nom_arq_outros + "." + extensao_arq})

            valor_anterior = texto

        # Escreva o conteúdo mesclado do PdfFileMerger em um objeto BytesIO
        if merge_patologicomolec.pages:
            bytes_io_patologico = BytesIO()
            merge_patologicomolec.write(bytes_io_patologico)
            merge_patologicomolec.close()
            bytes_io_patologico.seek(0)  # Move o cursor para o início do BytesIO
            # Agora, você pode passar o objeto BytesIO para a função upload_blob.
            upload_blob(connection_string, container_name, inf_mais_freq_pasta + patientCode + "_" +
                        nom_arq_patologicomolec + "." + extensao_arq, bytes_io_patologico)

        if merge_imagem.pages:
            bytes_io = BytesIO()
            merge_imagem.write(bytes_io)
            merge_imagem.close()
            bytes_io.seek(0)  # Move o cursor para o início do BytesIO
            upload_blob(connection_string, container_name, inf_mais_freq_pasta + patientCode + "_" +
                        nom_arq_imagem + "." + extensao_arq, bytes_io)

        if merge_laboratorial.pages:
            bytes_io = BytesIO()
            merge_laboratorial.write(bytes_io)
            merge_laboratorial.close()
            bytes_io.seek(0)  # Move o cursor para o início do BytesIO
            upload_blob(connection_string, container_name, inf_mais_freq_pasta + patientCode + "_" +
                        nom_arq_laboratorial + "." + extensao_arq, bytes_io)

        if merge_cardiologicos.pages:
            bytes_io = BytesIO()
            merge_cardiologicos.write(bytes_io)
            merge_cardiologicos.close()
            bytes_io.seek(0)  # Move o cursor para o início do BytesIO
            upload_blob(connection_string, container_name, inf_mais_freq_pasta + patientCode + "_" +
                        nom_arq_cardiologicos + "." + extensao_arq, bytes_io)

        if merge_anatomopa.pages:
            bytes_io = BytesIO()
            merge_anatomopa.write(bytes_io)
            merge_anatomopa.close()
            bytes_io.seek(0)  # Move o cursor para o início do BytesIO
            upload_blob(connection_string, container_name, inf_mais_freq_pasta + patientCode + "_"
                        + nom_arq_anatomopa + "." + extensao_arq, bytes_io)

        if merge_endoscopicos.pages:
            bytes_io = BytesIO()
            merge_endoscopicos.write(bytes_io)
            merge_endoscopicos.close()
            bytes_io.seek(0)  # Move o cursor para o início do BytesIO
            upload_blob(connection_string, container_name, inf_mais_freq_pasta + patientCode + "_"
                        + nom_arq_endoscopicos + "." + extensao_arq, bytes_io)

        if merge_outros.pages:
            bytes_io = BytesIO()
            merge_outros.write(bytes_io)
            merge_outros.close()
            bytes_io.seek(0)  # Move o cursor para o início do BytesIO
            upload_blob(connection_string, container_name, inf_mais_freq_pasta + patientCode + "_" +
                        nom_arq_outros + "." + extensao_arq, bytes_io)

    else:
        list_arq_laboratorial = []
        list_arq_cardiologicos = []
        list_arq_anatomopa = []
        list_arq_endoscopicos = []
        list_arq_imagem = []
        list_arq_patologicomolec = []
        list_arq_outros = []

        for index, row in df.iterrows():
            bytes_arquivo = row['Arquivo_Memoria']
            texto = row['Cat_Exame'].lower()
            pag = row['Pagina']

            if "n/a" == texto or "ni" == texto or "não" in texto or "-" in texto or "menção" in texto:
                texto = valor_anterior

            if texto is not None and "lab" in texto:
                list_arq_laboratorial.append(bytes_arquivo)
                informacoes_paginas.append({'Nome_Pasta': inf_mais_freq_pasta,
                                            'Pagina': pag,
                                            'Cod_Caminho_Cat': 6,
                                            'Caminho_Cat': inf_mais_freq_pasta + patientCode + "_" + nom_arq_laboratorial + "." + extensao_arq})

            elif texto is not None and "imag" in texto:
                list_arq_imagem.append(bytes_arquivo)
                informacoes_paginas.append({'Nome_Pasta': inf_mais_freq_pasta,
                                            'Pagina': pag,
                                            'Cod_Caminho_Cat': 12,
                                            'Caminho_Cat': inf_mais_freq_pasta + patientCode + "_" + nom_arq_imagem + "." + extensao_arq})

            elif texto is not None and "cardio" in texto:
                list_arq_cardiologicos.append(bytes_arquivo)
                informacoes_paginas.append({'Nome_Pasta': inf_mais_freq_pasta,
                                            'Pagina': pag,
                                            'Cod_Caminho_Cat': 7,
                                            'Caminho_Cat': inf_mais_freq_pasta + patientCode + "_" + nom_arq_cardiologicos + "." + extensao_arq})

            elif texto is not None and (
                    "anatomo" in texto or "anátomo" in texto or "patologico" in texto or "patológico" in texto):
                list_arq_anatomopa.append(bytes_arquivo)
                informacoes_paginas.append({'Nome_Pasta': inf_mais_freq_pasta,
                                            'Pagina': pag,
                                            'Cod_Caminho_Cat': 8,
                                            'Caminho_Cat': inf_mais_freq_pasta + patientCode + "_" + nom_arq_anatomopa + "." + extensao_arq})

            elif texto is not None and "endos" in texto:
                list_arq_endoscopicos.append(bytes_arquivo)
                informacoes_paginas.append({'Nome_Pasta': inf_mais_freq_pasta,
                                            'Pagina': pag,
                                            'Cod_Caminho_Cat': 9,
                                            'Caminho_Cat': inf_mais_freq_pasta + patientCode + "_" + nom_arq_endoscopicos + "." + extensao_arq})

            elif texto is not None and "outro" in texto:
                list_arq_outros.append(bytes_arquivo)
                informacoes_paginas.append({'Nome_Pasta': inf_mais_freq_pasta,
                                            'Pagina': pag,
                                            'Cod_Caminho_Cat': 10,
                                            'Caminho_Cat': inf_mais_freq_pasta + patientCode + "_" + nom_arq_outros + "." + extensao_arq})

            elif texto is not None and ("pato" in texto or "mole" in texto):
                list_arq_patologicomolec.append(bytes_arquivo)
                informacoes_paginas.append({'Nome_Pasta': inf_mais_freq_pasta,
                                            'Pagina': pag,
                                            'Cod_Caminho_Cat': 14,
                                            'Caminho_Cat': inf_mais_freq_pasta + patientCode + "_" + nom_arq_patologicomolec + "." + extensao_arq})

            elif texto is not None:
                list_arq_outros.append(bytes_arquivo)
                informacoes_paginas.append({'Nome_Pasta': inf_mais_freq_pasta,
                                            'Pagina': pag,
                                            'Cod_Caminho_Cat': 10,
                                            'Caminho_Cat': inf_mais_freq_pasta + patientCode + "_" + nom_arq_outros + "." + extensao_arq})

            valor_anterior = texto

            # Escreva o conteúdo mesclado do PdfFileMerger em um objeto BytesIO
            if list_arq_patologicomolec:
                # Agora, você pode passar o objeto BytesIO para a função upload_blob.
                for arquivo in list_arq_patologicomolec:
                    upload_blob(connection_string, container_name,
                                f"{inf_mais_freq_pasta}{patientCode}_{nom_arq_patologicomolec}.{extensao_arq}", arquivo)

            if list_arq_imagem:
                for arquivo in list_arq_imagem:
                    upload_blob(connection_string, container_name,
                                f"{inf_mais_freq_pasta}{patientCode}_{nom_arq_imagem}.{extensao_arq}", arquivo)

            if list_arq_laboratorial:
                for arquivo in list_arq_laboratorial:
                    upload_blob(connection_string, container_name,
                                f"{inf_mais_freq_pasta}{patientCode}_{nom_arq_laboratorial}.{extensao_arq}", arquivo)

            if list_arq_cardiologicos:
                for arquivo in list_arq_cardiologicos:
                    upload_blob(connection_string, container_name,
                                f"{inf_mais_freq_pasta}{patientCode}_{nom_arq_cardiologicos}.{extensao_arq}", arquivo)

            if list_arq_anatomopa:
                for arquivo in list_arq_anatomopa:
                    upload_blob(connection_string, container_name,
                                f"{inf_mais_freq_pasta}{patientCode}_{nom_arq_anatomopa}.{extensao_arq}", arquivo)

            if list_arq_endoscopicos:
                for arquivo in list_arq_endoscopicos:
                    upload_blob(connection_string, container_name,
                                f"{inf_mais_freq_pasta}{patientCode}_{nom_arq_endoscopicos}.{extensao_arq}", arquivo)

            if list_arq_outros:
                for arquivo in list_arq_outros:
                    upload_blob(connection_string, container_name,
                                f"{inf_mais_freq_pasta}{patientCode}_{nom_arq_outros}.{extensao_arq}", arquivo)


    return informacoes_paginas

def grava_regisrtro_bd(df):
    connection_string = 'Driver={ODBC Driver 17 for SQL Server};' \
                        'Server=192.168.31.80,1433;' \
                        'Database=DigitalizacaoExames;' \
                        'Uid={usr_digiExames};' \
                        'Pwd={0l0tV@ZR@CbK};'



    connection = pyodbc.connect(connection_string)
    connection.autocommit = True
    cursor = connection.cursor()

    insert_to_tmp_tbl_stmt = f"INSERT INTO DigitalizacaoExames..External_Exam_Info ( [patientCode] , [patientName] ,[receptionName] ,[fileName] ,[uploadedDate] ,[originalPdfPath] , [examOcr],  [examDate], [codExamCategory] ,[examCategory] , [nameDoctorRequesting] , [crmDoctorRequesting] , [laboratoryName] , [separatedLink] , [loadDate]  ) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    cursor.executemany(insert_to_tmp_tbl_stmt, df.values.tolist())  # load data into azure sql db

    cursor.commit()  # Close the cursor and connection
    cursor.close()
    connection.close()

def processa_arquivo(connection_string, container_name, uploadedlink, originalPdfPath, url_base):
    # Inicialize o dicionário pdf_pages_dict

    # Lista para armazenar os dados temporariamente
    data = []

    blob_data = download_blob(connection_string, container_name, uploadedlink)
    nome_pasta = pegar_ultimo_nome(uploadedlink, "/", ".")
    extensao_arq = pegar_extensao_arquivo(uploadedlink)

    if 'pdf' in str(extensao_arq).lower():
        try:
            pages = split_pdf_pages(blob_data)

            for i, page in enumerate(pages):
                page_blob_name = f"{pasta_proc}/{pasta_anomesdia}/{nome_pasta}/{i + 1}.pdf"
                page_blob_folder = f"{pasta_proc}/{pasta_anomesdia}/{nome_pasta}/"
                upload_blob(connection_string, container_name, page_blob_name, page)
                # Realiza OCR na página
                resultado_ocr = ocr_image(page)
                # Adiciona o resultado do OCR à lista associada ao nome do arquivo PDF no dicionário
                repostaData = openAI(pergunta_data_exm + " " + resultado_ocr)
                repostaCat = openAI(pergunta_cat_exm + " " + resultado_ocr)
                repostaLab = openAI(pergunta_lab_exm + " " + resultado_ocr)
                repostaMed = openAI(pergunta_med_sol + " " + resultado_ocr)
                repostaCrm = openAI(pergunta_med_crm + " " + resultado_ocr)

                # Acessando os valores com tratamento de exceção
                data_exame = remove_non_date_characters(tratar_data(tratar_data(str(repostaData.get('choices', [{}])[0].get('message', {}).get('content', '')).lower())))
                cat_exame = str(repostaCat.get('choices', [{}])[0].get('message', {}).get('content', '')).lower()
                lab_exm = tratar_lab(tratar_lab(str(repostaLab.get('choices', [{}])[0].get('message', {}).get('content', '').lower())))
                med_sol = tratar_med_sol(tratar_med_sol(str(repostaMed.get('choices', [{}])[0].get('message', {}).get('content', '')).lower()))
                med_crm = tratar_crm(tratar_crm(str(repostaCrm.get('choices', [{}])[0].get('message', {}).get('content', '')).lower()))

                # Adicione os dados à lista temporária
                data.append({'patientName': patientName,
                             'patientCode': patientCode,
                             'receptionName': receptionName,
                             'uploadedDate': uploadeddate,
                             'fileName': nome_pasta,
                             'Nome_Pasta': nome_pasta,
                             'Pagina': i + 1,
                             'Resultado_OCR': resultado_ocr,
                             'originalPdfPath': originalPdfPath,
                             'Caminho': page_blob_name,
                             'Pasta': page_blob_folder,
                             'Arquivo_Memoria': page,
                             'Data_Exame': data_exame,
                             'Cat_Exame': cat_exame,
                             'Lab_Exm': lab_exm,
                             'Med_Sol': med_sol,
                             'Med_Crm': med_crm,
                             'Extensao_Arq': extensao_arq,
                             'url_base': url_base
                             })
        except Exception as e:
            print(f"Erro ao processar o arquivo PDF: {e}")
            sys.exit(1)
    else:
        try:
            page_blob_name = f"{pasta_proc}/{pasta_anomesdia}/{nome_pasta}/{nome_pasta}.{extensao_arq}"
            page_blob_folder = f"{pasta_proc}/{pasta_anomesdia}/{nome_pasta}/"
            # Realiza OCR na página
            resultado_ocr = ocr_image(blob_data)
            upload_blob(connection_string, container_name, page_blob_name, blob_data)

            # Adiciona o resultado do OCR à lista associada ao nome do arquivo PDF no dicionário
            repostaData = openAI(pergunta_data_exm + " " + resultado_ocr)
            repostaCat = openAI(pergunta_cat_exm + " " + resultado_ocr)
            repostaLab = openAI(pergunta_lab_exm + " " + resultado_ocr)
            repostaMed = openAI(pergunta_med_sol + " " + resultado_ocr)
            repostaCrm = openAI(pergunta_med_crm + " " + resultado_ocr)

            # Acessando os valores com tratamento de exceção
            data_exame = repostaData.get('choices', [{}])[0].get('message', {}).get('content', '')
            cat_exame = str(repostaCat.get('choices', [{}])[0].get('message', {}).get('content', '')).lower()
            lab_exm = repostaLab.get('choices', [{}])[0].get('message', {}).get('content', '')
            med_sol = repostaMed.get('choices', [{}])[0].get('message', {}).get('content', '')
            med_crm = repostaCrm.get('choices', [{}])[0].get('message', {}).get('content', '')

            # Adicione os dados à lista temporária
            data.append({'patientName': patientName,
                         'patientCode': patientCode,
                         'receptionName': receptionName,
                         'uploadedDate': uploadeddate,
                         'fileName': nome_pasta,
                         'Nome_Pasta': nome_pasta,
                         'Pagina': 1,
                         'Resultado_OCR': resultado_ocr,
                         'originalPdfPath': originalPdfPath,
                         'Caminho': page_blob_name,
                         'Pasta': page_blob_folder,
                         'Arquivo_Memoria': blob_data,
                         'Data_Exame': data_exame,
                         'Cat_Exame': cat_exame,
                         'Lab_Exm': lab_exm,
                         'Med_Sol': med_sol,
                         'Med_Crm': med_crm,
                         'Extensao_Arq': extensao_arq,
                         'url_base': url_base
                         })
        except Exception as e:
            print(f"Erro ao processar o arquivo: {e}")
            sys.exit(1)

    df = pd.DataFrame(data)
    inf_mais_freq_pasta = df[df['Pasta'] != 'NI']['Pasta'].value_counts().idxmax() if not df[
        df['Pasta'] != 'NI'].empty else ""
    inf_mais_freq_extensao = df[df['Extensao_Arq'] != 'NI']['Extensao_Arq'].value_counts().idxmax() if not df[
        df['Extensao_Arq'] != 'NI'].empty else ""
    # Categorizar exames
    df_inf_comple = pd.DataFrame(
        gera_categoria(df, connection_string, container_name, inf_mais_freq_pasta, inf_mais_freq_extensao))

    # Juntar informações adicionar do exame
    df_joined = pd.merge(df, df_inf_comple, on=['Pagina', 'Pagina'], how='inner')
    # df_joined = pd.merge(df_joined, df_tipo_exame, on=['Cat_Exame', 'Cat_Exame'], how='inner')
    df_joined['loadDate'] = pd.to_datetime(datetime.now())
    df_joined['Resultado_OCR'] = df_joined['Resultado_OCR'].astype(str)
    print(type(df_joined['Resultado_OCR'].dtype))

    df_joined['Resultado_OCR'] = df_joined['Resultado_OCR'].astype(str)
    df_joined['Caminho_Cat'] = df_joined['url_base'] + df_joined['Caminho_Cat']

    # Deletar arquivos temporarios
    for index, row in df_joined.iterrows():
        excluir_blob(connection_string, container_name, row["Caminho"])




    # Dicionário de mapeamento de nomes de colunas
    rename_mapping = {
                      'Data_Exame': 'examDate',
                      'Cat_Exame': 'examCategory',
                      'Med_Sol': 'nameDoctorRequesting',
                      'Med_Crm': 'crmDoctorRequesting',
                      'Lab_Exm': 'laboratoryName',
                      'Caminho_Cat': 'separatedLink',
                      'Cod_Caminho_Cat': 'codExamCategory',
                      'Resultado_OCR': 'examOcr'
                      }
    df_joined = df_joined.rename(columns=rename_mapping)

    df_insert = df_joined[['patientCode', 'patientName', 'receptionName', 'fileName', 'uploadedDate', 'originalPdfPath', 'examOcr',
                           'examDate', 'codExamCategory', 'examCategory', 'nameDoctorRequesting',
                           'crmDoctorRequesting', 'laboratoryName', 'separatedLink', 'loadDate']]

    grava_regisrtro_bd(df_insert)

    # Selecionando as colunas necessárias do DataFrame principal
    df_main_selected = df_insert[['patientCode', 'patientName', 'receptionName', 'fileName', 'uploadedDate', 'originalPdfPath']]

    df_unique = df_main_selected.drop_duplicates()

    # Selecionando as colunas necessárias do DataFrame dos exames separados
    df_separated_selected = df_insert[['examDate', 'codExamCategory', 'nameDoctorRequesting', 'crmDoctorRequesting', 'laboratoryName', 'separatedLink']]

    # Criando uma lista de dicionários para os exames separados
    separated_exams_list = df_separated_selected.to_dict(orient='records')

    df_unique['separatedExams'] = separated_exams_list



    return df_unique

def tratar_crm(texto_env):

    if 'não tenho acesso a informações específicas de pacientes' in texto_env:
        texto_modificado = 'NI'
    elif 'desculpe, como um modelo de linguagem de ia' in texto_env:
        texto_modificado = 'NI'
    elif 'não foi informado' in texto_env:
        texto_modificado = 'NI'
    elif 'não informado' in texto_env:
        texto_modificado = 'NI'
    elif 'não há informação' in texto_env:
        texto_modificado = 'NI'
    elif 'mas não é um crm válido' in texto_env:
        texto_modificado = 'NI'
    elif 'crm:' in texto_env:
        texto_modificado = texto_env.replace("crm:", "")
    elif 'o crm do médico solicitante é:' in texto_env:
        texto_modificado = texto_env.replace("o crm do médico solicitante é:", "")
    elif 'crm do médico solicitante:' in texto_env:
        texto_modificado = texto_env.replace("crm do médico solicitante:", "")
    elif '(crm do médico solicitante não encontrado, trazer como ni):' in texto_env:
        texto_modificado = texto_env.replace("(crm do médico solicitante não encontrado, trazer como ni):", "")
    else:
        texto_modificado = "NI" if texto_env == "ni" else texto_env

    return texto_modificado.replace(".", "").strip()

def remove_non_date_characters(word):
    # Remover todos os caracteres que não são dígitos ou hífens
    return ''.join(char for char in word if char.isdigit() or char == '-')

def tratar_data(texto_env):

    if 'não tenho acesso a informações específicas de pacientes' in texto_env:
        texto_modificado = 'NI'
    elif 'não há informação' in texto_env:
        texto_modificado = 'NI'
    elif 'não foi informado' in texto_env:
        texto_modificado = 'NI'
    elif 'desculpe, como um modelo de linguagem de ia' in texto_env:
        texto_modificado = 'NI'
    elif 'não informado' in texto_env:
        texto_modificado = 'NI'
    elif 'a data do exame é:' in texto_env:
        texto_modificado = texto_env.replace("a data do exame é:", "")
    elif 'data do exame:' in texto_env:
        texto_modificado = texto_env.replace("data do exame:", "")
    elif 'a data do exame no formato yyyy-mm-dd é' in texto_env:
        texto_modificado = texto_env.replace("a data do exame no formato yyyy-mm-dd é", "")
    elif '/' in texto_env:
        texto_modificado = texto_env.replace("/", "-")

    else:
        texto_modificado = "NI" if texto_env == "ni" else texto_env

    return texto_modificado.replace(".", "").strip()

def tratar_lab(texto_env):

    if 'não tenho acesso a informações específicas de pacientes' in texto_env:
        texto_modificado = 'NI'
    elif 'Desculpe, como um modelo de linguagem de IA' in texto_env:
        texto_modificado = 'NI'
    elif 'não informado' in texto_env:
        texto_modificado = 'NI'
    elif 'não foi informado' in texto_env:
        texto_modificado = 'NI'
    elif 'laboratório:' in texto_env:
        texto_modificado = texto_env.replace("laboratório:", "")
    else:
        texto_modificado = "NI" if texto_env == "ni" else texto_env

    return texto_modificado.replace(".", "").strip()

def tratar_med_sol(texto_env):
    if 'não tenho acesso' in texto_env:
        texto_modificado = 'NI'
    elif 'desculpe, como um modelo de linguagem de ia' in texto_env:
        texto_modificado = 'NI'
    elif 'não informado' in texto_env:
        texto_modificado = 'NI'
    elif 'não foi informado' in texto_env:
        texto_modificado = 'NI'
    elif 'nome:' in texto_env:
        texto_modificado = texto_env.replace("nome:", "")
    elif 'dra ' in texto_env:
        texto_modificado = texto_env.replace("dra ", "")
    elif 'dr[a]' in texto_env:
        texto_modificado = texto_env.replace("dr[a]", "")
    elif 'dr(a):' in texto_env:
        texto_modificado = texto_env.replace("dr(a)", "")
    elif 'dr(a).' in texto_env:
        texto_modificado = texto_env.replace("dr(a).", "")
    elif 'dr.(a)' in texto_env:
        texto_modificado = texto_env.replace("dr.(a)", "")
    elif 'dr(a)' in texto_env:
        texto_modificado = texto_env.replace("dr(a)", "")
    elif 'dr:' in texto_env:
        texto_modificado = texto_env.replace("dr.", "")
    elif 'dr.' in texto_env:
        texto_modificado = texto_env.replace("dr.", "")
    elif 'dr' in texto_env:
        texto_modificado = texto_env.replace("dr", "")
    else:
        texto_modificado = "NI" if texto_env == "ni" else texto_env

    return texto_modificado.replace(".", "").strip()

def get_oauth_token(client_id, client_secret, grant_type, token_url):
    # Dados de autenticação
    auth_data = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": grant_type
    }

    try:
        # Realizar solicitação POST para obter o token de acesso
        response = requests.post(token_url, data=auth_data)
        response.raise_for_status()  # Verificar se ocorreu algum erro na solicitação

        # Retornar o token de acesso
        return response.json()["access_token"]
    except requests.exceptions.RequestException as e:
        print("Erro na solicitação:", e)
        return None

def send_data_with_token(access_token, data, api_url):
    # Cabeçalhos da solicitação com o token de acesso
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    try:
        # Realizar a solicitação POST com os dados fornecidos
        response = requests.post(api_url, headers=headers, json=data)
        response.raise_for_status()  # Verificar se ocorreu algum erro na solicitação

        # Retornar a resposta da API
        return response.json()
    except requests.exceptions.RequestException as e:
        print("Erro na solicitação:", e)
        return None