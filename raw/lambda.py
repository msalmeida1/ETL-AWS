import boto3
import pandas as pd
import requests
import json
import datetime
import os
from io import StringIO

def lambda_handler(event, context):
    s3 = boto3.client('s3',
                  aws_access_key_id='****************************',
                  aws_secret_access_key='********************************')

    # Nome do bucket e nome do arquivo CSV
    bucket_name = 'desafio-parte-1'
    file_name = 'movies.csv'
    dest_bucket_name = 'desafio-parte-1'
    today = datetime.datetime.today()
    year = today.year
    month = today.month
    day = today.day
    # Caminho completo do arquivo no S3
    file_path = f'Raw/Local/CSV/Movies/2023/09/30/movies.csv'

    # Baixar o arquivo CSV do S3
    s3_object = s3.get_object(Bucket=bucket_name, Key=file_path)
    csv_content_filmes = s3_object['Body'].read().decode('utf-8')

    # Ler o CSV com Pandas
    data_filmes = StringIO(csv_content_filmes)
    df = pd.read_csv(data_filmes, sep='|', low_memory=False)

    # Remover duplicatas com base no ID do filme
    df = df.drop_duplicates(subset=['id'])

    # Filtrar filmes
    filtered_df = df[df['genero'].str.contains('Fantasy|Sci-Fi')]

    base_url_find = "https://api.themoviedb.org/3/find/"

    base_url_movie_details = "https://api.themoviedb.org/3/movie/"

    external_ids_list = []

    for external_id in filtered_df['id']:
        url_find = f"{base_url_find}{external_id}?external_source=imdb_id"

        headers_movies = {
            "accept": "application/json",
            "Authorization": f"Bearer ******************************************"
    }

        try:
            response_find = requests.get(url_find, headers=headers_movies)

            if response_find.status_code == 200:
                external_ids = response_find.json().get("movie_results", [])
                for external_id in external_ids:
                    external_ids_list.append(external_id.get("id"))
            else:
             print(f"Erro ao buscar IDs externos: {response_find.json()}")
        except requests.exceptions.RequestException as e:
            print(f"Erro de conexão: {e}")

    # Lista para armazenar detalhes dos filmes
    movie_details_list = []

    # Iterar sobre os IDs externos e buscar detalhes dos filmes
    for movie_id in external_ids_list:
        # Montar a URL completa para buscar detalhes do filme
        url_movie_details = f"{base_url_movie_details}{movie_id}?language=en-US"

        try:
         # Fazer a chamada à API para buscar detalhes do filme
            response_movie_details = requests.get(url_movie_details, headers=headers_movies)

            if response_movie_details.status_code == 200:
             # Adicionar os detalhes do filme à lista
                movie_details_list.append(response_movie_details.json())
            else:
                # Trate os erros da chamada da API de detalhes do filme aqui
             print(f"Erro ao buscar detalhes do filme com o ID {movie_id}: {response_movie_details.json()}")
        except requests.exceptions.RequestException as e:
            # Trate a exceção RequestException aqui
            print(f"Erro de conexão: {e}")

    # Dividir os detalhes dos filmes em lotes de 100
    batch_size = 100
    movie_details_batches = [movie_details_list[i:i + batch_size] for i in range(0, len(movie_details_list), batch_size)]

    # Salvar os lotes de detalhes dos filmes em arquivos JSON
    for batch_index, movie_details_batch in enumerate(movie_details_batches):
        json_data = json.dumps(movie_details_batch, indent=4)
        output_file_movie = f'Raw/JSON/{year:04d}/{month:02d}/{day:02d}/movie_details_batch_{batch_index}.json'
        s3.put_object(Bucket=dest_bucket_name, Key=output_file_movie, Body=json_data)



    # Series

    bucket_name_series = 'desafio-parte-1'
    file_name_series = 'series.csv'

    # Caminho completo do arquivo de séries no S3
    file_path_series = f'Raw/Local/CSV/Series/2023/09/30/series.csv'

    # Baixar o arquivo CSV do S3
    s3_object_series = s3.get_object(Bucket=bucket_name_series, Key=file_path_series)
    csv_content_series = s3_object_series['Body'].read().decode('utf-8')

    # Ler o CSV com Pandas
    data_series = StringIO(csv_content_series)
    df_series = pd.read_csv(data_series, sep='|', low_memory=False)

    # Remover duplicatas com base no ID da série
    df_series = df_series.drop_duplicates(subset=['id'])

    # Filtrar séries
    filtered_df_series = df_series[df_series['genero'].str.contains('Fantasy|Sci-Fi')]

    base_url_find = "https://api.themoviedb.org/3/find/"

    base_url_series_details = "https://api.themoviedb.org/3/tv/"

    external_ids_list = []

    for external_id in filtered_df_series['id']:
        url_find = f"{base_url_find}{external_id}?external_source=imdb_id"

        headers_series = {
            "accept": "application/json",
            "Authorization": f"Bearer *******************************************************"
        }

        try:
            response_find = requests.get(url_find, headers=headers_series)

            if response_find.status_code == 200:
       
                external_ids = response_find.json().get("tv_results", [])
                for external_id in external_ids:
            
                    external_ids_list.append(external_id.get("id"))
            else:
          
                print(f"Erro ao buscar IDs externos: {response_find.json()}")
        except requests.exceptions.RequestException as e:
  
            print(f"Erro de conexão: {e}")

    # Lista para armazenar detalhes das séries
    series_details_list = []

    # Iterar sobre os IDs externos e buscar detalhes das séries
    for series_id in external_ids_list:
        # Montar a URL completa para buscar detalhes da série
        url_series_details = f"{base_url_series_details}{series_id}?language=en-US"

        try:
            # Fazer a chamada à API para buscar detalhes da série
            response_series_details = requests.get(url_series_details, headers=headers_series)

            if response_series_details.status_code == 200:
             # Adicionar os detalhes da série à lista
                series_details_list.append(response_series_details.json())
            else:
                # Trate os erros da chamada da API de detalhes da série aqui
                print(f"Erro ao buscar detalhes da série com o ID {series_id}: {response_series_details.json()}")
        except requests.exceptions.RequestException as e:
            # Trate a exceção RequestException aqui
            print(f"Erro de conexão: {e}")
        
    # Dividir os detalhes das séries em lotes de 100
    batch_size = 100
    series_details_batches = [series_details_list[i:i + batch_size] for i in range(0, len(series_details_list), batch_size)]

    # Salvar os lotes de detalhes das séries em arquivos JSON
    for batch_index, series_details_batch in enumerate(series_details_batches):
        json_data = json.dumps(series_details_batch, indent=4)
        output_file_series = f'Raw/JSON/{year:04d}/{month:02d}/{day:02d}/series_details_batch_{batch_index}.json'
        s3.put_object(Bucket=dest_bucket_name, Key=output_file_series, Body=json_data)