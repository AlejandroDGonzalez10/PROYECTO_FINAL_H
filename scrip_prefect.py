

from prefect import flow, task
import pandas as pd
import pycountry
import numpy as np


@task
def read_data():
    df = pd.read_csv('./linkedin-jobs-Latam_DS.csv', delimiter=',', encoding='utf-8')
    return df

@task
def clean_data(df):
    df = df.drop_duplicates()
    df.drop(df.index[0], inplace=True)
    df["nivel_de_experiencia"] = df["criteria"].str.extract(r"'(Nível de experiência|Nivel de antigüedad)':\s+'([^']+)'\s*}")[1]
    df["tipo_empleo"] = df["criteria"].str.extract(r"'(Tipo de emprego|Tipo de empleo)':\s+'([^']+)'\s*}")[1]
    df['nivel_de_experiencia'] = df['nivel_de_experiencia'].replace({'Não aplicável': np.nan,
                                                                       'Pleno-sênior': 'Semi-senior-Senior',
                                                                       'Assistente': 'Ayudante',
                                                                       'Estágio': 'Pasante',
                                                                       'Júnior': 'Junio'})
    df['tipo_empleo'] = df['tipo_empleo'].replace({'Tempo integral': 'Fulltime',
                                                   'Jornada completa': 'Fulltime',
                                                   'Media jornada': 'Part time',
                                                   'Contrato': 'Otro',
                                                   'Outro': 'Otro'})
    df = df.drop(['criteria','link'], axis=1)
    df.fillna(0, inplace=True)
    df = df.rename(columns={'title': 'titulo_puesto', 'company': 'empresa', 'description': 'descripcion_puesto', 'onsite_remote':'modalidad','salary':'salario','posted_date':'fecha_posteo','location':'localidad'})
    return df

@task
def replace_country_names(df):
    paises = [pais.name for pais in pycountry.countries]
    
    def identificar_pais(cadena):
        for pais in paises:
            if pais in cadena:
                return pais
        return cadena

    df['localidad'] = df['localidad'].apply(identificar_pais)
    return df

@task
def get_results(df):
    return df


@flow
def clean_data_flow():
    df = read_data()
    df = clean_data(df)
    df = replace_country_names(df)
    print(df)

if __name__ == '__main__':
    clean_data_flow()














