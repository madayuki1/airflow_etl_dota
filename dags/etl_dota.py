import pandas as pd
from sqlalchemy import create_engine

def extract(filename):
    df = pd.read_csv(f'data/rawest/{filename}.csv')
    df.to_csv(f'data/raw/{filename}.csv')

def transform(filename):
    df = pd.read_csv(f'data/raw/{filename}.csv')
    df_rename = df.rename(columns={'#': 'index', 'Name': "hero_name", 'Hero ID': 'hero_id'})
    df_rename.to_csv(f'data/transformed/{filename}.csv')

def load(filename):
    return 