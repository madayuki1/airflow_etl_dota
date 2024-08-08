import pandas as pd
from sqlalchemy import create_engine

def extract(filename):
    df = pd.read_csv(f'data/raw/{filename}.csv')
    engine = create_engine('postgres+psycopg2://postgres:postgres@postgres-data/postgres')
    df.to_sql(f"bronze_{filename}", engine, if_exists='replace', index=False)
    # df.to_csv(f'data/raw/{filename}.csv')

def transform_heroes(filename):
    # df = pd.read_csv(f'data/raw/{filename}.csv')
    engine = create_engine('postgres+psycopg2://postgres:postgres@postgres-data/postgres')
    df = pd.read_sql(f"bronze_{filename}", engine)
    df_drop = df.drop('Unnamed: 0.1', axis=1)
    df_rename = df_drop.rename(columns={'Unnamed: 0': 'index', 'Name': "hero_name", 'Hero ID': 'hero_id'})
    df_rename.to_sql(f"silver_{filename}", engine, if_exists='replace', index=False)
    # df_rename.to_csv(f'data/transformed/{filename}.csv')

def transform_meta(filename):
    # df = pd.read_csv(f'data/raw/{filename}.csv')
    engine = create_engine('postgres+psycopg2://postgres:postgres@postgres-data/postgres')
    df = pd.read_sql(f"bronze_{filename}", engine)
    df_drop = df.drop('Unnamed: 0.1', axis=1)
    df_rename = df_drop.rename(columns={'Unnamed: 0': 'index', 'Name': "hero_name", "Primary Attribute": "primary_attribute", \
                                   "Attack Range": "attack_range", "Roles": "roles", "Total Pro wins": "total_pro_wins", \
                                    "Times Picked": "times_picked", "Times Banned": "times_banned", "Win Rate": "win_rate"\
                                        , "Niche Hero?": "niche_hero?"})
    df_rename.to_sql(f"silver_{filename}", engine, if_exists='replace', index=False)

def load(filename):
    # df = pd.read_csv(f'/opt/airflow/data/transformed/{filename}.csv')
    engine = create_engine('postgres+psycopg2://postgres:postgres@postgres-data/postgres')
    df = pd.read_sql(f"silver_{filename}", engine)
    df.to_sql(f"gold_{filename}", engine, if_exists='replace', index=False)
    return 