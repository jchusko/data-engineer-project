from pandas import *
from ast import literal_eval
import os

# Custom evaluator to parse the Genres and 
# Production Companies into arrays of jsons
def custom_eval(value):
    import json
    try:
        arr = []
        litVal = literal_eval(value)
        if isinstance(litVal, list):
            if len(litVal) > 0:
                return(litVal)
            else:
                return [{'id':-1, 'name': 'NONE'}]
        else:
            return [{'id':-2, 'name': 'UNDETERMINED'}]
    except:
        return [{'id':-2, 'name': 'UNDETERMINED'}]

# Output directory
tablePath = './tables'
if not os.path.exists(tablePath):
    os.mkdir(tablePath)

# Load into dataframe
df = pandas.read_csv('./movies_metadata.csv', usecols = ['id', 'genres', 'production_companies', 'budget', 'revenue', 'release_date', 'popularity'], converters={'genres':custom_eval, 'production_companies':custom_eval})

# Recasting of relevant columns
df.budget = to_numeric(df.budget, errors='coerce')
df.revenue = to_numeric(df.revenue, errors='coerce')
df.popularity = to_numeric(df.revenue, errors='coerce')
df.release_date = to_datetime(df.release_date, errors='coerce')

# Adding columns
df['year'] = df.release_date.dt.year
df['month'] = df.release_date.dt.month
df['profit'] = df.revenue - df.budget 

# Movie Data
df[['id', 'year', 'month', 'budget', 'revenue', 'profit', 'release_date', 'popularity']].to_csv('./tables/movies.csv', index=False)

# Genre
df_genres = df.explode('genres').reset_index(drop=True).rename(columns = {'id':'movie_id'})
df_genres[['genre_id', 'genre_name']] = json_normalize(df_genres['genres'], errors='ignore')

df_genres[['genre_id', 'genre_name']].drop_duplicates(subset=['genre_id', 'genre_name']).sort_values(by=['genre_id']).to_csv('./tables/ref_genres.csv', index=False)
df_genres[['movie_id', 'genre_id']].to_csv('./tables/rel_movie_genre.csv', index=False)

# Production Company
df_prods = df.explode('production_companies').reset_index(drop=True).rename(columns = {'id':'movie_id'})
df_prods[['production_name', 'production_id']] = json_normalize(df_prods['production_companies'])

df_prods[['production_id', 'production_name']].drop_duplicates(subset=['production_id', 'production_name']).sort_values(by=['production_id']).to_csv('./tables/ref_production_companies.csv', index=False)
df_prods[['movie_id', 'production_id']].to_csv('./tables/rel_movie_production_company.csv', index=False)
