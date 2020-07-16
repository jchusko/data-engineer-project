from pandas import *

df_movies = pandas.read_csv('./tables/movies.csv')
df_genres = pandas.read_csv('./tables/ref_genres.csv')
df_movie_to_genre = pandas.read_csv('./tables/rel_movie_genre.csv')
df_companies = pandas.read_csv('./tables/ref_production_companies.csv')
df_movie_to_company = pandas.read_csv('./tables/rel_movie_production_company.csv')

genreMetrics = df_movies.join(df_movie_to_genre, lsuffix='id', rsuffix='movie_id') \
.merge(df_genres, on='genre_id') \
.groupby(['year','genre_name']) \
.agg({'budget':'sum', 'revenue':'sum', 'profit':'sum', 'popularity':'sum'})

companyMetrics = df_movies.join(df_movie_to_company, lsuffix='id', rsuffix='movie_id') \
.merge(df_companies, on='production_id') \
.groupby(['year','production_name']) \
.agg({'budget':'sum', 'revenue':'sum', 'profit':'sum', 'popularity':'mean'})

companyGenreMetrics = df_movies.join(df_movie_to_company, lsuffix='id', rsuffix='movie_id') \
.merge(df_companies, on='production_id') \
.join(df_movie_to_genre, lsuffix='id', rsuffix='movie_id') \
.merge(df_genres, on='genre_id') \
[['year','production_name', 'genre_name', 'id']] \
.groupby(['year','production_name', 'genre_name']) \
.count()
