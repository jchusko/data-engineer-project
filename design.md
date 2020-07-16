# Introduction

This project contains a demonstration of data transformation and a sample architectecture for the Kaggle Movies Dataset.

# Instructions

Place a copy of movies_metadata.csv into the root directory.

```
pip install pandas
python movies.py
```

# Discussion

## Data Model

There are some questions about these data that could potentially alter the model.

1. Who are these data for? If they're strictly for analysis, there may not be a need to normalize the model as a denormalized table would be quicker to read. If we're interested in creating an app around this model, a normalized structure would be beneficial for developers.
1. What is the nature of the monthly data drops? Are we receiving all of history each month as we were provided in the example, or are we getting monthly updates? I chose to separate year and month into separate fields. This lets us insert data into year/month pairs which eases reprocessing and it gives us the ability to partition down to the month level should we expect this set to grow significantly.
1. How do we handle double-counting? For instance `budget per year by genre` leaves us a situation where some movies are classified under multiple genres. Does an Action/Adventure movie get the budget counted under both Action and Adventure? The same issue exists with the Production Companies.
1. What does `most popular genre by year` mean? Sum? Average? Max?

The model I created is normalized and includes only the fields needed to calculate the desired metrics. A model for the full movie dataset would obviously include all available fields and require many more relations.

## Transformation

The transformation performed in `movies.py` produces five resulting data sets: the movies fact table, reference tables for genres and companies, and the relations between those references and the movies. This is sufficient for calculating the desired metrics.

If this data set grows significantly, we may find it fit to pre-aggregate these metric for the analysts and store them in their own tables. 

As a concern of scale, the Pandas library may become unfit as jobs cannot be distributed. If we anticipate running into a compute or memory bottlenecks, we may want to pivot to a spark engine as those jobs can be split across machines.

## Design

We're short on specifics as to a desired design, so we are going to have to make a few assumptions:

1. Our data source is provided by a 3rd party via SFTP
1. We want an entirely cloud-based solution

### Storage

I've selected S3 as the main storage. It's cheap, very scalable and interacts with most major cloud services and analytic platforms.

### Ingestion

As part of the simplifying assumptions, we can configure AWS Transfer to allow uploads of our data via SFTP directly into S3.

### Processing

We can kick off our transformation scripts by utilizing AWS SNS upon new file uploads. A modified version of `movies.py` could be deployed as a Lambda which could process our data and output the results. We should also move the raw files to a `processed` path within our source bucket upon completion or a `failed` path upon failure. In disaster scenarios, we should always be able to recreate all data from the raw backups.

The resulting data should be stored in a relational database in a columnar format, ORC or Parquet. Redshift is an appropriate choice of database.

### API

We can write bespoke Lambda functions to access our Redshift data. The AWS API Gateway can be used as the entry point as it can handle authentication and all permissions will be managed from AWS user accounts.

At minimum, the API should allow users to pull in movie data by year or range of years. Some filtering by genre or production company seems reasonable. While the payload should be json, the exact schema should come from an agreement between Data Engineering and the Analysts/Software Engineers.