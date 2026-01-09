import pandas as pd
import click
from tqdm.auto import tqdm
from sqlalchemy import create_engine

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
]


def run(
    pg_user: str = "root",
    pg_pass: str = "root",
    pg_host: str = "localhost",
    pg_port: int = 5432,
    pg_db: str = "ny_taxi",
    year: int = 2021,
    month: int = 1,
    target_table: str = "yellow_taxi_trips",
    chunksize: int = 100000,
):
    prefix = (
        f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/"
        f"yellow_tripdata_{year}-{month:02d}.csv.gz"
    )
    print(f"Loading data from {prefix}\n\n\n")
    engine = create_engine(f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}")

    df_iter = pd.read_csv(
        prefix,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize,
    )
    first = True
    for df_chunk in tqdm(df_iter):
        if first:
            df_chunk.head(n=0).to_sql(name=target_table, con=engine, if_exists="replace")
            first = False
        else:
            df_chunk.to_sql(name=target_table, con=engine, if_exists="append")
            print(len(df_chunk))


@click.command()
@click.option("--pg-user", default="root", help="Postgres username")
@click.option("--pg-pass", default="root", help="Postgres password")
@click.option("--pg-host", default="localhost", help="Postgres host")
@click.option("--pg-port", default=5432, type=int, help="Postgres port")
@click.option("--pg-db", default="ny_taxi", help="Postgres database name")
@click.option("--year", default=2021, type=int, help="Year for data file")
@click.option("--month", default=1, type=int, help="Month for data file (1-12)")
@click.option("--target-table", default="yellow_taxi_data", help="Target table name in Postgres")
@click.option("--chunksize", default=100000, type=int, help="Chunk size for pandas read_csv iterator")
def main(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, target_table, chunksize):
    run(
        pg_user=pg_user,
        pg_pass=pg_pass,
        pg_host=pg_host,
        pg_port=pg_port,
        pg_db=pg_db,
        year=year,
        month=month,
        target_table=target_table,
        chunksize=chunksize,
    )


if __name__ == "__main__":
    main()






