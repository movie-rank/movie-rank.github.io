import logging
import sqlite3
from pathlib import Path
from time import time

import pandas as pd

URLTEMPLATE = "https://datasets.imdbws.com/title.{}.tsv.gz"
MINVOTES = 5000
CHUNKSIZE = 10 ** 4
SCHEMA = {
    "ratings": {"tconst": "string", "averageRating": "Float32", "numVotes": "Int32"},
    "basics": {
        "tconst": "string",
        "titleType": "string",
        "primaryTitle": "string",
        "originalTitle": "string",
        # "isAdult",
        "startYear": "Int32",
        # "endYear": "Int32",
        # "runtimeMinutes",
        "genres": "string",
    },
}
RENAMES = {
    "primaryTitle": "title",
    "startYear": "year",
    "averageRating": "rating",
    "genres": "genres",
    "numVotes": "votes",
    "titleType": "type",
    "originalTitle": "orig_title",
}

logger = logging.getLogger(__name__)


def fetch_table(dataname, **kwargs):
    df = pd.read_table(
        URLTEMPLATE.format(dataname),
        usecols=list(SCHEMA[dataname]),
        dtype=SCHEMA[dataname],
        na_values=r"\N",
        index_col="tconst",
        **kwargs,
    )
    logger.info(f"Fetched {dataname}")
    return df


def iter_movies(title_types=None):
    ratings = fetch_table("ratings")[lambda d: d.numVotes >= MINVOTES]
    basics_iter = fetch_table("basics", chunksize=CHUNKSIZE)

    for df in basics_iter:
        df = df.join(ratings, how="inner")

        if title_types is not None:
            df = df[df.titleType.isin(title_types)]

        df = (
            df[list(RENAMES)]
            .rename(columns=RENAMES)
            .assign(rating=lambda d: d.rating.astype("float32").round(1))
        )

        yield df


def to_sqlite(df_iter, path):
    path = Path(path)

    if path.exists():
        path.unlink()

    path.parent.mkdir(exist_ok=True, parents=True)
    conn = sqlite3.connect(path, isolation_level=None)

    for df in df_iter:
        df.to_sql("movies", conn, if_exists="append", index=False)

    conn.close()
    logger.info(f"Written {path}")


def main():
    start = time()
    logging.basicConfig(
        format="%(asctime)s %(levelname)7s [%(name)s] - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S%z",
        level="INFO",
    )
    to_sqlite(iter_movies(), "data/im.db")
    duration = time() - start
    logger.info(f"Total runtime: {duration:.1f}s")


if __name__ == "__main__":
    main()
