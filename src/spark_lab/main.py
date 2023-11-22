import pyspark
from pyspark.sql import SparkSession, functions
# import json


def rename_columns(
    df: pyspark.sql.DataFrame, new_names: list[str]
) -> pyspark.sql.DataFrame:
    return df.select([functions.col(c).alias(new_names[i]) for i, c in enumerate(df.columns)])


def download(
    spark: SparkSession, data_path: str, delimiter: str
) -> pyspark.sql.DataFrame:
    return spark.read.option("delimiter", delimiter).csv(data_path)


def main() -> None:
    spark = SparkSession.builder.getOrCreate()

    data_path = "hdfs://localhost:9000/user/lab/u.data"
    data_names = ["user id", "item id", "rating", "timestamp"]
    df = rename_columns(download(spark, data_path, "\t"), data_names)
    df.show()

    item_path = "hdfs://localhost:9000/user/lab/u.item"
    item_names = [
        "movie id", "movie title", "release date", "video release date",
        "IMDb URL", "unknown", "Action", "Adventure", "Animation", "Children's",
        "Comedy", "Crime", "Documentary", "Drama", "Fantasy", "Film-Noir", "Horror",
        "Musical", "Mystery", "Romance", "Sci-Fi", "Thriller", "War", "Western"
    ]
    df_items = rename_columns(download(spark, item_path, "|"), item_names)
    df_items.show()


if __name__ == "__main__":
    main()
