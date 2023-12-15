import pyspark
from pyspark.sql import SparkSession, functions
import json


def rename_columns(
    df: pyspark.sql.DataFrame, new_names: list[str]
) -> pyspark.sql.DataFrame:
    return df.select(
        [functions.col(c).alias(new_names[i]) for i, c in enumerate(df.columns)]
    )


def download(
    spark: SparkSession, data_path: str, delimiter: str
) -> pyspark.sql.DataFrame:
    return spark.read.option("delimiter", delimiter).csv(data_path)


def get_json(data: dict) -> None:
    with open("output.json", "w") as F:
        F.write(json.dumps(data, indent=4))


def main() -> None:
    print("Start")
    spark = SparkSession.builder.master("local").getOrCreate()
    print("Got past session")

    data_path = "hdfs://localhost:9900/user/lab/u.data"
    data_names = ["user id", "item id", "rating", "timestamp"]
    df = rename_columns(download(spark, data_path, "\t"), data_names)
    print("u.data table")
    df.show()

    item_path = "hdfs://localhost:9900/user/lab/u.item"
    item_names = [
        "movie id",
        "movie title",
        "release date",
        "video release date",
        "IMDb URL",
        "unknown",
        "Action",
        "Adventure",
        "Animation",
        "Children's",
        "Comedy",
        "Crime",
        "Documentary",
        "Drama",
        "Fantasy",
        "Film-Noir",
        "Horror",
        "Musical",
        "Mystery",
        "Romance",
        "Sci-Fi",
        "Thriller",
        "War",
        "Western",
    ]
    df_items = rename_columns(download(spark, item_path, "|"), item_names)
    print("u.item table")
    df_items.show(n=5)

    FILM_ID = 211 + 5
    print("Target film")
    df_items[df_items["movie id"] == FILM_ID].show()

    rat_film = (
        df[df["item id"] == FILM_ID]
        .groupBy("rating")
        .agg({"timestamp": "count"})
        .sort("rating")
    )
    rat_film = rat_film.select(
        "rating", functions.col("count(timestamp)").alias("rat_film")
    )

    rat_all = df.groupBy("rating").agg({"timestamp": "count"}).sort("rating")
    rat_all = rat_all.select(
        "rating", functions.col("count(timestamp)").alias("rat_all")
    )

    combined_df = (
        rat_film.join(rat_all, "rating")
        .sort("rating")
        .select(functions.col("rat_film"), functions.col("rat_all"))
    )
    print("Resulting table")
    combined_df.show()

    dict_data = {
        column: combined_df.select(column).rdd.flatMap(lambda x: x).collect()
        for column in combined_df.columns
    }
    get_json(dict_data)
    print("JSON output")
    print(json.dumps(dict_data, indent=4))


if __name__ == "__main__":
    main()
