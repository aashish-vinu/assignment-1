import argparse
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main(input_path, output_path, stats_path):
    spark = SparkSession.builder.appName("TaxiClean").getOrCreate()

    df = spark.read.parquet(input_path)
    records_read = df.count()

    for c in df.columns:
        df = df.withColumnRenamed(c, c.lower())

    df_clean = df.filter(
        (col("tpep_pickup_datetime").isNotNull()) &
        (col("trip_distance") > 0)
    )
    records_written = df_clean.count()
    df_clean.write.mode("overwrite").parquet(output_path)

    stats = {
        "records_read": records_read,
        "records_written": records_written,
        "status": "success" if records_written > 0 else "failure"
    }
    with open(stats_path, "w") as f:
        json.dump(stats, f)

    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--stats", required=True)
    args = parser.parse_args()
    main(args.input, args.output, args.stats)