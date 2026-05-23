import os
from pyspark.sql import functions as F
from src.utils import load_config, get_spark

def main():
    cfg = load_config()
    spark = get_spark("gold-usd-etl")

    input_path = cfg["input_path"]
    curated_dir = cfg["curated_dir"]
    cols = cfg["columns"]

    # 1) Read raw CSV
    df_raw = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(input_path)
    )

    # 2) Select ONLY the needed columns and alias them to stable names
    #    (This avoids rename issues with case/spacing/BOM, etc.)
    df = df_raw.select(
        F.col(cols["ts"]).alias("ts_str"),
        F.col(cols["open"]).alias("open"),
        F.col(cols["high"]).alias("high"),
        F.col(cols["low"]).alias("low"),
        F.col(cols["close"]).alias("close"),
        F.col(cols["volume"]).alias("volume"),
    )

    # 3) Parse timestamp robustly (try a few common formats)
    df = (
        df
        .withColumn("ts1", F.to_timestamp("ts_str"))
        .withColumn("ts2", F.to_timestamp("ts_str", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("ts3", F.to_timestamp("ts_str", "yyyy-MM-dd'T'HH:mm:ss"))
        .withColumn("timestamp", F.coalesce("ts1", "ts2", "ts3"))
        .drop("ts1","ts2","ts3")
    )

    # 4) Basic cleaning & ordering
    df = (
        df
        .withColumn("open",  F.col("open").cast("double"))
        .withColumn("high",  F.col("high").cast("double"))
        .withColumn("low",   F.col("low").cast("double"))
        .withColumn("close", F.col("close").cast("double"))
        .withColumn("volume",F.col("volume").cast("double"))
        .dropna(subset=["timestamp","close"])
        .orderBy("timestamp")
    )

    # 5) Simple derived fields
    df = (
        df
        .withColumn("price_change", F.col("close") - F.col("open"))
        .withColumn("avg_price", (F.col("high") + F.col("low")) / 2.0)
    )

    # 6) Partitioned Parquet output
    df = df.withColumn("year", F.year("timestamp")).withColumn("month", F.month("timestamp"))
    outpath = os.path.join(curated_dir, "curated.parquet")
    df.write.mode("overwrite").partitionBy("year","month").parquet(outpath)

    print(f"[ETL] Wrote curated data to: {outpath}")

if __name__ == "__main__":
    main()
