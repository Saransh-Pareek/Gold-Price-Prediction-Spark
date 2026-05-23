import yaml
from pyspark.sql import SparkSession

def load_config(path="config.yaml"):
    """Load YAML configuration file."""
    with open(path, "r") as f:
        return yaml.safe_load(f)

def get_spark(app_name="gold-usd-analytics"):
    """Create or retrieve a local SparkSession."""
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )
    return spark
