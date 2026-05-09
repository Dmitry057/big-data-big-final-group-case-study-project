"""
Prepare IoT network data for Spark ML by loading Hive data,
encoding time features, building the feature pipeline,
and splitting train/test data, and saving parquet-ready ML datasets.

Parquet is used for saving instead of json to persist Vector type of features
to avoid casting in future
"""

import math
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.ml import Transformer
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.ml import Pipeline
from pyspark.ml.feature import (StringIndexer,
                                OneHotEncoder,
                                VectorAssembler,
                                StandardScaler,
                                ChiSqSelector)


class CyclicalTimeTransformer(Transformer, DefaultParamsReadable, DefaultParamsWritable):
    """
    A PySpark Transformer that extracts cyclical time features from a timestamp column.
    This transformer takes a timestamp column as input and generates additional columns
    representing the cyclical nature of time features such as year, month, day, hour,
    minute, and second. It computes sine and cosine transformations for these features
    to capture their periodicity.
    Parameters
    ----------
    inputCol : str, optional
        The name of the input timestamp column. Default is "ts".
    outputCol : str, optional
        The name of the output column containing the generated time features.
        Default is "time_features".
    Methods
    -------
    _transform(dataset)
        Transforms the input DataFrame by adding cyclical time features.
    Returns
    -------
    DataFrame
        A DataFrame with the original columns and additional columns for year, month, day, hour,
        minute, second, and their respective sine and cosine transformations.
    """

    def __init__(self, input_col="ts", output_col="time_features"):
        super().__init__()
        self.inputCol = input_col
        self.outputCol = output_col

    def _transform(self, dataset):
        return dataset\
            .withColumn("year", F.year(F.col(self.inputCol)))\
            .withColumn("month", F.month(F.col(self.inputCol)))\
            .withColumn("day", F.dayofmonth(F.col(self.inputCol)))\
            .withColumn("hour", F.hour(F.col(self.inputCol)))\
            .withColumn("minute", F.minute(F.col(self.inputCol)))\
            .withColumn("second", F.second(F.col(self.inputCol)))\
            .withColumn("month_sin", F.sin(2 * math.pi * F.col("month") / 12))\
            .withColumn("month_cos", F.cos(2 * math.pi * F.col("month") / 12))\
            .withColumn("day_sin", F.sin(2 * math.pi * F.col("day") / 31))\
            .withColumn("day_cos", F.cos(2 * math.pi * F.col("day") / 31))\
            .withColumn("hour_sin", F.sin(2 * math.pi * F.col("hour") / 24))\
            .withColumn("hour_cos", F.cos(2 * math.pi * F.col("hour") / 24))\
            .withColumn("minute_sin", F.sin(2 * math.pi * F.col("minute") / 60))\
            .withColumn("minute_cos", F.cos(2 * math.pi * F.col("minute") / 60))\
            .withColumn("second_sin", F.sin(2 * math.pi * F.col("second") / 60))\
            .withColumn("second_cos", F.cos(2 * math.pi * F.col("second") / 60))

    def get_input_col(self):
        """
        returns the name of the input column
        """

        return self.inputCol

    def get_output_col(self):
        """
        returns the name of the output column
        """

        return self.outputCol


print("=" * 40)
print("Started ML preprocessing")


TEAM = 30
WAREHOUSE = "project/hive/warehouse"
NETWORK_CONNECTIONS_PATH = "project/hive/warehouse/network_connections_part"

spark = SparkSession.builder\
    .appName(f"{TEAM} - data preparation")\
    .master("yarn")\
    .config("spark.sql.warehouse.dir", WAREHOUSE)\
    .config("spark.sql.avro.compression.codec", "snappy")\
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
spark._jsc.hadoopConfiguration().set("dfs.replication", "1")


print("Data schema")
df = spark.read.format("parquet").load(NETWORK_CONNECTIONS_PATH)
df.printSchema()

print("Null values")
df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c)
          for c in df.columns]).show()

time_transformer = CyclicalTimeTransformer(
    input_col="ts", output_col="time_features")
df_with_time = time_transformer.transform(df)

# Dropping tunnel_parents because it's all nulls
df_with_time = df_with_time.drop("ts", "uid", "tunnel_parents")
df_with_time = df_with_time.fillna({
    "history": "unknown",
    "local_orig": False,
    "local_resp": False,
    "missed_bytes": 0,
    "service": "unknown",
    "duration": 0.0,
    "orig_bytes": 0.0,
    "resp_bytes": 0.0,
    "detailed_label": "unknown",
})


# Preprocess pipeline
categorical_features = ["proto", "service", "conn_state", "history"]
numeric_features = [
    "duration", "orig_bytes", "resp_bytes", "missed_bytes",
    "orig_pkts", "orig_ip_bytes", "resp_pkts", "resp_ip_bytes",
    "id_orig_p", "id_resp_p", "year", "local_orig", "local_resp"
]
time_encoded_features = [
    "month_sin", "month_cos", "day_sin", "day_cos",
    "hour_sin", "hour_cos", "minute_sin", "minute_cos",
    "second_sin", "second_cos"
]

string_indexers_cat = [
    StringIndexer(inputCol=col, outputCol=f"{col}_indexed").setHandleInvalid("skip")
    for col in categorical_features
]

one_hot_encoders = [
    OneHotEncoder(inputCol=f"{col}_indexed", outputCol=f"{col}_encoded")
    for col in categorical_features
]

categorical_assembler = VectorAssembler(
    inputCols=[f"{col}_encoded" for col in categorical_features],
    outputCol="categorical_features_raw"
)

label_indexer = StringIndexer(inputCol="label", outputCol="label_indexed").setHandleInvalid("skip")

chi_sq_selector = ChiSqSelector(
    selectorType="fpr",
    fpr=0.1,
    featuresCol="categorical_features_raw",
    outputCol="selected_categorical_features",
    labelCol="label_indexed"
)

all_vector_assembler = VectorAssembler(
    inputCols=numeric_features + time_encoded_features + ["selected_categorical_features"],
    outputCol="features_raw"
)

scaler = StandardScaler(
    inputCol="features_raw",
    outputCol="features",
    withMean=False,
    withStd=True
)

pipeline = Pipeline(
    stages=string_indexers_cat + one_hot_encoders + [
    categorical_assembler,
    label_indexer,
    chi_sq_selector,
    all_vector_assembler,
    scaler
])

print(f"All Categorical features: {categorical_features}")
print(f"Numeric features: {numeric_features}")
print(f"Time-encoded features: {time_encoded_features}")

train_data_raw, test_data_raw = df_with_time.randomSplit([0.6, 0.4], seed=42)

pipeline_model = pipeline.fit(train_data_raw)
train_data_transformed = pipeline_model.transform(train_data_raw)
test_data_transformed = pipeline_model.transform(test_data_raw)

train_data = train_data_transformed.select(["features", "label_indexed"])
test_data = test_data_transformed.select(["features", "label_indexed"])

train_data = train_data.withColumnRenamed("label_indexed", "label")
test_data = test_data.withColumnRenamed("label_indexed", "label")


print("Preprocessing complete, saving to HDFS")

# Commiting to hdfs (partitioning to speed up computations)
train_data.select("features", "label")\
    .coalesce(4)\
    .write\
    .mode("overwrite")\
    .format("parquet")\
    .save("project/data/train")

test_data.select("features", "label")\
    .coalesce(4)\
    .write\
    .mode("overwrite")\
    .format("parquet")\
    .save("project/data/test")

print("=" * 40)
print("Preprocessing completed")
