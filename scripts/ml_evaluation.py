"""
Load saved model predictions from HDFS, compute classification metrics,
and write a simple comparison report for the trained Spark ML models.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

TEAM = 30

spark = SparkSession.builder\
    .appName(f"{TEAM} - model evaluation and comparison")\
    .master("yarn")\
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("Session connected")


def load_predictions(path, model_name):
    """
    Loads predictions from HDFS

    :param path: path to the predictions HDFS
    :param model_name: name of the model
    """

    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(path)
        .select("label", "prediction")
        .withColumn("model", lit(model_name))
    )


model1_predictions = load_predictions(
    "project/output/model1_predictions", "Random Forest")
model2_predictions = load_predictions(
    "project/output/model2_predictions", "Multilayer Perceptron")

print("Data loaded")

evaluator_accuracy = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="accuracy")
evaluator_precision = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="weightedPrecision")
evaluator_recall = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="weightedRecall")
evaluator_f1 = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="f1")


def metrics_row(dataframe):
    """
    Computes evaluation metrics for a given DataFrame.

    :param dataframe: spark DataFrame containing predictions and labels
    :return: Tuple of evaluation metrics
    """

    return (
        evaluator_accuracy.evaluate(dataframe),
        evaluator_precision.evaluate(dataframe),
        evaluator_recall.evaluate(dataframe),
        evaluator_f1.evaluate(dataframe),
    )


comparison_data = [
    ("Random Forest", *metrics_row(model1_predictions)),
    ("Multilayer Perceptron", *metrics_row(model2_predictions)),
]

comparison_df = spark.createDataFrame(
    comparison_data,
    ["model", "accuracy", "precision", "recall", "f1_score"]
)

comparison_df.orderBy(col("f1_score").desc()).show(truncate=False)

comparison_df.coalesce(1)\
    .write.mode("overwrite")\
    .format("csv")\
    .option("header", "true")\
    .save("project/output/evaluation")

spark.stop()
