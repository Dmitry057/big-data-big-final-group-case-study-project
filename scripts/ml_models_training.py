"""Train the project models sequentially: Random Forest first, then Logistic Regression.

The script reuses the same persisted train/test samples, runs grid search with
cross-validation for each model, and saves models plus prediction CSVs to HDFS.
"""

from pyspark import StorageLevel
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import SparkSession

TEAM = 30
DESIRED_TOTAL_CORES = 18
SEED = 42
NUM_CLASSES = 7
CV_FOLDS = 2
CV_PARALLELISM = 3


def load_sample_data(spark):
    """
    Loads training data from HDFS and persists it

    :param spark: spark session
    """

    spark.conf.set("spark.sql.shuffle.partitions", str(DESIRED_TOTAL_CORES))
    spark.conf.set("spark.default.parallelism", str(DESIRED_TOTAL_CORES))

    train_data = spark.read.format("parquet").load("project/data/train")
    test_data = spark.read.format("parquet").load("project/data/test")

    train_data = train_data.repartition(DESIRED_TOTAL_CORES).persist(
        StorageLevel.MEMORY_AND_DISK
    )
    test_data = test_data.repartition(DESIRED_TOTAL_CORES)

    print("Data loaded")

    return train_data, test_data


def train_random_forest(train_data, test_data):
    """
    Trains a Random Forest classifier with Spark ML

    :param train_data: train spark dataframe
    :param test_data: test spark dataframe
    """


    print("Starting model 1 (Random Forest)")

    rf_classifier = RandomForestClassifier(
        labelCol="label",
        featuresCol="features",
        seed=SEED,
    )

    param_grid_rf = (
        ParamGridBuilder()
        .addGrid(rf_classifier.numTrees, [30, 60])
        .addGrid(rf_classifier.maxDepth, [4, 6])
        .build()
    )

    print(f"RF total combinations: {len(param_grid_rf)}")

    evaluator_f1 = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="f1",
    )

    cv_rf = CrossValidator(
        estimator=rf_classifier,
        estimatorParamMaps=param_grid_rf,
        evaluator=evaluator_f1,
        numFolds=CV_FOLDS,
        parallelism=CV_PARALLELISM,
        seed=SEED,
    )

    print("Starting RF k-fold cross validation")
    cv_model_rf = cv_rf.fit(train_data)
    best_model_rf = cv_model_rf.bestModel

    print("Best model parameters:")
    for param, value in zip(
        [p.name for p in rf_classifier.params],
        best_model_rf.extractParamMap().values(),
    ):
        print(f"  {param}: {value}")

    print("Saving model 1")
    best_model_rf.write().overwrite().save("project/models/model1")

    print("Predicting with model 1")
    (
        best_model_rf.transform(test_data)
        .select("label", "prediction")
        .coalesce(1)
        .write.mode("overwrite")
        .format("csv")
        .option("sep", ",")
        .option("header", "true")
        .save("project/output/model1_predictions")
    )


def train_logistic_regression(train_data, test_data):
    """
    Trains a Logistic Regression classifier with Spark ML

    :param train_data: train spark dataframe
    :param test_data: test spark dataframe
    """

    print("Starting model 2 (Logistic Regression)")

    logistic_regression = LogisticRegression(
        labelCol="label",
        featuresCol="features",
        maxIter=50,
        family="multinomial",
    )

    param_grid = (
        ParamGridBuilder()
        .addGrid(logistic_regression.regParam, [0.01, 0.1])
        .addGrid(logistic_regression.elasticNetParam, [0.0, 0.5])
        .build()
    )

    print(f"Logistic Regression total combinations: {len(param_grid)}")

    evaluator_f1 = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="f1",
    )

    cross_validator = CrossValidator(
        estimator=logistic_regression,
        estimatorParamMaps=param_grid,
        evaluator=evaluator_f1,
        numFolds=CV_FOLDS,
        parallelism=CV_PARALLELISM,
        seed=SEED,
    )

    print("Starting Logistic Regression k-fold cross validation")
    cv_model = cross_validator.fit(train_data)
    best_model = cv_model.bestModel

    print("Best model parameters:")
    for param, value in zip(
        [p.name for p in logistic_regression.params], best_model.extractParamMap().values()
    ):
        print(f"  {param}: {value}")

    print("Saving model 2")
    best_model.write().overwrite().save("project/models/model2")

    print("Predicting with model 2")
    (
        best_model.transform(test_data)
        .select("label", "prediction")
        .coalesce(1)
        .write.mode("overwrite")
        .format("csv")
        .option("sep", ",")
        .option("header", "true")
        .save("project/output/model2_predictions")
    )


def main():
    """
    Main entry point for the script.
    Builds Spark session, loads data, trains both models sequentially,
    """

    spark = SparkSession.builder\
        .appName(f"{TEAM} - model training")\
        .master("yarn")\
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    print("Session connected")


    train_data, test_data = load_sample_data(spark)

    train_random_forest(train_data, test_data)
    train_logistic_regression(train_data, test_data)

    train_data.unpersist(blocking=True)
    spark.stop()


if __name__ == "__main__":
    main()
