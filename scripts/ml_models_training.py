"""Train the project models sequentially: Random Forest first, then Multilayer Perceptron.

The script reuses the same persisted train/test samples, runs grid search with
cross-validation for each model, and saves models plus prediction CSVs to HDFS.
"""

from pyspark import StorageLevel
from pyspark.ml.classification import MultilayerPerceptronClassifier, RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import SparkSession

TEAM = 30
DESIRED_TOTAL_CORES = 12
SEED = 42
NUM_CLASSES = 7


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
        .addGrid(rf_classifier.numTrees, [150, 200])
        .addGrid(rf_classifier.maxDepth, [5, 8])
        .addGrid(rf_classifier.minInstancesPerNode, [5, 10])
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
        numFolds=3,
        parallelism=3,
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


def train_mlp(train_data, test_data):
    """
    Trains a Multilayer Perceptron classifier with Spark ML

    :param train_data: train spark dataframe
    :param test_data: test spark dataframe
    """

    print("Starting model 2 (Multilayer Perceptron)")

    input_size = int(train_data.select("features").head()[0].size)
    layers = [input_size, 64, 32, NUM_CLASSES]


    print(f"Num classes: {NUM_CLASSES}, feature size: {input_size}")

    mlp = MultilayerPerceptronClassifier(
        labelCol="label",
        featuresCol="features",
        maxIter=100,
        layers=layers,
        seed=SEED,
        solver="gd"
    )

    param_grid = (
        ParamGridBuilder()
        .addGrid(mlp.blockSize, [128, 256])
        .addGrid(mlp.stepSize, [0.03, 0.1])
        .build()
    )

    print(f"MLP total combinations: {len(param_grid)}")

    evaluator_f1 = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="f1",
    )

    cross_validator = CrossValidator(
        estimator=mlp,
        estimatorParamMaps=param_grid,
        evaluator=evaluator_f1,
        numFolds=3,
        parallelism=3,
        seed=SEED,
    )

    print("Starting MLP k-fold cross validation")
    cv_model_mlp = cross_validator.fit(train_data)
    best_model = cv_model_mlp.bestModel

    print("Best model parameters:")
    for param, value in zip(
        [p.name for p in mlp.params], best_model.extractParamMap().values()
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
    train_mlp(train_data, test_data)

    train_data.unpersist(blocking=True)
    spark.stop()


if __name__ == "__main__":
    main()
