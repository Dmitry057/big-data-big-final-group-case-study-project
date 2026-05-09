SET hive.execution.engine=tez;
SET hive.resultset.use.unique.column.names=false;
SET hive.vectorized.execution.enabled=false;
SET hive.vectorized.execution.reduce.enabled=false;

USE team30_projectdb;

DROP TABLE IF EXISTS stage3_model1_predictions_ext;
CREATE EXTERNAL TABLE stage3_model1_predictions_ext (
    label DOUBLE,
    prediction DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'project/output/model1_predictions'
TBLPROPERTIES ('skip.header.line.count'='1');

DROP TABLE IF EXISTS stage3_model2_predictions_ext;
CREATE EXTERNAL TABLE stage3_model2_predictions_ext (
    label DOUBLE,
    prediction DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'project/output/model2_predictions'
TBLPROPERTIES ('skip.header.line.count'='1');

DROP TABLE IF EXISTS stage3_model_evaluation_ext;
CREATE EXTERNAL TABLE stage3_model_evaluation_ext (
    model STRING,
    accuracy DOUBLE,
    model_precision DOUBLE,
    recall DOUBLE,
    f1_score DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'project/output/evaluation'
TBLPROPERTIES ('skip.header.line.count'='1');

DROP TABLE IF EXISTS stage4_model_evaluation;
CREATE TABLE stage4_model_evaluation
STORED AS PARQUET
AS
SELECT
    model,
    accuracy,
    model_precision,
    recall,
    f1_score
FROM stage3_model_evaluation_ext;

DROP TABLE IF EXISTS stage4_prediction_counts;
CREATE TABLE stage4_prediction_counts
STORED AS PARQUET
AS
SELECT
    'Random Forest' AS model,
    label,
    prediction,
    COUNT(*) AS row_count
FROM stage3_model1_predictions_ext
GROUP BY label, prediction
UNION ALL
SELECT
    'Logistic Regression' AS model,
    label,
    prediction,
    COUNT(*) AS row_count
FROM stage3_model2_predictions_ext
GROUP BY label, prediction;

DROP TABLE IF EXISTS stage4_prediction_distribution;
CREATE TABLE stage4_prediction_distribution
STORED AS PARQUET
AS
SELECT
    model,
    prediction,
    SUM(row_count) AS row_count
FROM stage4_prediction_counts
GROUP BY model, prediction;

DROP TABLE IF EXISTS stage4_feature_extraction_summary;
CREATE TABLE stage4_feature_extraction_summary
STORED AS PARQUET
AS
SELECT
    'numeric_features' AS feature_group,
    13 AS feature_count,
    'kept as numeric and scaled with StandardScaler' AS treatment,
    'duration, bytes, packets, ports, year, local flags' AS examples
UNION ALL
SELECT
    'cyclical_time_features' AS feature_group,
    10 AS feature_count,
    'timestamp parts encoded with sine and cosine pairs' AS treatment,
    'month, day, hour, minute, second' AS examples
UNION ALL
SELECT
    'categorical_features' AS feature_group,
    4 AS feature_count,
    'StringIndexer, OneHotEncoder, then ChiSqSelector with fpr=0.1' AS treatment,
    'proto, service, conn_state, history' AS examples
UNION ALL
SELECT
    'final_feature_vector' AS feature_group,
    152 AS feature_count,
    'assembled selected categorical, numeric, and time features' AS treatment,
    'features column used by Spark ML models' AS examples
UNION ALL
SELECT
    'target_label' AS feature_group,
    1 AS feature_count,
    'StringIndexer converted string label to numeric class label' AS treatment,
    'label' AS examples;

DROP TABLE IF EXISTS stage4_hyperparameter_results;
CREATE TABLE stage4_hyperparameter_results
STORED AS PARQUET
AS
SELECT
    'Random Forest' AS model,
    'numTrees' AS hyperparameter,
    '[30, 60]' AS tested_values,
    '60' AS best_value,
    2 AS cv_folds,
    'f1' AS selection_metric
UNION ALL
SELECT
    'Random Forest' AS model,
    'maxDepth' AS hyperparameter,
    '[4, 6]' AS tested_values,
    '6' AS best_value,
    2 AS cv_folds,
    'f1' AS selection_metric
UNION ALL
SELECT
    'Logistic Regression' AS model,
    'regParam' AS hyperparameter,
    '[0.01, 0.1]' AS tested_values,
    '0.1' AS best_value,
    2 AS cv_folds,
    'f1' AS selection_metric
UNION ALL
SELECT
    'Logistic Regression' AS model,
    'elasticNetParam' AS hyperparameter,
    '[0.0, 0.5]' AS tested_values,
    '0.5' AS best_value,
    2 AS cv_folds,
    'f1' AS selection_metric;

SHOW TABLES LIKE 'stage4_*';
