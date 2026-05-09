# Team 30 — IoT Malware Detection on Hadoop

End-to-end big data pipeline for the **Big Data — Final Project** course
(Innopolis University, 2026). The pipeline ingests the
[Aposemat IoT-23](https://www.stratosphereips.org/datasets-iot23) corpus of
Zeek connection logs into PostgreSQL, lifts it onto a Hive warehouse on HDFS,
trains distributed Spark ML classifiers for 7 malware behaviours, and surfaces
the results through an Apache Superset dashboard.

> **Cluster:** Innopolis Hadoop, 3 nodes
> **Local repo on cluster:** `/home/team30/project30` (host `hadoop-01.uni.innopolis.ru`, user `team30`)
> **Remote repo (this URL):** [Dmitry057/big-data-big-final-group-case-study-project](https://github.com/Dmitry057/big-data-big-final-group-case-study-project)

---

## Team 30

| Role | Member | Responsibilities |
| --- | --- | --- |
| Data Engineer | **Ivan Ilyichev** | Manage databases, build the data pipeline, write automation scripts |
| ML Specialist | **Kirill Shumsky** | Prepare dataset for ML modelling, build distributed ML models, monitor models |
| Data Scientist | **Nikita Zagainov** | Understand the data, perform EDA, analyse features, build the dashboard |
| Tester / Tech Writer | **Dmitry Tetkin** | Document every stage, organise the report, document the repo, test artifacts |

---

## How the pipeline runs

A single entry point runs every stage of the pipeline; every stage is
idempotent and re-runnable:

```bash
bash main.sh
```

`main.sh` invokes the following stages in order:

| Stage | Script | What it does |
| --- | --- | --- |
| Pre | `scripts/preprocess.sh` | Optional environment prep |
| 1 | `scripts/stage1.sh` | Collect IoT-23 CSVs, build PostgreSQL DB, Sqoop → HDFS Parquet |
| 2 | `scripts/stage2.sh` | Build Hive warehouse, run 7 EDA queries on Tez, export results |
| 3 | `scripts/stage3.sh` | Spark ML data prep, train RF + LR with grid-search CV, evaluate |
| 4 | `scripts/stage4.sh` | Refresh / publish the Apache Superset dashboard |
| Post | `scripts/postprocess.sh` | Optional cleanup / artifact tidy-up |

The grader can also run any stage script in isolation — each one re-creates
its own intermediate state.

---

## Repository layout

```
project30/
├── main.sh                  # Single entry point — runs every stage end-to-end
├── requirements.txt         # Python dependencies for PySpark and helpers
├── README.MD                # This file
│
├── data/                    # IoT-23 CSV captures + train/test parquet (NOT in git, .gitignored)
├── secrets/                 # Hive / PostgreSQL passwords (NOT in git)
│
├── scripts/                 # Pipeline stages and helpers
│   ├── preprocess.sh
│   ├── stage1.sh            # Stage 1 — Ingestion
│   ├── data_collection.sh   #   ↳ pulls 12 IoT-23 captures via gdown
│   ├── build_projectdb.py   #   ↳ creates Citus tables, loads & types CSVs
│   ├── import_to_hdfs.sh    #   ↳ Sqoop import to HDFS as Parquet/Snappy
│   ├── stage2.sh            # Stage 2 — Hive warehouse + EDA via Beeline
│   ├── stage3.sh            # Stage 3 — Spark ML on YARN
│   ├── ml_data_preparation.py  #   ↳ feature pipeline + train/test split
│   ├── ml_models_training.py   #   ↳ Random Forest + Logistic Regression with CV
│   ├── ml_evaluation.py        #   ↳ Multiclass metrics on held-out set
│   ├── stage4.sh            # Stage 4 — Dashboard refresh
│   ├── benchmark_storage_*.{sh,py}  # Stage-1 storage-format benchmark
│   └── postprocess.sh
│
├── sql/                     # PostgreSQL DDL + Hive HQL
│   ├── create_tables.sql    # Postgres schema (captures, network_connections)
│   ├── import_data.sql      # COPY command for staging table
│   ├── test_database.sql    # 7 validation queries
│   ├── db.hql               # Hive warehouse setup (ext + bucketed + partitioned)
│   └── q1.hql … q7.hql      # 7 EDA queries materialised as q*_results
│
├── models/                  # Trained Spark ML models pulled from HDFS
│   ├── model1/              # Best Random Forest (numTrees=60, maxDepth=6)
│   └── model2/              # Best Multinomial Logistic Regression
│
├── output/                  # Reproducible artefacts the grader inspects
│   ├── q1.csv … q7.csv      # EDA query results (CSV exports)
│   ├── q1.jpg … q7.jpg      # EDA charts
│   ├── q1_hive.txt … q7_hive.txt   # Beeline run logs per query
│   ├── hive_results.txt     # Output of db.hql (table create + describe)
│   ├── stage1_storage_benchmark_*.csv   # Format/codec benchmark
│   ├── model1_predictions.csv          # RF predictions on test set
│   ├── model2_predictions.csv          # LR predictions on test set
│   ├── evaluation.csv                  # Final per-model metrics
│   └── sqoop_artifacts/                # Generated .avsc / .java files
│
├── notebooks/               # Exploratory Jupyter notebooks (ml.ipynb, …)
├── docs/                    # Course / stage methodology HTML exports
└── presentation/            # Final defence presentation (HTML + PDF)
    ├── index.html
    ├── presentation.pdf
    ├── print.css
    ├── fonts/               # Roobert + RoobertMono
    └── img/                 # Embedded EDA charts
```

---

## Stage 1 — Data Collection & Ingestion

**Source.** 12 hand-picked captures from the Aposemat IoT-23 corpus (Mirai,
Hide-and-Seek, Hakai, Torii, Trojan, Okiru, Kenjiro, plus benign baselines)
in Zeek `conn.log` format, multi-class labelled.

**Pipeline:**

1. `data_collection.sh` — `gdown` pulls 12 capture CSVs from a Google Drive folder into `data/`.
2. `build_projectdb.py` — connects to the team30 PostgreSQL DB on Citus
   (`hadoop-04.uni.innopolis.ru:5432`), recreates `captures` + `network_connections`,
   loads each CSV through a typed staging table.
3. `import_to_hdfs.sh` — Sqoop import of both tables to HDFS as Parquet + Snappy.

**Storage benchmark** (Stage-1 supplementary): four format/codec pairs were
benchmarked on write-time and full-scan time; **Parquet + Snappy** was chosen.

---

## Stage 2 — Storage & EDA

A single Hive warehouse `team30_projectdb` is built on top of the Sqoop output:

- **External Parquet tables** mirror the Sqoop layout.
- **`captures_bucketed`** — `CLUSTERED BY (capture_id) INTO 4 BUCKETS`.
- **`network_connections_part`** — `PARTITIONED BY (label)` so every
  EDA query and ML scan prunes to a single class partition.

7 EDA queries run on Tez; each materialises a `qN_results` Parquet table and
exports a CSV + a chart for the dashboard:

| # | Insight | Headline |
| --- | --- | --- |
| q1 | Label distribution | 35% Benign, 28% Malicious, 23% DDoS, 14% port-scan, 0.05% rare |
| q2 | Protocol per label | Almost all malicious traffic is TCP; UDP is benign DNS |
| q3 | Avg bytes per label | DDoS averages ~3.7 × 10⁸ orig bytes / flow |
| q4 | Per-capture maliciousness | Bimodal: 8 captures > 79% malicious, 4 captures < 11% |
| q5 | Conn_state per label | `S0` (SYN-only) dominates almost every class |
| q6 | Top targeted ports | Port 23 (Telnet) is the single most attacked destination |
| q7 | Avg packets per label | C&C sessions ≈ 500 pkts out vs. 4 for scan / 3.9 for benign |

---

## Stage 3 — Predictive Analytics

`scripts/ml_data_preparation.py` builds a single PySpark `Pipeline`:

1. `CyclicalTimeTransformer` — sin/cos for month, day, hour, minute, second.
2. Imputation for nulls, drop of constant `tunnel_parents`.
3. `StringIndexer + OneHotEncoder` for `proto`, `service`, `conn_state`, `history`.
4. `ChiSqSelector(fpr=0.1)` — keeps only label-correlated dummies.
5. `VectorAssembler` + `StandardScaler` → final `features` column.

Train/test split: 60% / 40%, seed 42.

**Models** (`scripts/ml_models_training.py`): both fit with
`CrossValidator(numFolds=2, parallelism=3)` optimising multiclass `f1` on
YARN with 6 executors × 3 cores × 6 GB.

| Model | Grid | Best |
| --- | --- | --- |
| Random Forest | `numTrees ∈ {30, 60} × maxDepth ∈ {4, 6}` | `numTrees=60, maxDepth=6` |
| Logistic Regression (multinomial) | `regParam ∈ {0.01, 0.1} × elasticNet ∈ {0.0, 0.5}` | `regParam=0.01, elasticNet=0.0` |

**Evaluation** (`scripts/ml_evaluation.py`, `output/evaluation.csv`):

| Model | Accuracy | Precision | Recall | F1 |
| --- | ---: | ---: | ---: | ---: |
| **Random Forest** | **0.9979** | **0.9979** | **0.9979** | **0.9978** |
| Logistic Regression | 0.3510 | 0.3395 | 0.3510 | 0.1824 |

---

## Stage 4 — Dashboard

A single Apache Superset dashboard, fed by Hive 2 over Beeline
(`jdbc:hive2://hadoop-03.uni.innopolis.ru:10001`). Sections:

- Data characteristics — 25M rows, 23 features, 7 classes, 12 captures.
- Class balance (q1).
- Behaviour signatures (q2, q3, q5, q7).
- Top targets (q6).
- Capture strata (q4).
- Model performance (`evaluation.csv`).

The dashboard is **public** for grading purposes.

---

## Presentation

The defence presentation is committed to this repo as both source and PDF:

- `presentation/index.html` — the slide source (16:9, 1600×900, Roobert).
- `presentation/presentation.pdf` — the rendered PDF.
- `presentation/print.css` — print rules so 1 slide = 1 page.

To re-render the PDF after editing `index.html`:

```bash
"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
    --headless --disable-gpu \
    --no-pdf-header-footer --print-to-pdf-no-header --no-margins \
    --print-to-pdf=presentation/presentation.pdf \
    "file://$PWD/presentation/index.html"
```

---

## What is *not* in this repo (and why)

| Path | Reason |
| --- | --- |
| `data/` | Raw IoT-23 CSVs (~737 MB) — kept on the cluster only, per project notes. |
| `secrets/` | Hive / Postgres passwords. |
| `.venv/` | Local Python virtualenv. |

All three are listed in `.gitignore`. The TA can re-create `data/` by running
`bash scripts/data_collection.sh` (which calls `gdown` on the original
Google Drive folder) and `secrets/` by placing the team's password files at
`secrets/.psql.pass` and `secrets/.hive.pass`.
