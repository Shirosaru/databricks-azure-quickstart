# 🚀 Azure Databricks Quick-Start Tutorial Guide

A beginner-friendly guide for getting started with Azure Databricks — covering notebooks, data queries, ETL pipelines, ML model training and deployment.

> **Source:** [Microsoft Learn — Azure Databricks Getting Started](https://learn.microsoft.com/en-us/azure/databricks/getting-started/)

---

## Prerequisites

- An active **Azure Databricks workspace** (with dashboard access)
- Permissions to create **Compute** resources
- **Unity Catalog** enabled (for Tutorials 1 & 3)
- Cluster running **Databricks Runtime 17.3 LTS ML** or above (for Tutorial 3)

> Don't have an account? [Sign up for a free trial](https://learn.microsoft.com/en-us/azure/databricks/getting-started/free-trial)

---

## ✅ Step 0 — Orient Yourself First!

When you log into your Databricks workspace, look at the **left sidebar**. Key sections:

| Section | What it's for |
|---|---|
| **Workspace** | Your notebooks and files |
| **Compute** | Virtual machines/clusters that run your code |
| **Data** | Tables, Unity Catalog, and datasets |
| **Workflows / Jobs** | Scheduled pipelines and automation |
| **Experiments** | MLflow experiment tracking |
| **Models** | Registered ML models (Unity Catalog) |
| **Serving** | Deploy models as REST API endpoints |

> ⚠️ **Important:** You need an active **Compute cluster** before running any code. Without one, nothing will execute!  
> ⚠️ **First run is slow** — clusters take ~2–5 minutes to spin up. This is normal.

---

## 🟢 Tutorial 1 — Query & Visualize Data (~5 min)

> Uses built-in NYC Taxi sample data — no data upload needed!

### Step 1: Create a Notebook

1. Click **New** in the left sidebar
2. Click **Notebook**
3. A blank notebook opens — you're ready to code!

### Step 2: Query Sample Data

Paste one of the following into the first cell and press **Shift+Enter** to run it:

**SQL**
```sql
SELECT * FROM samples.nyctaxi.trips
```

**Python**
```python
display(spark.read.table("samples.nyctaxi.trips"))
```

**Scala**
```scala
display(spark.read.table("samples.nyctaxi.trips"))
```

**R**
```r
library(SparkR)
display(sql("SELECT * FROM samples.nyctaxi.trips"))
```

> 👀 **What to look for:** A table of rows appears below the cell showing taxi trip data — columns like `fare_amount`, `trip_distance`, `pickup_zip`, etc.

### Step 3: Create a Visualization

1. Below the query results, next to the **Table** tab, click **+** → **Visualization**
2. The Visualization Editor opens on the right
3. Set the following:
   - **Visualization Type:** `Bar`
   - **X column:** `fare_amount`
   - **Y column:** `trip_distance`
   - **Aggregation:** `Average`
   - **Group by:** `pickup_zip`
4. Click **Save**

> 👀 **What to look for:** A bar chart showing average trip distance by fare amount, grouped by pickup zip code.

---

## 🟡 Tutorial 2 — Build an ETL Pipeline (~15 min)

**ETL = Extract, Transform, Load** — the backbone of data engineering. This tutorial ingests JSON event data using Auto Loader and stores it in a Delta Lake table.

### Step 1: Create a Compute Resource

1. Click **Compute** in the sidebar
2. Click **Create Compute**
3. Give it a unique name (e.g., `my-cluster`)
4. Leave all other settings as default
5. Click **Create Compute** and wait for the green dot (Running)

> ⏳ Takes about 2–5 minutes. Grab a coffee!

### Step 2: Create a Notebook

1. Click **New → Notebook**
2. At the top of the notebook, click the **Connect** dropdown and select your cluster

### Step 3: Configure Auto Loader to Ingest Data

Auto Loader automatically picks up new files as they arrive in cloud storage — no manual polling needed.

Paste this into **Cell 1** and press **Shift+Enter**:

**Python**
```python
from pyspark.sql.functions import col, current_timestamp

# Define variables
file_path = "/databricks-datasets/structured-streaming/events"
username = spark.sql("SELECT regexp_replace(session_user(), '[^a-zA-Z0-9]', '_')").first()[0]
table_name = f"{username}_etl_quickstart"
checkpoint_path = f"/tmp/{username}/_checkpoint/etl_quickstart"

# Clear previous run data (safe to re-run)
spark.sql(f"DROP TABLE IF EXISTS {table_name}")
dbutils.fs.rm(checkpoint_path, True)

# Configure Auto Loader to ingest JSON data into a Delta table
(spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "json")
  .option("cloudFiles.schemaLocation", checkpoint_path)
  .load(file_path)
  .select("*", col("_metadata.file_path").alias("source_file"), current_timestamp().alias("processing_time"))
  .writeStream
  .option("checkpointLocation", checkpoint_path)
  .trigger(availableNow=True)
  .toTable(table_name))
```

**Scala**
```scala
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.streaming.Trigger
import spark.implicits._

val file_path = "/databricks-datasets/structured-streaming/events"
val username = spark.sql("SELECT regexp_replace(session_user(), '[^a-zA-Z0-9]', '_')").first.get(0)
val table_name = s"${username}_etl_quickstart"
val checkpoint_path = s"/tmp/${username}/_checkpoint"

spark.sql(s"DROP TABLE IF EXISTS ${table_name}")
dbutils.fs.rm(checkpoint_path, true)

spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "json")
  .option("cloudFiles.schemaLocation", checkpoint_path)
  .load(file_path)
  .select($"*", $"_metadata.file_path".as("source_file"), current_timestamp.as("processing_time"))
  .writeStream
  .option("checkpointLocation", checkpoint_path)
  .trigger(Trigger.AvailableNow)
  .toTable(table_name)
```

> 👀 **What to look for:** The cell runs and shows a streaming progress bar. Once complete, it shows `Stopped` — data has been loaded.

### Step 4: Query and Preview the Data

In **Cell 2**, paste and run:

```python
df = spark.read.table(table_name)
display(df)
```

> 👀 **What to look for:** A table of JSON event rows with two extra columns: `source_file` (where the file came from) and `processing_time` (when it was ingested).

### Step 5: Schedule as a Job

1. Click **Schedule** in the top-right corner of the notebook
2. Enter a unique **Job name**
3. Select **Manual** as the trigger
4. Pick your compute resource from the dropdown
5. Click **Create**
6. In the window that pops up, click **Run now**
7. Click the link next to **Last run** to see the job run results

> 👀 **What to look for:** A green checkmark next to the run = success!

---

## 🔴 Tutorial 3 — AI/ML Model Training & Management (~30 min)

> **Goal:** Train a wine quality classifier, tune it with hyperparameter optimization, register it in Unity Catalog, and deploy it as a live endpoint.
>
> **What you're learning:** scikit-learn + MLflow + Optuna + Model Serving — the full ML workflow on Databricks.

### Requirements for this tutorial

- Cluster running **Databricks Runtime 17.3 LTS ML** or above
- `USE_CATALOG` privilege on the `main` catalog
- `USE_SCHEMA`, `CREATE_TABLE`, and `CREATE_MODEL` privileges on the `default` schema

### Setup — Configure MLflow & Load Data

**Cell 1 — Configure MLflow to use Unity Catalog:**

```python
import mlflow

# Tell MLflow to store models in Unity Catalog (not the legacy workspace registry)
mlflow.set_registry_uri("databricks-uc")
```

> 👀 **What to look for:** No output — just confirm the cell runs without errors.

**Cell 2 — Set your catalog and schema:**

```python
# Change these if you're using a different catalog/schema
CATALOG_NAME = "main"
SCHEMA_NAME = "default"
```

**Cell 3 — Load the wine dataset and save to Unity Catalog:**

```python
# Read wine quality CSVs (built into Databricks datasets)
white_wine = spark.read.csv("/databricks-datasets/wine-quality/winequality-white.csv", sep=';', header=True)
red_wine = spark.read.csv("/databricks-datasets/wine-quality/winequality-red.csv", sep=';', header=True)

# Clean up column names (remove spaces)
for c in white_wine.columns:
    white_wine = white_wine.withColumnRenamed(c, c.replace(" ", "_"))
for c in red_wine.columns:
    red_wine = red_wine.withColumnRenamed(c, c.replace(" ", "_"))

# Save to Unity Catalog tables
spark.sql(f"DROP TABLE IF EXISTS {CATALOG_NAME}.{SCHEMA_NAME}.white_wine")
spark.sql(f"DROP TABLE IF EXISTS {CATALOG_NAME}.{SCHEMA_NAME}.red_wine")
white_wine.write.saveAsTable(f"{CATALOG_NAME}.{SCHEMA_NAME}.white_wine")
red_wine.write.saveAsTable(f"{CATALOG_NAME}.{SCHEMA_NAME}.red_wine")

print("Data saved to Unity Catalog ✅")
```

> 👀 **What to look for:** `Data saved to Unity Catalog ✅` — two tables now exist in your catalog under **Data** in the sidebar.

**Cell 4 — Preprocess the data:**

```python
import numpy as np
import pandas as pd
import sklearn.datasets
import sklearn.metrics
import sklearn.model_selection
import sklearn.ensemble
import matplotlib.pyplot as plt
import optuna
from mlflow.optuna.storage import MlflowStorage
from mlflow.pyspark.optuna.study import MlflowSparkStudy

# Load from Unity Catalog into Pandas
white_wine = spark.read.table(f"{CATALOG_NAME}.{SCHEMA_NAME}.white_wine").toPandas()
red_wine = spark.read.table(f"{CATALOG_NAME}.{SCHEMA_NAME}.red_wine").toPandas()

# Tag red vs white
white_wine['is_red'] = 0.0
red_wine['is_red'] = 1.0
data_df = pd.concat([white_wine, red_wine], axis=0)

# Label: 1 = high quality (score >= 7), 0 = not high quality
data_labels = data_df['quality'].astype('int') >= 7
data_df = data_df.drop(['quality'], axis=1)

# 80/20 train-test split
X_train, X_test, y_train, y_test = sklearn.model_selection.train_test_split(
    data_df, data_labels, test_size=0.2, random_state=1
)

print(f"Training samples: {len(X_train)}, Test samples: {len(X_test)}")
```

> 👀 **What to look for:** Training/test counts printed. The `quality` column is gone — replaced by a binary label (`True`/`False` = high quality or not).

---

### Part 1 — Train a Baseline Model with MLflow Tracking

**Cell 5 — Enable MLflow autologging and train:**

```python
# Autolog automatically captures: model params, metrics, the model artifact itself
mlflow.sklearn.autolog()

with mlflow.start_run(run_name='gradient_boost') as run:
    model = sklearn.ensemble.GradientBoostingClassifier(random_state=0)
    model.fit(X_train, y_train)

    # Calculate AUC (area under ROC curve) — higher is better, max = 1.0
    predicted_probs = model.predict_proba(X_test)
    roc_auc = sklearn.metrics.roc_auc_score(y_test, predicted_probs[:,1])

    # Plot and save the ROC curve
    roc_curve = sklearn.metrics.RocCurveDisplay.from_estimator(model, X_test, y_test)
    roc_curve.figure_.savefig("roc_curve.png")

    # Log additional metric and artifact manually
    mlflow.log_metric("test_auc", roc_auc)
    mlflow.log_artifact("roc_curve.png")

    print(f"✅ Baseline model Test AUC: {roc_auc:.4f}")
```

> 👀 **What to look for:**
> - AUC score printed (expect ~0.88–0.92 for a decent baseline)
> - Click the **flask icon (Experiments)** in the top-right of the notebook to see your run logged in MLflow
> - You'll see parameters (like `n_estimators`, `learning_rate`), metrics (`test_auc`), and the ROC curve image all auto-saved

**Cell 6 — Load and verify the model from MLflow:**

```python
# Load the model back from MLflow — this is how you'd use it in another notebook or job
model_loaded = mlflow.pyfunc.load_model(
    'runs:/{run_id}/model'.format(run_id=run.info.run_id)
)

predictions_loaded = model_loaded.predict(X_test)
predictions_original = model.predict(X_test)

# Sanity check — loaded model should give identical predictions
assert(np.array_equal(predictions_loaded, predictions_original))
print("✅ Model loaded from MLflow and verified!")
```

> 👀 **What to look for:** `✅ Model loaded from MLflow and verified!` — this confirms the model was saved and can be retrieved.

---

### Part 2 — Hyperparameter Tuning with Optuna

Instead of guessing the best settings, Optuna automatically searches for the best combination of hyperparameters across 32 trials.

**Cell 7 — Define the tuning objective:**

```python
def objective(trial):
    mlflow.sklearn.autolog()
    with mlflow.start_run(nested=True):
        # Optuna suggests different values for each trial
        params = {
            'n_estimators': trial.suggest_int('n_estimators', 20, 1000),
            'learning_rate': trial.suggest_float('learning_rate', 0.05, 1.0, log=True),
            'max_depth': trial.suggest_int('max_depth', 2, 5),
        }
        model_hp = sklearn.ensemble.GradientBoostingClassifier(random_state=0, **params)
        model_hp.fit(X_train, y_train)
        predicted_probs = model_hp.predict_proba(X_test)
        roc_auc = sklearn.metrics.roc_auc_score(y_test, predicted_probs[:,1])
        mlflow.log_metric('test_auc', roc_auc)
        return -roc_auc  # Optuna minimizes, so negate AUC
```

**Cell 8 — Run 32 parallel trials:**

```python
with mlflow.start_run(run_name='gb_optuna') as run:
    experiment_id = mlflow.active_run().info.experiment_id
    mlflow_storage = MlflowStorage(experiment_id=experiment_id)

    # MlflowSparkStudy uses Spark workers to run trials in parallel
    mlflow_study = MlflowSparkStudy(
        study_name="gb-optuna-tuning",
        storage=mlflow_storage,
    )
    mlflow_study.optimize(objective, n_trials=32, n_jobs=4)

print("✅ Hyperparameter tuning complete!")
```

> ⏳ This runs 32 trials across 4 workers — takes a few minutes.  
> 👀 **What to look for:** In the Experiments panel, you'll see 32 child runs nested under `gb_optuna`. Each shows different params and its AUC score.

**Cell 9 — Find the best model:**

```python
# Sort all runs by AUC descending to find the winner
best_run = mlflow.search_runs(
    order_by=['metrics.test_auc DESC', 'start_time DESC'],
    max_results=10,
).iloc[0]

print("🏆 Best Run Results:")
print(f"  AUC:           {best_run['metrics.test_auc']:.4f}")
print(f"  n_estimators:  {best_run['params.n_estimators']}")
print(f"  max_depth:     {best_run['params.max_depth']}")
print(f"  learning_rate: {best_run['params.learning_rate']}")

# Load the best model
best_model_pyfunc = mlflow.pyfunc.load_model(
    'runs:/{run_id}/model'.format(run_id=best_run.run_id)
)

# Generate predictions with the best model
best_model_predictions = X_test.copy()
best_model_predictions["prediction"] = best_model_pyfunc.predict(X_test)
display(best_model_predictions.head(10))
```

> 👀 **What to look for:** Best AUC should be noticeably higher than your baseline (~0.90+). The printed params show the winning hyperparameter combo.

---

### Part 3 — Save Results & Register Model in Unity Catalog

**Cell 10 — Save predictions table:**

```python
predictions_table = f"{CATALOG_NAME}.{SCHEMA_NAME}.predictions"
spark.sql(f"DROP TABLE IF EXISTS {predictions_table}")

results = spark.createDataFrame(best_model_predictions)
results.write.saveAsTable(predictions_table)

print(f"✅ Predictions saved to {predictions_table}")
```

**Cell 11 — Register the model:**

```python
model_uri = 'runs:/{run_id}/model'.format(run_id=best_run.run_id)

registered_model = mlflow.register_model(
    model_uri,
    f"{CATALOG_NAME}.{SCHEMA_NAME}.wine_quality_model"
)

print(f"✅ Model registered: {registered_model.name} version {registered_model.version}")
```

> 👀 **What to look for:**
> - Go to **Data → main → default** in the sidebar and you'll see `wine_quality_model` listed as a registered model
> - You can click it to see version history, lineage, and metadata

---

### Part 4 — Deploy the Model as a REST Endpoint

**This is the "management" part — your model is now live and callable via API.**

1. Click **Serving** in the left sidebar
2. Click **Create serving endpoint**
3. Fill in the form:
   - **Name:** `wine-quality-endpoint` (or any name)
   - **Entity:** Click the field → Select **My models - Unity Catalog** → choose `wine_quality_model`
   - **Version:** Pick the latest version
   - **Traffic:** Set to `100%`
   - **Compute type:** `CPU`
   - **Scale-out size:** `Small`
4. Click **Create**

> ⏳ The endpoint status shows **Not Ready** for a few minutes while it initializes.

> 👀 **What to look for:** Once the status turns **Ready** (green), click **Use** to send a test inference request. You'll get a live API response with wine quality predictions!

---

## ⚠️ Common Pitfalls & What to Watch Out For

| Issue | What to do |
|---|---|
| "No cluster attached" | Go to **Compute → Create Compute** first |
| Notebook won't run | Make sure cluster shows a **green dot** (Running) |
| Permission errors | Ask Azure admin — Unity Catalog `CREATE_MODEL` privileges needed |
| Slow first run | Cluster startup takes ~2–5 min — totally normal |
| Cell stuck running | Click **Interrupt** and check cluster status |
| `table not found` error | Make sure Unity Catalog is enabled in your workspace |
| MLflow runs not appearing | Click the 🔄 refresh icon on the Experiments panel |
| Optuna tuning is slow | Reduce `n_trials` from 32 to 8 for a faster test run |
| Serving endpoint stuck "Not Ready" | Wait 5+ minutes; check cluster logs if still stuck |

---

## 🎯 Recommended Learning Path

1. ✅ **Tutorial 1** — Query & visualize (get comfortable with notebooks)
2. ✅ **Tutorial 2** — ETL pipeline with Auto Loader + Delta Lake
3. ✅ **Tutorial 3** — Train, tune, register, and deploy an ML model
4. 🔜 **Workflows** — Schedule notebooks as automated jobs
5. 🔜 **Lakeflow Declarative Pipelines** — Declarative ETL with less code

---

## 🧠 Key Concepts Glossary

| Term | What it means |
|---|---|
| **Notebook** | Interactive code editor (like Jupyter) built into Databricks |
| **Compute / Cluster** | Virtual machines that execute your code |
| **Delta Lake** | Databricks' default storage format — supports ACID transactions |
| **Auto Loader** | Automatically ingests new files from cloud storage |
| **Unity Catalog** | Centralized data governance — manages tables, models, permissions |
| **MLflow** | Open-source tool for tracking ML experiments, models, and deployments |
| **Optuna** | Hyperparameter tuning library — finds the best model settings automatically |
| **AUC** | Area Under the Curve — ML metric for classification quality (0.5 = random, 1.0 = perfect) |
| **Model Serving** | Deploy a registered model as a live REST API endpoint |

---

## 📚 Further Reading

- [Azure Databricks Docs](https://learn.microsoft.com/en-us/azure/databricks/)
- [What is Auto Loader?](https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/)
- [Delta Lake Overview](https://learn.microsoft.com/en-us/azure/databricks/delta/)
- [Unity Catalog Getting Started](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/get-started)
- [MLflow on Databricks](https://learn.microsoft.com/en-us/azure/databricks/mlflow/)
- [Hyperparameter Tuning with Optuna](https://learn.microsoft.com/en-us/azure/databricks/machine-learning/automl-hyperparam-tuning/optuna)
- [Model Serving Endpoints](https://learn.microsoft.com/en-us/azure/databricks/machine-learning/model-serving/create-manage-serving-endpoints)
- [Databricks Community Forum](https://community.databricks.com)
