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

> **Goal:** Train a classifier, tune it with hyperparameter optimization, register it in Unity Catalog, and deploy it as a live endpoint.
>
> **What you're learning:** scikit-learn + MLflow + Optuna + Model Serving — the full ML workflow on Databricks.

### 🧪 Choose Your Dataset

You have **two options** — pick whichever fits your setup:

| Option | Dataset | Requires Unity Catalog? | Best for |
|---|---|---|---|
| **Option A** | Built-in wine quality CSV (Databricks datasets) | ✅ Yes | If you have Unity Catalog enabled |
| **Option B ⭐ Recommended for beginners** | Synthetic data generated in-memory | ❌ No | Zero setup, works anywhere, no permissions needed |

---

### ⭐ Option B — Synthetic Dataset (Zero Setup, Works Anywhere)

> This is the fastest way to get the full ML pipeline running. No files, no catalog, no permissions needed.

#### Setup — Requirements & Imports

**Cell 1 — Imports and MLflow setup:**

```python
import mlflow
import numpy as np
import pandas as pd
import sklearn.datasets
import sklearn.metrics
import sklearn.model_selection
import sklearn.ensemble

# Use the standard Databricks workspace model registry (no Unity Catalog needed)
mlflow.set_registry_uri("databricks")
```

> 👀 **What to look for:** No errors. All libraries are pre-installed on Databricks ML Runtime.

#### Generate a Synthetic Dataset

**Cell 2 — Create fake data with scikit-learn:**

```python
# Generate a synthetic binary classification dataset — 1000 samples, 10 features
X, y = sklearn.datasets.make_classification(
    n_samples=1000,      # 1000 rows of data
    n_features=10,       # 10 input features (e.g., "sensor_1" through "sensor_10")
    n_informative=6,     # 6 features actually matter for the prediction
    n_redundant=2,       # 2 features are noisy copies of informative ones
    random_state=42
)

# Name the columns so they're readable
feature_names = [f"feature_{i}" for i in range(1, 11)]
X_df = pd.DataFrame(X, columns=feature_names)
y_series = pd.Series(y, name="label")  # label = 0 or 1

# 80/20 train-test split
X_train, X_test, y_train, y_test = sklearn.model_selection.train_test_split(
    X_df, y_series, test_size=0.2, random_state=42
)

print(f"✅ Dataset ready!")
print(f"   Training samples : {len(X_train)}")
print(f"   Test samples     : {len(X_test)}")
print(f"   Features         : {list(X_train.columns)}")
print(f"   Label balance    : {y_train.value_counts().to_dict()}")
```

> 👀 **What to look for:**
> - 800 training, 200 test samples
> - Features named `feature_1` through `feature_10`
> - Label balance close to 50/50 (it's designed to be balanced)

#### Part 1 — Train a Baseline Model

**Cell 3 — Train with MLflow autologging:**

```python
mlflow.sklearn.autolog()

with mlflow.start_run(run_name="synthetic_baseline") as run:
    model = sklearn.ensemble.GradientBoostingClassifier(random_state=42)
    model.fit(X_train, y_train)

    predicted_probs = model.predict_proba(X_test)
    roc_auc = sklearn.metrics.roc_auc_score(y_test, predicted_probs[:, 1])

    # Plot and save the ROC curve
    roc_display = sklearn.metrics.RocCurveDisplay.from_estimator(model, X_test, y_test)
    roc_display.figure_.savefig("roc_curve.png")

    mlflow.log_metric("test_auc", roc_auc)
    mlflow.log_artifact("roc_curve.png")

    print(f"✅ Baseline Test AUC: {roc_auc:.4f}")
    print(f"   Run ID: {run.info.run_id}")
```

> 👀 **What to look for:**
> - AUC score around 0.92–0.96 (synthetic data is cleaner than real data)
> - Click the **Experiments** flask icon (top-right) to see this run tracked automatically
> - MLflow auto-captured: model parameters, training metrics, the model file itself

#### Part 2 — Hyperparameter Tuning

**Cell 4 — Define the Optuna objective:**

```python
import optuna

def objective(trial):
    mlflow.sklearn.autolog()
    with mlflow.start_run(nested=True):
        params = {
            "n_estimators":  trial.suggest_int("n_estimators", 50, 500),
            "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.5, log=True),
            "max_depth":     trial.suggest_int("max_depth", 2, 6),
        }
        clf = sklearn.ensemble.GradientBoostingClassifier(random_state=42, **params)
        clf.fit(X_train, y_train)
        probs = clf.predict_proba(X_test)
        auc = sklearn.metrics.roc_auc_score(y_test, probs[:, 1])
        mlflow.log_metric("test_auc", auc)
        return -auc  # Optuna minimizes — negate to maximize AUC
```

**Cell 5 — Run 20 tuning trials (fast for beginners):**

```python
with mlflow.start_run(run_name="synthetic_optuna_tuning") as parent_run:
    study = optuna.create_study(direction="minimize")
    study.optimize(objective, n_trials=20)

print(f"✅ Tuning complete! Best AUC: {-study.best_value:.4f}")
print(f"   Best params: {study.best_params}")
```

> 👀 **What to look for:**
> - 20 child runs appear nested under `synthetic_optuna_tuning` in the Experiments panel
> - Each run has different params — Optuna is automatically searching the space
> - Best AUC should beat your baseline

**Cell 6 — Retrieve the best run from MLflow:**

```python
best_run = mlflow.search_runs(
    order_by=["metrics.test_auc DESC", "start_time DESC"],
    max_results=10
).iloc[0]

print("🏆 Best Tuned Model:")
print(f"   AUC:           {best_run['metrics.test_auc']:.4f}")
print(f"   n_estimators:  {best_run['params.n_estimators']}")
print(f"   max_depth:     {best_run['params.max_depth']}")
print(f"   learning_rate: {best_run['params.learning_rate']}")

best_model = mlflow.pyfunc.load_model(f"runs:/{best_run.run_id}/model")
best_preds = X_test.copy()
best_preds["prediction"] = best_model.predict(X_test)
display(best_preds.head(10))
```

#### Part 3 — Register the Model

**Cell 7 — Register in the workspace model registry:**

```python
model_uri = f"runs:/{best_run.run_id}/model"
model_name = "synthetic_classifier"

registered = mlflow.register_model(model_uri, model_name)
print(f"✅ Registered: {registered.name}  version {registered.version}")
```

> 👀 **What to look for:**
> - Go to **Models** in the left sidebar — you'll see `synthetic_classifier` listed
> - Click it to see version history and the model artifact

#### Part 4 — Deploy the Model as a REST Endpoint

1. Click **Serving** in the left sidebar
2. Click **Create serving endpoint**
3. Fill in:
   - **Name:** `synthetic-classifier-endpoint`
   - **Entity:** Select `synthetic_classifier` from your models
   - **Version:** Latest
   - **Traffic:** `100%`
   - **Compute type:** `CPU`
   - **Scale-out:** `Small`
4. Click **Create**

> ⏳ Wait ~5 min for status to go from **Not Ready** → **Ready** (green)

5. Once ready, click **Use** and paste this test payload:

```json
{
  "inputs": {
    "feature_1":  [0.5],
    "feature_2":  [-1.2],
    "feature_3":  [0.8],
    "feature_4":  [0.1],
    "feature_5":  [-0.3],
    "feature_6":  [1.1],
    "feature_7":  [0.0],
    "feature_8":  [-0.7],
    "feature_9":  [0.4],
    "feature_10": [0.9]
  }
}
```

> 👀 **What to look for:** A live JSON response with `predictions: [0]` or `predictions: [1]` — your model is now a live API! 🎉

---

### Option A — Built-in Wine Quality Dataset (Requires Unity Catalog)

> Use this if your workspace has Unity Catalog enabled with `CREATE_MODEL` privileges on `main.default`.

#### Setup

**Cell 1 — Configure MLflow for Unity Catalog:**

```python
import mlflow
mlflow.set_registry_uri("databricks-uc")

CATALOG_NAME = "main"
SCHEMA_NAME  = "default"
```

**Cell 2 — Load and save wine data to Unity Catalog:**

```python
white_wine = spark.read.csv("/databricks-datasets/wine-quality/winequality-white.csv", sep=';', header=True)
red_wine   = spark.read.csv("/databricks-datasets/wine-quality/winequality-red.csv",   sep=';', header=True)

for c in white_wine.columns:
    white_wine = white_wine.withColumnRenamed(c, c.replace(" ", "_"))
for c in red_wine.columns:
    red_wine = red_wine.withColumnRenamed(c, c.replace(" ", "_"))

spark.sql(f"DROP TABLE IF EXISTS {CATALOG_NAME}.{SCHEMA_NAME}.white_wine")
spark.sql(f"DROP TABLE IF EXISTS {CATALOG_NAME}.{SCHEMA_NAME}.red_wine")
white_wine.write.saveAsTable(f"{CATALOG_NAME}.{SCHEMA_NAME}.white_wine")
red_wine.write.saveAsTable(f"{CATALOG_NAME}.{SCHEMA_NAME}.red_wine")
print("✅ Wine data saved to Unity Catalog!")
```

**Cell 3 — Preprocess:**

```python
import numpy as np
import pandas as pd
import sklearn.datasets, sklearn.metrics, sklearn.model_selection, sklearn.ensemble
import matplotlib.pyplot as plt
import optuna
from mlflow.optuna.storage import MlflowStorage
from mlflow.pyspark.optuna.study import MlflowSparkStudy

white_wine = spark.read.table(f"{CATALOG_NAME}.{SCHEMA_NAME}.white_wine").toPandas()
red_wine   = spark.read.table(f"{CATALOG_NAME}.{SCHEMA_NAME}.red_wine").toPandas()

white_wine['is_red'] = 0.0
red_wine['is_red']   = 1.0
data_df = pd.concat([white_wine, red_wine], axis=0)

data_labels = data_df['quality'].astype('int') >= 7
data_df = data_df.drop(['quality'], axis=1)

X_train, X_test, y_train, y_test = sklearn.model_selection.train_test_split(
    data_df, data_labels, test_size=0.2, random_state=1
)
print(f"Training: {len(X_train)}, Test: {len(X_test)}")
```

**Cell 4 — Train:**

```python
mlflow.sklearn.autolog()

with mlflow.start_run(run_name='gradient_boost') as run:
    model = sklearn.ensemble.GradientBoostingClassifier(random_state=0)
    model.fit(X_train, y_train)
    predicted_probs = model.predict_proba(X_test)
    roc_auc = sklearn.metrics.roc_auc_score(y_test, predicted_probs[:,1])
    roc_curve = sklearn.metrics.RocCurveDisplay.from_estimator(model, X_test, y_test)
    roc_curve.figure_.savefig("roc_curve.png")
    mlflow.log_metric("test_auc", roc_auc)
    mlflow.log_artifact("roc_curve.png")
    print(f"✅ Baseline AUC: {roc_auc:.4f}")
```

**Cell 5 — Tune with Optuna (32 trials, parallel):**

```python
def objective(trial):
    mlflow.sklearn.autolog()
    with mlflow.start_run(nested=True):
        params = {
            'n_estimators':  trial.suggest_int('n_estimators', 20, 1000),
            'learning_rate': trial.suggest_float('learning_rate', 0.05, 1.0, log=True),
            'max_depth':     trial.suggest_int('max_depth', 2, 5),
        }
        m = sklearn.ensemble.GradientBoostingClassifier(random_state=0, **params)
        m.fit(X_train, y_train)
        auc = sklearn.metrics.roc_auc_score(y_test, m.predict_proba(X_test)[:,1])
        mlflow.log_metric('test_auc', auc)
        return -auc

with mlflow.start_run(run_name='gb_optuna') as run:
    experiment_id = mlflow.active_run().info.experiment_id
    mlflow_storage = MlflowStorage(experiment_id=experiment_id)
    mlflow_study = MlflowSparkStudy(study_name="gb-optuna-tuning", storage=mlflow_storage)
    mlflow_study.optimize(objective, n_trials=32, n_jobs=4)

print("✅ Tuning complete!")
```

**Cell 6 — Register best model to Unity Catalog:**

```python
best_run = mlflow.search_runs(
    order_by=['metrics.test_auc DESC', 'start_time DESC'], max_results=10
).iloc[0]

print(f"🏆 Best AUC: {best_run['metrics.test_auc']:.4f}")

mlflow.register_model(
    f"runs:/{best_run.run_id}/model",
    f"{CATALOG_NAME}.{SCHEMA_NAME}.wine_quality_model"
)
print("✅ Model registered in Unity Catalog!")
```

**Deploy:** Follow the same Serving steps as Option B above, selecting `wine_quality_model` instead.

---

## ⚠️ Common Pitfalls & What to Watch Out For

| Issue | What to do |
|---|---|
| "No cluster attached" | Go to **Compute → Create Compute** first |
| Notebook won't run | Make sure cluster shows a **green dot** (Running) |
| Permission errors on Unity Catalog | Use **Option B** (synthetic) — no catalog permissions needed |
| Slow first run | Cluster startup takes ~2–5 min — totally normal |
| Cell stuck running | Click **Interrupt** and check cluster status |
| `table not found` error | Switch to Option B, or ask admin to enable Unity Catalog |
| MLflow runs not appearing | Click the 🔄 refresh icon on the Experiments panel |
| Optuna tuning is slow | Reduce `n_trials` to 8–10 for a faster test run |
| Serving endpoint stuck "Not Ready" | Wait 5+ minutes; check cluster logs if still stuck |
| Serving test payload errors | Make sure feature names in JSON match your training data exactly |

---

## 🎯 Recommended Learning Path

1. ✅ **Tutorial 1** — Query & visualize (get comfortable with notebooks)
2. ✅ **Tutorial 2** — ETL pipeline with Auto Loader + Delta Lake
3. ✅ **Tutorial 3 Option B** — Synthetic ML pipeline (fastest path to deployment)
4. ✅ **Tutorial 3 Option A** — Wine quality dataset (real data with Unity Catalog)
5. 🔜 **Workflows** — Schedule notebooks as automated jobs
6. 🔜 **Lakeflow Declarative Pipelines** — Declarative ETL with less code

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
| **AUC** | Area Under the Curve — ML metric for classification (0.5 = random, 1.0 = perfect) |
| **Model Serving** | Deploy a registered model as a live REST API endpoint |
| **Synthetic Dataset** | Fake data generated by code — great for testing pipelines without real data |

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
