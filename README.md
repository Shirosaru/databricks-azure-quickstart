# 🚀 Azure Databricks Quick-Start Tutorial Guide

A beginner-friendly guide for getting started with Azure Databricks — covering notebooks, data queries, ETL pipelines, and common pitfalls.

> **Source:** [Microsoft Learn — Azure Databricks Getting Started](https://learn.microsoft.com/en-us/azure/databricks/getting-started/)

---

## Prerequisites

- An active **Azure Databricks workspace** (with dashboard access)
- Permissions to create **Compute** resources
- **Unity Catalog** enabled (for Tutorial 1)

> Don't have an account? [Sign up for a free trial](https://learn.microsoft.com/en-us/azure/databricks/getting-started/free-trial)

---

## ✅ Step 0 — Orient Yourself

When you log into your Databricks workspace, look at the **left sidebar**. Key sections:

| Section | What it's for |
|---|---|
| **Workspace** | Your notebooks and files |
| **Compute** | Virtual machines/clusters that run your code |
| **Data** | Tables and Unity Catalog |
| **Workflows / Jobs** | Scheduled pipelines and automation |

> ⚠️ **Important:** You need an active **Compute cluster** before running any code. Without one, nothing will execute!

---

## 🟢 Tutorial 1 — Query & Visualize Data (~5 min)

> Uses built-in NYC Taxi sample data — no data upload needed!

### Step 1: Create a Notebook

Click **New → Notebook** in the sidebar. A blank notebook opens.

### Step 2: Query Sample Data

Paste one of the following into the first cell and press **Shift+Enter**:

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

### Step 3: Create a Visualization

1. Next to the **Table** tab, click **+** → **Visualization**
2. Set **Visualization Type** to `Bar`
3. Set **X column** to `fare_amount`
4. Set **Y column** to `trip_distance`
5. Set **Aggregation** to `Average`
6. Set **Group by** to `pickup_zip`
7. Click **Save**

---

## 🟡 Tutorial 2 — Build an ETL Pipeline (~15 min)

### Step 1: Create a Compute Resource

1. Click **Compute** in the sidebar
2. Click **Create Compute**
3. Give it a unique name, leave defaults, click **Create Compute**

### Step 2: Create a Notebook

Click **New → Notebook** and attach it to the cluster you just created.

### Step 3: Configure Auto Loader (Ingest Data to Delta Lake)

Paste the following into a notebook cell and press **Shift+Enter**:

**Python**
```python
from pyspark.sql.functions import col, current_timestamp

# Define variables
file_path = "/databricks-datasets/structured-streaming/events"
username = spark.sql("SELECT regexp_replace(session_user(), '[^a-zA-Z0-9]', '_')").first()[0]
table_name = f"{username}_etl_quickstart"
checkpoint_path = f"/tmp/{username}/_checkpoint/etl_quickstart"

# Clear previous run data
spark.sql(f"DROP TABLE IF EXISTS {table_name}")
dbutils.fs.rm(checkpoint_path, True)

# Configure Auto Loader to ingest JSON data to a Delta table
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

### Step 4: Query and Preview the Data

```python
# Read the table
df = spark.read.table(table_name)

# Display the data
display(df)
```

### Step 5: Schedule as a Job

1. Click **Schedule** in the top-right of the notebook
2. Enter a unique **Job name**
3. Select **Manual** trigger
4. Select your compute resource
5. Click **Create**
6. Click **Run now** to test it

---

## ⚠️ Common Pitfalls & What to Watch Out For

| Issue | What to do |
|---|---|
| "No cluster attached" | Go to **Compute → Create Compute** first |
| Notebook won't run | Make sure your cluster shows a **green dot** (Running) |
| Permission errors | Ask your Azure admin — Unity Catalog permissions may be missing |
| Slow first run | Cluster startup takes ~2–5 min — this is normal |
| Cell stuck running | Click **Interrupt** and check cluster status |
| `table not found` error | Make sure Unity Catalog is enabled in your workspace |

---

## 🎯 Recommended Learning Path

1. ✅ **Tutorial 1** — Query & visualize (get comfortable with notebooks)
2. ✅ **Tutorial 2** — Build an ETL pipeline (real data engineering workflow)
3. 🔜 **Workflows** — Schedule and automate your notebooks as jobs
4. 🔜 **ML Tutorial** — Train and deploy a model with scikit-learn + MLflow

---

## 📚 Further Reading

- [Azure Databricks Docs](https://learn.microsoft.com/en-us/azure/databricks/)
- [What is Auto Loader?](https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/)
- [Delta Lake Overview](https://learn.microsoft.com/en-us/azure/databricks/delta/)
- [Unity Catalog Getting Started](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/get-started)
- [Databricks Community Forum](https://community.databricks.com)
