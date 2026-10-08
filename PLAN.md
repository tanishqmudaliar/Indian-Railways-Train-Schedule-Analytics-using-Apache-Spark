# PROMPT: paste everything below this line into Antigravity (Claude Opus)

---

You are building a small but complete **Big Data Analytics (BDA) mini-project** for a final-year Computer Engineering student (Mumbai University). Build it **from scratch, end to end**: download the dataset, process it with Apache Spark, save the results, and show them on a small website. Run the code yourself and verify that it works. Do not just write files and stop.

## 1. Project

**Title:** Indian Railways Train Schedule Analytics using Apache Spark

**Idea:** Take the real Indian Railways schedule data from Kaggle (every train stop at every station), process it with PySpark as an ETL pipeline, save cleaned data and aggregated results as Parquet, and show the findings in a small Streamlit app.

The project has to look like a proper big data pipeline: **Extract → Transform → Load → Analyse → Visualise**.

## 2. Hard constraints

- **Environment:** Windows machine. Run commands inside **WSL2 (Ubuntu)**. If you are in PowerShell, run them with `wsl -e bash -lc "..."`. If WSL is not available, **stop and tell me**. Do not fall back to native-Windows Spark.
- **Everything stays inside the current project directory** (the folder that is open in this workspace). I must be able to see and track every file in my editor. **Do NOT create anything in the Linux home directory (`~`) or anywhere else outside the project folder.** That includes:
  - the virtual environment (`./.venv`)
  - downloaded data (`./data/raw/`)
  - the Kaggle / kagglehub cache: set `KAGGLEHUB_CACHE=./.cache/kagglehub` (and similar variables for any other tool) so downloads do not go to `~/.cache`
  - Spark scratch files: set `spark.local.dir` to `./.spark_tmp` and keep any `metastore_db`, `spark-warehouse` and logs inside the project
  - the pip cache: use `--cache-dir ./.cache/pip`
  - outputs, logs and results (`./data/processed/`, `./data/results/`, `./logs/`)
  - Use only **relative paths** from the project root in the code, so the project still works if the folder is moved.
  - Add a `.gitignore` for `.venv/`, `.cache/`, `.spark_tmp/`, `data/raw/` and `logs/`.
  - The only things allowed outside the project are system packages (for example Java via `apt`). Tell me when you install one.
  - The one exception is Kaggle credentials, which must stay wherever they already are (`~/.kaggle/kaggle.json`). Never copy them into the project and never commit them.
- **The project folder is probably on the Windows drive (for example under `/mnt/c/...` in WSL).** This is slower than the Linux filesystem, so keep Spark's shuffle partitions small (`spark.sql.shuffle.partitions` = 8), and don't worry about it for this data size. Tell me if you notice any real problems, such as permission or file-locking errors, and fix them without moving the project out of the folder.
- **Stack:** Python 3.10+, PySpark (local mode, `local[*]`), OpenJDK 11 or 17, Parquet, Streamlit, pandas, matplotlib or Altair. Nothing else. **No Docker, no Hadoop install, no cloud, no Airflow, no Kafka.**
- **Keep it small.** The website is one Streamlit file (`app.py`). No React, no databases, no extra servers.
- **The code must be simple and readable.** The student has to explain every line in a viva. Use short functions, clear names and a comment above every non-obvious step. No clever one-liners or heavy abstractions.
- **No fake data. Never invent numbers.** Every number shown on the website must come from the real run. If something in the data does not support an analysis, say so and skip it. Do not substitute made-up values.
- **Don't ask me questions unless you are truly blocked.** Make sensible decisions, state your assumptions briefly, and keep going.

## 3. Dataset

- Kaggle dataset: **`sripaadsrinivasan/indian-railways-dataset`**
  https://www.kaggle.com/datasets/sripaadsrinivasan/indian-railways-dataset
- It is described as having a large `schedules.json` file (about 82 MB), where each object is a schedule defining a train stop at a station. It likely also has train and station files.
- **I have not verified the exact files, field names or row counts. You must inspect them first and adapt to what is really there.**

### Downloading it (do this yourself)

1. Try `kagglehub` first: `kagglehub.dataset_download("sripaadsrinivasan/indian-railways-dataset")`.
2. If that needs credentials, use the Kaggle CLI with `~/.kaggle/kaggle.json` or the `KAGGLE_USERNAME` / `KAGGLE_KEY` environment variables, if they already exist.
3. If neither works, **stop and give me the exact manual steps**: download the zip from the Kaggle page, then the folder to unzip it into (`data/raw/`). Do not ask me to paste secrets into the chat, and never hard-code or commit credentials.
4. Put the files in `data/raw/`.

## 4. Build in these phases (show real output after each one)

### Phase 0: Setup

- Check `java -version`; install OpenJDK if missing (`sudo apt install openjdk-17-jre-headless -y`, or 11).
- Create a virtual environment and `requirements.txt` (pyspark, pandas, pyarrow, streamlit, matplotlib or altair, kagglehub).
- Run a 5-line Spark smoke test and print the Spark version.

### Phase 1: Inspect (`src/01_inspect.py`)

- Load every file in `data/raw/` with Spark. JSON files may be a single multi-line array, so use `multiLine=True` where needed.
- Print for each file: schema, row count, 10 sample rows, and the number of nulls in each column.
- Write a short `data/results/inspection_report.txt`.
- **Print the real row count and column names and tell me what you found before moving on.** If the main file has fewer than 12,000 rows, say so clearly.

### Phase 2: Clean and transform (`src/02_clean.py`)

Use only fields that really exist. The target is a clean "stops" table with a few columns:

- train number, train name, station code, station name, arrival time, departure time, and day number if available.
- Convert time strings to real times. Treat values like `"None"` or empty strings as null.
- Compute **stop duration in minutes** (departure minus arrival), handling trains whose stop crosses midnight. Keep the first and last stations of a train (no arrival or no departure), but mark their duration as null.
- Remove exact duplicate rows and rows with a missing train or station.
- Print how many rows were removed and why.
- Write the result to `data/processed/stops.parquet`.

### Phase 3: Analyse (`src/03_analyse.py`)

Compute the following with the Spark DataFrame API, using only fields that exist. Skip any that the data cannot support and say why.

1. **Busiest stations:** top 20 stations by number of train stops and by number of distinct trains.
2. **Stoppage times:** per station, average, max and 90th percentile (`percentile_approx`) of stop duration. Show only stations with at least a reasonable number of stops (choose a sensible minimum, such as 20).
3. **Hourly traffic:** how many train arrivals happen in each hour of the day (0 to 23).
4. **Longest trains:** the 20 trains with the most stops.
5. **Station and train type breakdowns** if the data has fields for them, such as zone, state or train type.
6. **At least two of the above written as Spark SQL queries** using `spark.sql(...)` on a temp view, to show both APIs.

Write each result as its own small Parquet file in `data/results/`. Also write `data/results/run_summary.json` with the real figures: the raw row count, the cleaned row count, the number of distinct trains, the number of distinct stations, and the pipeline run time.

### Phase 4: Website (`app.py`, Streamlit)

One file, simple and clean. It reads only from `data/results/` and `data/processed/`, using pandas on the Parquet files. It must not run Spark.

- **Header:** the project title, and a row of metric cards from `run_summary.json`.
- **Tab 1, Overview:** the busiest stations chart and a table.
- **Tab 2, Stoppage:** the stoppage chart, plus a slider for the minimum number of stops.
- **Tab 3, Hourly traffic:** an arrivals-per-hour bar chart.
- **Tab 4, Station lookup:** the user picks a station and sees all trains stopping there with their arrival, departure and stop duration (from `stops.parquet`, filtered).
- **Tab 5, About the pipeline:** a short plain-English description of Extract / Transform / Load / Analyse, the tools used, and the real limitations (below).
- The website must not crash on missing files: show a friendly message telling the user which script to run first.

### Phase 5: One-command run and README

- `run_all.sh` runs phases 1 to 3 in order, then prints the command to start the website (`streamlit run app.py`).
- `README.md` with: the problem statement, the dataset (with link), the architecture in a few lines, exact run steps for WSL, the real findings from the run (copied from the real output), and a **Limitations** section.
- `EXPLAINER.md`, about one page: what each script does and why Spark and Parquet were chosen, written so a student can explain it in a viva.

## 5. Limitations that must be stated honestly in the README and on the About tab

- This is **scheduled** timetable data, **not** actual running or delay data. Do not call anything "delay", "late" or "real-time".
- The dataset is several years old and may not match today's timetable.
- It is a batch pipeline, with no streaming.
- The data is large enough to justify Spark, but not terabytes. Say the pipeline is built with big data tools and would scale to larger data on a cluster.

## 6. Verification before you say you are done

1. Run the whole pipeline end to end from a clean state and show the real console output.
2. Start the Streamlit app, confirm it loads, and check that each tab renders without errors.
3. Report the real row counts and the actual findings.
4. List any assumption you made and anything you could not do.

Do not say "done" until you have actually run it. If a step fails, fix it, or tell me exactly why you could not.

## 7. Final message to me

When finished, reply briefly with: the project path, the exact commands to run it again, the real numbers (rows in, rows out, stations, trains), the three most interesting findings, and anything I must do manually.

---

# END OF PROMPT
