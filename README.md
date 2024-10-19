# Streaming with PySpark

This project involves creating a workflow with Apache Airflow that downloads files from a source (currently supporting **Google Drive** and only **CSV** files) and processes them in real time using PySpark’s streaming functionality.

## Requirements

To use this project, you **must have** at least 4 processor cores and 4GB of RAM available for Spark.
It is also desirable that you know how to use Apache Airflow to start the workflow, define a schedule, and configure your DAGs.
The project was created using a virtual environment with Python 3.9.6 and Apache Airflow 2.6.3. It is **highly recommended** that you create your Python virtual environment using this version of Python, as some things may not work as expected with other versions.
Download and install PostgreSQL in your environment, as this is the database the project uses.
Download the [PostgreSQL driver](https://jdbc.postgresql.org/download/) that will be used by Spark to submit messages to the tables.
You must have a Google account to create files on Google Drive.

## Note

This project was built on Windows 11 using Ubuntu in WSL2. I recommend using an environment with Linux (preferably Ubuntu), WSL2, or a Linux-based VM. I cannot guarantee that everything will work if you use Windows as your main OS.

## First steps

- Create your Python virtual environment with version 3.9.6 and install dependencies from the requirements.txt file.
- After installing the dependencies, everything will be almost ready to use.
- Activate your Python environment and start Airflow using the command: `airflow standalone`. This will start Airflow with your configuration. Move the `/airflow/dags/download_files_from_drive.py` to **your dags folder**.
- Start the PostgreSQL service with the command `sudo service postgresql start` and create the tables using the SQL files located in `/profiles/sql/CREATE_RAW_PROFILES.sql` and `/tracking/sql/CREATE_RAW_TRACKING.sql`.

## Changing Airflow dag settings

The Airflow DAG in this project is configured to download a file from Google Drive and convert it into a format that Spark will accept. In my tests, I saved the files with the same name and shared the link with "Anyone with the link," so the link remains the same. The changes are then inserted into PostgreSQL based on your schedule settings. If no changes are made to the files, Spark will detect that and skip any duplicate entries.

> I have included two file examples in this directory. You can create data using their schema.

The DAG schedule is set to run with the cron: `* * * * *`. You can change this to your preferred schedule. The timezone is set to **America/Sao_Paulo** (my default timezone), so you may want to change that too.

Update the `FILE_ID_<>` constant with your Google Drive file link. The Python library used to download the file follows a specific rule. Copy only the file ID, which is located after `/file/d/` and before `/view` in the URL (e.g., `https://drive.google.com/file/d/<1-random_strings>(this is the ID)/view?usp=drive_link`). Copy that ID and paste it into the constant.

## Changing Spark settings

In each directory (profiles and tracking), there is a UDF and a main file that processes data from the files, converts it into JSON messages, and writes them to PostgreSQL.

Start Spark with the command `start-master.sh`, which will launch the Spark Master on `localhost:8080`. Access it and copy your Spark URL, which can be found next to **Spark Master at** in the upper left corner. After copying the URL, start your Spark worker using the command `start-worker.sh -h <YOUR_SPARK_URL> -c 6 -m 4G`. The **-c** parameter specifies the number of CPU cores, and the **-m** parameter specifies memory (6 cores and 4GB RAM are required for this project).

In the UDF file, update the `transform_csv_to_json` function with your Spark URL and set the number of cores to 1.
In the main file, do the same in the `run_streaming` function. Then, update the `insert_postgres` function with your PostgreSQL settings. At this point, save the PostgreSQL driver in the main directory.

## Starting streaming

Once you’ve made the necessary changes, run the following commands in your terminal, in this order:

- `spark-submit profiles/udf/udf.py`
- `spark-submit tracking/udf/udf.py`
- `spark-submit --jars postgresql-<version_you_downloaded>.jar profiles/pyspark_streaming_profiles.py`
- `spark-submit --jars postgresql-<version_you_downloaded>.jar tracking/pyspark_streaming_tracking.py`

After completing these steps, your streaming process will be running. Based on your Airflow schedule, it will periodically download the files, and if changes are detected, Spark will continuously update your PostgreSQL tables.