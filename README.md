# Streaming with PySpark
This project consists of creating a workflow with Apache Airflow that will download files from a download source (currently only supports **Google Drive** and only **CSV** files) and will process them in real time with PySpark using the tool's streaming functionality.

# Requirements
To use the project, you **must have** at least 4 processor cores and 4GB of RAM available for Spark to be able to use. <br>
It is also desirable that you know how to use Apache Airflow to start the workflow, define a schedule of your choice and basically configure your DAG's. <br>
The project was created using a virtual environment with Python 3.9.6 and Apache Airflow 2.6.3, it is **highly recommended** that you create your Python virtual environment with this version of the language, some things may not happen as expected if you use a different version from what I reported.

## First steps
- Create your Python virtual environment with version 3.9.6 and install dependencies with the requirements.txt file
-  After installing the dependencies, everything will be practically ready to use.
