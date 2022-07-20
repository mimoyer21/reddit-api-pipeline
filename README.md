# Reddit ETL Pipeline

A data pipeline to extract Reddit data from [r/dataengineering](https://www.reddit.com/r/dataengineering/).

Output is a Snowflake dashboard, providing insight into the Data Engineering official subreddit.


## Architecture

<img src="" width=70% height=70%>

1. Extract data using [Reddit API](https://www.reddit.com/dev/api/)
1. Load into [AWS S3](https://aws.amazon.com/s3/)
1. Copy into [Snowflake](https://www.snowflake.com/)
1. Transform using [dbt](https://www.getdbt.com)
1. Create Snowflake Dashboard
1. Orchestrate the above a Python script

## Output

[<img src="" width=70% height=70%>](https://app.snowflake.com/us-east-2.aws/uu74728/reddit-r_dataengineering-dash-dEKnsvQr5)

* Final output in form of Snowflake dash (may need permissions to view). Link [here](https://app.snowflake.com/us-east-2.aws/uu74728/reddit-r_dataengineering-dash-dEKnsvQr5).

## Pipeline Orchestration Details

The code in this repo handles the steps up until the transformation via dbt. dbt steps onward were configured separately (e.g. building the Snowflake dashboard).
To use this pipeline in production, the execution of dbt models could be brought into this workflow (orchestrated via the Python scripts) or executed on a schedule.

The Python script `reddit_pipeline.py` acts as the orchestrator for this pipeline, executing the following Python scripts for each stage of the pipeline:
1. `extract_reddit_etl.py` to extract data from Reddit API and save as CSV
1. `upload_aws_s3.etl.py` to upload CSV to S3
1. `upload_to_snowflake_etl.py` to copy data to Snowflake

This means the full pipeline can be run by simply running the command `python reddit_pipeline.py <output-file-name>` (where <output-file-name> is the name of the CSV to store in S3).

**For a production environment, we'd certainly want to use a more advanced implementation for orchestration, such as Airflow.**

## Configuration

As a best practice, I used a configuration.conf file to store all sensitive info (credentials for AWS, Snowflake, Reddit, etc.). To recreate this pipeline, you
should do the same, with a `configuration.conf` file in the form of:

    ```
    [aws_config]
    bucket_name = xxxxxx
    account_id = xxxxxx
    aws_region = xxxxxx

    [reddit_config]
    secret = xxxxxx
    developer = xxxxxx
    name = xxxxxx
    client_id = xxxxxx

    [snowflake_config]
    username = xxxxxx
    password = xxxxxx
    account = xxxxxx
    ```
