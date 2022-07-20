import sys
from validation import validate_input
from extract_reddit_etl import main as extract_from_reddit
from upload_aws_s3_etl import main as upload_to_s3
from upload_to_snowflake_etl import main as upload_to_snowflake

"""
This script simply orchestrates the execution of the steps of the pipeline:
    1. Extract data from Reddit and store as CSV
    2. Upload CSV to S3
    3. Copy data from S3 into Snowflake

Typically this would be done via an orchestrator such as Airflow, but this is a lightweight implementation.

The subsequent steps of the project (transformation in dbt and creation of charts) happens outside of this pipeline.
In a production environment, we'd likely include the dbt transformations as part of this pipeline. (Again, excluded for simplicity of the implementation.)
"""

try:
  output_name = sys.argv[1]
except Exception as e:
  print(f"Command line argument not passed. Error {e}")
  sys.exit(1)

def main():
    """Orchestrate the execution of the steps of the pipeline:
        1. Extract data from Reddit and store as CSV
        2. Upload CSV to S3
        3. Copy data from S3 into Snowflake
    """
    validate_input(output_name)
    extract_from_reddit()
    upload_to_s3()
    upload_to_snowflake()

if __name__ == '__main__':
  main()
