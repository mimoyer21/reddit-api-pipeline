import snowflake.connector
import configparser
import pathlib
import sys
from validation import validate_input

"""
Part of Reddit pipeline. Upload S3 CSV data to Snowflake. Takes one argument of format YYYYMMDD. This is the name of
the file to copy from S3. Script will load data into temporary table in Snowflake, delete
records with the same post ID from main table, then insert these from temp table (along with new data)
to main table. This means that if we somehow pick up duplicate records in a new DAG run,
the record in Snowflake will be updated to reflect any changes in that record, if any (e.g. higher score or more comments).
"""

# Parse configuration file
parser = configparser.ConfigParser()
script_path = pathlib.Path(__file__).parent.resolve()
parser.read(f"{script_path}/configuration.conf")

try:
  output_name = sys.argv[1]
except Exception as e:
  print(f"Command line argument not passed. Error {e}")
  sys.exit(1)

# Configuration Variables
SNOW_USER = parser.get("snowflake_config", "username")
SNOW_PW = parser.get("snowflake_config", "password")
SNOW_ACCT = parser.get("snowflake_config", "account")
BUCKET_NAME = parser.get("aws_config", "bucket_name")
DATABASE = 'reddit'
STAGING_SCHEMA = 'staging'
PROD_SCHEMA = 'prod'
prod_table = f"{PROD_SCHEMA}.r_dataengineering_posts"
staging_table = f"{STAGING_SCHEMA}.r_dataengineering_posts_staging"

# Our S3 file & role_string
file_path = f"s3://{BUCKET_NAME}/{output_name}.csv"

# Check command line argument passed
try:
  output_name = sys.argv[1]
except Exception as e:
  print(f"Command line argument not passed. Error {e}")
  sys.exit(1)

# Create Snowflake prod table if it doesn't exist
sql_create_table = f"""CREATE TABLE IF NOT EXISTS {prod_table} (
                            id varchar PRIMARY KEY,
                            title varchar(),
                            score int,
                            num_comments int,
                            author varchar(),
                            created_utc timestamp,
                            url varchar(),
                            upvote_ratio float,
                            over_18 boolean,
                            edited boolean,
                            spoiler boolean,
                            stickied boolean
                        );"""

# If ID already exists in table, we remove it and add new ID record during load.
create_temp_table = f"CREATE TEMP TABLE {staging_table} LIKE {prod_table};"
sql_copy_to_temp = f"""
    COPY INTO {staging_table}
    FROM @my_s3_stage/{output_name}.csv
    ;""" # my_s3_stage has been configured in Snowflake directly to store the reddit_s3_integration with details like s3_url + default CSV format
delete_from_table = f"DELETE FROM {prod_table} USING {staging_table} WHERE {prod_table}.id = {staging_table}.id;"
insert_into_table = f"INSERT INTO {prod_table} SELECT * FROM {staging_table};"
drop_temp_table = f"DROP TABLE {staging_table};"

def connect_to_snowflake():
    """Connect to Snowflake instance"""
    try:
        snowflake_conn = snowflake.connector.connect(
            user = SNOW_USER,
            password = SNOW_PW,
            account = SNOW_ACCT
        )
        return snowflake_conn
    except Exception as e:
        print(f"Unable to connect to Snowflake. Error {e}")
        sys.exit(1)

def load_data_into_snowflake(snowflake_conn):
    """Load data from S3 into Snowflake"""
    with snowflake_conn:
        cur = snowflake_conn.cursor()
        cur.execute(f"USE DATABASE {DATABASE};")
        cur.execute(sql_create_table)
        cur.execute(create_temp_table)
        cur.execute(sql_copy_to_temp)
        cur.execute(delete_from_table)
        cur.execute(insert_into_table)
        cur.execute(drop_temp_table)

        # Commit only at the end, so we won't end up
        # with a temp table and deleted main table if something fails
        # snowflake_conn.commit() # Commented this line out. By default, autocommit mode is enabled in Snowflake connector. Can figure out how to override to use commit if desired


def main():
    """Upload file from S3 to Snowflake table"""
    validate_input(output_name)
    snowflake_conn = connect_to_snowflake()
    load_data_into_snowflake(snowflake_conn)

if __name__ == '__main__':
  main()
