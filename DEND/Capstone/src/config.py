# FILE FORMATS
DEFAULT_OUTPUT_FILE_FORMAT = "csv"
DEFAULT_DELIMITER = ","

# AWS Credentials
AWS_ACCESS_KEY_ID = "AKIAU4EED6GQE46YEWH2"
AWS_SECRET_ACCESS_KEY = "Removed for security"

# S3
S3_BUCKET = "s3://udacity-capstone/"
INPUT_PATH = "{}{}".format(S3_BUCKET,"input")
OUTPUT_PATH = "{}{}/{}".format(S3_BUCKET,"output",DEFAULT_OUTPUT_FILE_FORMAT)

# REDSHIFT CONNECTION PARAMETERS
HOST = "udacity-capstone.chbi3wa10xxr.us-west-2.redshift.amazonaws.com"
DBNAME = "udacity"
USER = "astronaut"
PASSWORD = "Removed for security"
PORT = "5439"
DEFAULT_SCHEMA = "public"

# IAM ROLE FOR REDSHIFT
ARN = "Removed for security"

# PROGRAM CONSTANTS
air_quality_pollutants = [44201, 42401, 42101, 42602, 88101, 81102]

