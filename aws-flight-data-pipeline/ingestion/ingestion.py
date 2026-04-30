import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import logging

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s"
)
logger = logging.getLogger(__name__)

# Configuration
AWS_REGION = "us-east-1"

BUCKET_NAME  = "flight-pipeline-vishara"
S3_RAW_KEY   = "raw/Clean_Dataset.csv"
LOCAL_FILE   = r"D:\Vishara\IIT\BSC HONS AIDS\Y2\Semester 2\Data Engineering\cw\Clean_Dataset.csv"

# S3 client
def get_s3_client():
    return boto3.client(
        "s3",
        region_name=AWS_REGION
    )

# Upload function
def upload_to_s3(local_path: str, bucket: str, s3_key: str) -> bool:
    s3 = get_s3_client()
    try:
        logger.info(f"Uploading  {local_path}")
        logger.info(f"       →   s3://{bucket}/{s3_key}")
        s3.upload_file(local_path, bucket, s3_key)
        logger.info("Upload complete.")
        return True

    except FileNotFoundError:
        logger.error(f"Local file not found: {local_path}")
    except NoCredentialsError:
        logger.error("AWS credentials are missing. Run 'aws configure'.")
    except ClientError as e:
        logger.error(f"AWS error: {e.response['Error']['Message']}")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")

    return False

# Entry point
if __name__ == "__main__":
    logger.info("=== Flight Pipeline — Ingestion started ===")
    success = upload_to_s3(LOCAL_FILE, BUCKET_NAME, S3_RAW_KEY)

    if success:
        logger.info("=== Ingestion completed successfully ===")
    else:
        logger.error("=== Ingestion failed ===")