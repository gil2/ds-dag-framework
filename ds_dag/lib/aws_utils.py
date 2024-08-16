import json
import boto3
from airflow.models import Connection


class AWSConnection:
    def __init__(self,
                 connection_id=None,
                 repository_name=None,
                 logger=None
                 ):
        self.connection_id = connection_id
        self.repository_name = repository_name
        self.logger = logger

    def get_airflow_aws_conn(self) -> dict:
        """Read the sagemaker access key airflow connection and create aws_credentials dict out of it"""

        self.logger.info(f"using {self.connection_id}")
        try:
            aws_credentials = Connection.get_connection_from_secrets(conn_id=self.connection_id)

            # Extract AWS credentials
            aws_access_key_id = aws_credentials.login
            aws_secret_access_key = aws_credentials.password
            aws_extra_details = json.loads(aws_credentials.extra or '{}')
            region_name = aws_extra_details.get('region_name', None)
            aws_session_token = aws_extra_details.get('aws_session_token', None)

            return dict(aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key,
                        aws_session_token=aws_session_token,
                        region_name=region_name)
        except Exception:
            raise RuntimeError(f"Error in loading the connection {self.connection_id}")


    def get_ecr_image_size(self, image_tag: str):
        # Create an ECR client
        aws_credentials = self.get_airflow_aws_conn()
        ecr_client = boto3.client('ecr', **aws_credentials)

        # Describe the image in the specified repository
        response = ecr_client.describe_images(
            repositoryName=self.repository_name,
            imageIds=[{'imageTag': image_tag}]
        )
        self.logger.info(f"Response: {response}")
        # Extract image details
        image_details = response['imageDetails'][0]
        self.logger.info(f"Image details: {image_details}")

        # Extract the image size in bytes
        image_size_bytes = image_details['imageSizeInBytes']

        self.logger.info(f"Image size: {image_size_bytes} bytes")
        return image_size_bytes