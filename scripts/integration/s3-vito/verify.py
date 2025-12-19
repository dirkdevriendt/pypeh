import os
from dotenv import load_dotenv
from botocore.session import get_session
from botocore.config import Config


def verify():
    load_dotenv()

    key = os.getenv("S3_AWS_ACCESS_KEY_ID")
    secret = os.getenv("S3_AWS_SECRET_ACCESS_KEY")
    endpoint_url = os.getenv("S3_ENDPOINT_URL")
    region = os.getenv("S3_REGION", "eu-west-1")
    bucket = os.getenv("S3_BUCKET_NAME")

    print(f"Endpoint URL: {endpoint_url}")
    print(f"Bucket: {bucket}")
    print(f"Region: {region}")

    session = get_session()

    config = Config(
        region_name="eu-west-1",
        signature_version="s3v4",
        s3={"addressing_style": "path"},
    )

    client = session.create_client(
        "s3",
        aws_access_key_id=key,
        aws_secret_access_key=secret,
        endpoint_url=endpoint_url,
        config=config,
    )

    try:
        response = client.list_objects_v2(Bucket=bucket, Prefix="cache/parc-aligned/")
        print("Objects found:")
        for obj in response.get("Contents", []):
            print(f"  - {obj['Key']}")
    except Exception as e:
        print(f"List objects failed: {e}")

    resp = client.get_object(
        Bucket=bucket,
        Key="cache/parc-aligned/adults/v20250516/ObservableProperties_Adults_Analyticalinfo.yaml",
    )
    data = resp["Body"].read()  # bytes
    text = data.decode("utf-8")
    assert len(text) > 0
    print("Connection verified")


def main():
    verify()


if __name__ == "__main__":
    main()
