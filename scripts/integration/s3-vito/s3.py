import inspect
import pathlib

from contextlib import contextmanager
from pypeh.core.models.settings import S3Config
from pypeh.core.session.session import Session


@contextmanager
def get_session(bucket_list: list[str], env_file=".env", env_prefix="S3_"):
    connection_config = []
    for bucket_name in bucket_list:
        connection_config.append(
            S3Config(
                config_dict={"bucket_name": bucket_name},
                env_prefix=env_prefix,
                label=bucket_name,
            )
        )
    env_file = env_file
    session = Session(
        connection_config=connection_config,
        env_file=env_file,
    )
    yield session


def get_absolute_path(path: str) -> str:
    """Returns the absolute path of the input file given its relative path."""
    caller_frame = inspect.stack()[1]
    caller_path = pathlib.Path(caller_frame.filename).resolve().parent
    return str((caller_path / path).resolve())


def debug_connection(connection):
    print("--- S3 Debug ---")
    print(f"Protocol: {connection.protocol}")
    print(f"Root: {connection.root}")
    print(f"FileSystem Config: {connection.file_system.storage_options}")
    try:
        # Try listing the raw bucket
        bucket_only = connection.root.split("/")[0]
        print(
            f"Listing bucket '{bucket_only}': {connection.file_system.ls(bucket_only, detail=False)}"
        )
    except Exception as e:
        print(f"Debug LS failed: {e}")


def save_to_bucket(file_path: str, bucket_name: str, key: str):
    bucket_list = ["sas-peh-staging"]
    local_path = file_path
    with get_session(bucket_list=bucket_list) as session:
        try:
            with session.connection_manager.get_connection(
                connection_label=bucket_name
            ) as connection:
                remote_path = f"{connection.root}/{key}"
                connection.file_system.put(local_path, remote_path)
        except Exception as e:
            raise AssertionError(f"Error encountered while storing file: {e}")


def load_cache():
    """Requirement: VPN connection"""
    bucket_list = ["sas-peh-staging"]
    target_key = "cache/parc-aligned/adults/v20250516/"
    bucket_name = bucket_list[0]
    with get_session(bucket_list=bucket_list) as session:
        try:
            with session.connection_manager.get_connection(
                connection_label="sas-peh-staging"
            ) as connection:
                debug_connection(connection)
                print("Loading cache ...")
                session.load_persisted_cache(
                    source=target_key, connection_label=bucket_list[0]
                )
        except Exception as e:
            raise AssertionError(
                f"Error while checking existence of bucket {bucket_name}: {e}"
            )


def main():
    load_cache()


if __name__ == "__main__":
    main()
