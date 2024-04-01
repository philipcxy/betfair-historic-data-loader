import os

from azure.storage.filedatalake import (
    DataLakeServiceClient,
    DataLakeFileClient,
    FileSystemClient,
    PathProperties,
)

from shared.key_vault_wrapper import KeyVaultWrapper


class AdlsWrapper:
    def __init__(self, key_vault_wrapper: KeyVaultWrapper):
        self._kv_wrapper: KeyVaultWrapper = key_vault_wrapper

        self._account_name = os.environ.get("ADLS_ACCOUNT_NAME")
        self._file_system_name = os.environ.get("ADLS_FILE_SYSTEM_NAME")
        self._sas_secret_name = os.environ.get("ADLS_SAS_TOKEN_SECRET_NAME")
        self._sas_token = None

    def get_service_client(self) -> DataLakeServiceClient:
        account_url = f"https://{self._account_name}.dfs.core.windows.net"
        if not self._sas_token:
            self._sas_token = self._kv_wrapper.get_secret(self._sas_secret_name)

        service_client = DataLakeServiceClient(account_url, credential=self._sas_token)

        return service_client

    def get_file_client(self, path: str) -> DataLakeFileClient:
        service_client: DataLakeServiceClient = self.get_service_client()
        file_client: DataLakeFileClient = service_client.get_file_client(
            self._file_system_name, path
        )

        return file_client

    def get_file_system_client(self) -> FileSystemClient:
        service_client: DataLakeServiceClient = self.get_service_client()
        fs_client: FileSystemClient = service_client.get_file_system_client(
            self._file_system_name
        )

        return fs_client

    def list_tar_files(self, directory_name: str) -> list[str]:
        fs_client: FileSystemClient = self.get_file_system_client()

        paths: list[PathProperties] = fs_client.get_paths(
            path=directory_name, recursive=False
        )

        return [path.name for path in paths if path.name[-4:] == ".tar"]

    def get_file_content(self, path: str) -> bytes:
        file_client = self.get_file_client(path)

        download = file_client.download_file()
        file_bytes = download.readall()

        return file_bytes

    def upload_bytes(self, path: str, content: bytes):
        file_client: DataLakeFileClient = self.get_file_client(path)
        file_client.upload_data(content, overwrite=True)

    def move_file(self, source: str, destination: str):
        directory = (
            "/".join(destination.split("/")[:-1]) if "." in destination else destination
        )
        directory_client: FileSystemClient = self.get_file_system_client()
        directory_client.create_directory(directory)

        file_client: DataLakeFileClient = self.get_file_client(source)
        file_client.rename_file(f"{self._file_system_name}/{destination}")
