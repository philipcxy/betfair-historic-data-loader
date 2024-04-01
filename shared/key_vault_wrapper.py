import os
from azure.keyvault.secrets import SecretClient, KeyVaultSecret
from azure.identity import DefaultAzureCredential


class KeyVaultWrapper:
    def __init__(self):
        key_vault_name = os.environ.get("KEY_VAULT_NAME")
        self._kv_uri = f"https://{key_vault_name}.vault.azure.net"

    def get_secret(self, secret_name: str) -> str:
        client = self.get_client()

        secret: KeyVaultSecret = client.get_secret(secret_name)

        return secret.value

    def get_client(self) -> SecretClient:
        credential = DefaultAzureCredential()
        client = SecretClient(vault_url=self._kv_uri, credential=credential)

        return client
