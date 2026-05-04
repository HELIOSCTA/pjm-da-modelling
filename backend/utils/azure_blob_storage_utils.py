"""Azure Blob Storage client.

Ported from helioscta_python (helioscta.utils.azure_blob_storage_utils).
Env loading is delegated to backend.credentials / backend.settings; do not
load .env inside this module.

Env vars consumed (with `MORNING_BRIEFING_*` taking precedence over generic):
    MORNING_BRIEFING_BLOB_CONNECTION_STRING | AZURE_STORAGE_CONNECTION_STRING
    MORNING_BRIEFING_BLOB_ACCOUNT_NAME      | AZURE_STORAGE_ACCOUNT_NAME
    MORNING_BRIEFING_BLOB_CONTAINER         | AZURE_STORAGE_CONTAINER_NAME
"""
from __future__ import annotations

import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Union

import pandas as pd
from azure.core.exceptions import AzureError
from azure.storage.blob import BlobClient, BlobProperties, BlobServiceClient, ContentSettings


def _env(*names: str) -> Optional[str]:
    for n in names:
        v = os.getenv(n)
        if v:
            return v
    return None


class AzureBlobStorageClient:
    """Client for Azure Blob Storage operations."""

    def __init__(
        self,
        connection_string: Optional[str] = None,
        storage_account_name: Optional[str] = None,
        container_name: Optional[str] = None,
    ):
        self.connection_string = connection_string or _env(
            "MORNING_BRIEFING_BLOB_CONNECTION_STRING",
            "AZURE_STORAGE_CONNECTION_STRING",
        )
        self.storage_account_name = storage_account_name or _env(
            "MORNING_BRIEFING_BLOB_ACCOUNT_NAME",
            "AZURE_STORAGE_ACCOUNT_NAME",
        )
        self.container_name = container_name or _env(
            "MORNING_BRIEFING_BLOB_CONTAINER",
            "AZURE_STORAGE_CONTAINER_NAME",
        )

        if not self.connection_string:
            raise ValueError(
                "Azure Blob connection string not configured. Set "
                "MORNING_BRIEFING_BLOB_CONNECTION_STRING or AZURE_STORAGE_CONNECTION_STRING."
            )
        if not self.container_name:
            raise ValueError(
                "Azure Blob container not configured. Set "
                "MORNING_BRIEFING_BLOB_CONTAINER or AZURE_STORAGE_CONTAINER_NAME."
            )

    def get_blob_service_client(self) -> BlobServiceClient:
        return BlobServiceClient.from_connection_string(self.connection_string)

    def get_blob_client(
        self,
        blob_name: str,
        container_name: Optional[str] = None,
    ) -> BlobClient:
        container = container_name or self.container_name
        service_client = self.get_blob_service_client()
        return service_client.get_blob_client(container=container, blob=blob_name)

    def upload_blob(
        self,
        data: Union[str, bytes],
        blob_name: str,
        container_name: Optional[str] = None,
        content_type: Optional[str] = None,
        content_disposition: Optional[str] = None,
        overwrite: bool = True,
        metadata: Optional[Dict[str, str]] = None,
    ) -> str:
        container = container_name or self.container_name

        try:
            blob_client = self.get_blob_client(blob_name, container)

            content_settings = None
            if content_type or content_disposition:
                content_settings = ContentSettings(
                    content_type=content_type,
                    content_disposition=content_disposition or "inline",
                )

            blob_client.upload_blob(
                data,
                overwrite=overwrite,
                content_settings=content_settings,
                metadata=metadata,
            )

            url = self.get_blob_url(blob_name, container)
            logging.info(f"Uploaded blob: {blob_name} to {url}")
            return url

        except AzureError as e:
            logging.error(f"Error uploading blob: {str(e)}")
            raise

    def upload_file(
        self,
        file_path: Union[str, Path],
        blob_name: Optional[str] = None,
        container_name: Optional[str] = None,
        content_type: Optional[str] = None,
        overwrite: bool = True,
    ) -> str:
        file_path = Path(file_path)

        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        if blob_name is None:
            blob_name = file_path.name

        if content_type is None:
            content_type = self._get_content_type(file_path)

        with open(file_path, 'rb') as file_data:
            return self.upload_blob(
                data=file_data.read(),
                blob_name=blob_name,
                container_name=container_name,
                content_type=content_type,
                overwrite=overwrite,
            )

    def upload_dataframe_csv(
        self,
        df: pd.DataFrame,
        blob_name: str,
        container_name: Optional[str] = None,
        overwrite: bool = True,
        include_timestamp: bool = False,
        **csv_kwargs,
    ) -> str:
        if include_timestamp:
            name, ext = blob_name.rsplit('.', 1) if '.' in blob_name else (blob_name, 'csv')
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            blob_name = f"{name}_{timestamp}.{ext}"

        if not blob_name.endswith('.csv'):
            blob_name = f"{blob_name}.csv"

        csv_data = df.to_csv(index=False, **csv_kwargs)

        return self.upload_blob(
            data=csv_data,
            blob_name=blob_name,
            container_name=container_name,
            content_type='text/csv',
            overwrite=overwrite,
        )

    def upload_html(
        self,
        html_content: str,
        blob_name: str,
        container_name: Optional[str] = None,
        overwrite: bool = True,
        include_timestamp: bool = False,
    ) -> str:
        if include_timestamp:
            name, ext = blob_name.rsplit('.', 1) if '.' in blob_name else (blob_name, 'html')
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            blob_name = f"{name}_{timestamp}.{ext}"

        if not blob_name.endswith('.html'):
            blob_name = f"{blob_name}.html"

        return self.upload_blob(
            data=html_content,
            blob_name=blob_name,
            container_name=container_name,
            content_type='text/html',
            overwrite=overwrite,
        )

    def download_blob(
        self,
        blob_name: str,
        container_name: Optional[str] = None,
    ) -> bytes:
        try:
            blob_client = self.get_blob_client(blob_name, container_name)
            blob_data = blob_client.download_blob()
            return blob_data.readall()
        except AzureError as e:
            logging.error(f"Error downloading blob: {str(e)}")
            raise

    def delete_blob(
        self,
        blob_name: str,
        container_name: Optional[str] = None,
    ) -> bool:
        try:
            blob_client = self.get_blob_client(blob_name, container_name)
            blob_client.delete_blob()
            logging.info(f"Deleted blob: {blob_name}")
            return True
        except AzureError as e:
            logging.error(f"Error deleting blob: {str(e)}")
            raise

    def list_blobs(
        self,
        container_name: Optional[str] = None,
        name_starts_with: Optional[str] = None,
    ) -> list:
        container = container_name or self.container_name

        try:
            service_client = self.get_blob_service_client()
            container_client = service_client.get_container_client(container)
            blobs = container_client.list_blobs(name_starts_with=name_starts_with)
            return [blob.name for blob in blobs]
        except AzureError as e:
            logging.error(f"Error listing blobs: {str(e)}")
            raise

    def list_blobs_with_properties(
        self,
        name_starts_with: Optional[str] = None,
        container_name: Optional[str] = None,
    ) -> List[BlobProperties]:
        container = container_name or self.container_name

        try:
            service_client = self.get_blob_service_client()
            container_client = service_client.get_container_client(container)
            return list(container_client.list_blobs(name_starts_with=name_starts_with))
        except AzureError as e:
            logging.error(f"Error listing blobs with properties: {str(e)}")
            raise

    def blob_exists(
        self,
        blob_name: str,
        container_name: Optional[str] = None,
    ) -> bool:
        try:
            blob_client = self.get_blob_client(blob_name, container_name)
            return blob_client.exists()
        except AzureError as e:
            logging.error(f"Error checking blob existence: {str(e)}")
            return False

    def get_blob_url(
        self,
        blob_name: str,
        container_name: Optional[str] = None,
    ) -> str:
        container = container_name or self.container_name
        if not self.storage_account_name:
            from urllib.parse import quote
            return f"<blob:{quote(container)}/{quote(blob_name)}>"
        return f"https://{self.storage_account_name}.blob.core.windows.net/{container}/{blob_name}"

    def _get_content_type(self, file_path: Path) -> str:
        import mimetypes
        content_type, _ = mimetypes.guess_type(str(file_path))
        return content_type or 'application/octet-stream'
