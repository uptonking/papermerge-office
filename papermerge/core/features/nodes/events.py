import os
import logging
from pathlib import Path

from papermerge.celery_app import app as celery_app
from papermerge.core import constants as const
from papermerge.core import config
from papermerge.core.pathlib import abs_docver_path
from papermerge.core.utils.decorators import if_redis_present

from .schema import DeleteDocumentsData

logger = logging.getLogger(__name__)

@if_redis_present
def delete_documents_s3_data(data: DeleteDocumentsData):
    celery_app.send_task(
        const.S3_WORKER_REMOVE_DOC_VER,
        kwargs={"doc_ver_ids": [str(i) for i in data.document_version_ids]},
        route_name="s3",
    )
    celery_app.send_task(
        const.S3_WORKER_REMOVE_DOCS_THUMBNAIL,
        kwargs={"doc_ids": [str(i) for i in data.document_ids]},
        route_name="s3",
    )
    celery_app.send_task(
        const.S3_WORKER_REMOVE_PAGE_THUMBNAIL,
        kwargs={"page_ids": [str(i) for i in data.page_ids]},
        route_name="s3",
    )

def delete_local_files(data: DeleteDocumentsData):
    """Delete physical files from local filesystem"""
    # Use pre-collected file paths from prepare_documents_s3_data_deletion
    if hasattr(data, 'file_paths') and data.file_paths:
        for file_path_str in data.file_paths:
            try:
                file_path = Path(file_path_str)
                if file_path.exists():
                    file_path.unlink()
                    logger.info(f"Deleted local file: {file_path}")
                else:
                    logger.warning(f"File not found for deletion: {file_path}")
            except Exception as e:
                logger.error(f"Error deleting file {file_path}: {e}")
    else:
        # Fallback: try to reconstruct file paths from document_version_ids
        logger.warning("No pre-collected file paths, attempting fallback method")
        if data.document_version_ids:
            for doc_ver_id in data.document_version_ids:
                try:
                    # Try common file extensions
                    for ext in ['.pdf', '.jpg', '.jpeg', '.png', '.tiff', '.tif']:
                        file_name = f"{doc_ver_id}{ext}"
                        file_path = abs_docver_path(doc_ver_id, file_name)
                        if file_path.exists():
                            file_path.unlink()
                            logger.info(f"Deleted local file: {file_path}")
                            break
                    else:
                        logger.warning(f"No file found for document version: {doc_ver_id}")
                except Exception as e:
                    logger.error(f"Error deleting file for {doc_ver_id}: {e}")

def delete_documents_data(data: DeleteDocumentsData):
    """Delete document files based on configured file server type"""
    file_server = config.settings.papermerge__main__file_server
    
    if file_server == config.FileServer.LOCAL:
        # Delete files from local filesystem
        delete_local_files(data)
    elif file_server == config.FileServer.S3:
        # Delete files from S3 storage
        delete_documents_s3_data(data)
    else:
        logger.warning(f"Unknown file server type: {file_server}")
