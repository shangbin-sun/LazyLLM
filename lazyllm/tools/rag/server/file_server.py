import os
import re
import logging
from fastapi import UploadFile, File, HTTPException
from fastapi.responses import FileResponse
import s3fs
from fsspec.implementations.local import LocalFileSystem
from lazyllm import FastapiApp as app
from lazyllm.tools.rag.utils import BaseResponse
from lazyllm.tools.rag.db import FileRecord, FORDER_TYPE

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Ensure the directory exists
FILE_STORAGE_DIR = "/yhm/jisiyuan/LazyLLM/dataset/kb_server_test"
STORE_MODE = "local"
BASE_NAME = "bucket"

def ensure_directory_exists(directory: str):
    """
    Ensure that a directory exists. If it doesn't, create it.
    """
    if not os.path.exists(directory):
        os.makedirs(directory)

ensure_directory_exists(FILE_STORAGE_DIR)

# def validate_folder_name(folder_name: str) -> str:
#     """
#     Validate the folder name.
#     """
#     if not folder_name:
#         raise HTTPException(status_code=400, detail="Folder name cannot be empty.")
    
#     folder_name = folder_name.strip()
#     if len(folder_name) < 1 or len(folder_name) > 50:
#         raise HTTPException(status_code=400, detail="Folder name must be between 1 and 50 characters.")
    
#     if not re.match(r'^[a-zA-Z0-9-_]+$', folder_name):
#         raise HTTPException(status_code=400, detail="Folder name can only contain letters, numbers, hyphens, and underscores.")
#     return folder_name

class MyLocalFileSystem(LocalFileSystem):
    def __init__(self, root_path=""):
        self.root_path = root_path
        super().__init__()
    
    def _strip_protocol(self, path):
        if not path.startswith(self.root_path):
            path = os.path.join(self.root_path, path)
        new_path = super()._strip_protocol(path)
        return new_path


class MyS3FileSystem(s3fs.S3FileSystem):
    root_path = BASE_NAME
    def __init__(self, root_path, **kwargs):
        self.root_path = root_path
        super().__init__(**kwargs)
        if not self.exists(root_path):
            self.mkdir(root_path)
            
    @classmethod
    def _strip_protocol(cls, path):
        # 添加默认桶名称
        if not path.startswith(cls.root_path):
            path = f"{cls.root_path}/{path}"
        new_path = super()._strip_protocol(path)
        return new_path
    
if STORE_MODE == "local":
    fs = MyLocalFileSystem(root_path=os.path.join(FILE_STORAGE_DIR, BASE_NAME))
elif STORE_MODE == "minio":
    # MinIO 配置
    minio_endpoint = "http://103.177.28.196:9000"
    minio_access_key = "ROOTNAME"
    minio_secret_key = "CHANGEME123"
    fs = MyS3FileSystem(
        root_path=BASE_NAME,
        key=minio_access_key,
        secret=minio_secret_key,
        client_kwargs={'endpoint_url': minio_endpoint}
    )

class FileManageBase:
    def _upload_file(self, file: UploadFile, forder_path: str = '', is_overwrite: bool = True):
        """
        Upload a file to the server.
        """
        try:
            file_path = os.path.join(forder_path, file.filename)
            file_info = FileRecord.first(file_path=file_path)
            if file_info and not is_overwrite:
                # return BaseResponse(msg="File already exists and will not be overwritten.")
                return 
            
            if file_info and is_overwrite:
                FileRecord.del_node(file_path=file_path)
            
            # 删除文件
            if fs.exists(file_path):
                fs.rm(file_path)
            # 保存文件
            with fs.open(file_path, "wb") as file_object:
                file_object.write(file.file.read())

            file_info = FileRecord(
                file_name=file.filename,
                file_path=file_path,
                file_type=os.path.splitext(file.filename)[1][1:].upper(),
                file_size=file.size
            )
            node = FileRecord.add_node(file_info)
            return node
        except Exception as e:
            raise e
    
    def _create_folder(self, folder_path: str = None, add_file_node = False):
        """
        Create a new folder.
        """
        try:
            if not fs.exists(folder_path):
                fs.mkdir(folder_path)
            if add_file_node:
                node = FileRecord.create(
                    file_name=folder_path,
                    file_path=folder_path,
                    file_type=FORDER_TYPE,
                    file_size=0
                )
                return node
        except Exception as e:
            raise e
    
    def _delete_file(self, file_id: int):
        """
        Delete a file by its ID.
        """
        file_record = FileRecord.first(id=file_id)
        if not file_record:
            raise Exception("File not found")
        
        try:
            fs.rm(file_record.file_path)
        except Exception as e:
            logger.error(f"Failed to delete file {file_record.file_path}: {str(e)}")
        
        try:
            FileRecord.del_node(id=file_id)
        except Exception as e:
            logger.error(f"Failed to delete file record {file_id}: {str(e)}")
            

class FileServer(FileManageBase):
    """
    File server for managing file operations.
    """
    def _build_file_tree(self, all_files: list) -> BaseResponse:
        """
        Get the file tree structure.
        """
        files_dict = {}
        for file in all_files:
            files_dict[file.file_path] = {
                "name": file.file_name,
                "type": "folder" if file.file_type == FORDER_TYPE else "file",
                "children": []
            }

        file_tree = []
        for file in all_files:
            parent_path = os.path.dirname(file.file_path)
            if parent_path in files_dict:
                files_dict[parent_path]["children"].append(files_dict[file.file_path])
            else:
                file_tree.append(files_dict[file.file_path])
        return file_tree
    
    @app.post("/create_folder")
    def create_folder(self, folder_path: str = None) -> BaseResponse:
        """
        Create a new folder.
        """
        try:
            node = self._create_folder(folder_path=folder_path, add_file_node=True)
            return BaseResponse(msg=f"Folder '{folder_path}' created successfully.", data_id=node.id)
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.post("/upload_file")
    async def upload_file(self, file: UploadFile = File(...), forder_path: str = '', is_overwrite: bool = True) -> BaseResponse:
        """
        Upload a file to the server.
        """
        try:
            node = self._upload_file(file=file, forder_path=forder_path, is_overwrite=is_overwrite)
            if node:
                return BaseResponse(msg=f"File '{node.file_name}' uploaded successfully.")
            else:
                BaseResponse(msg="File already exists and will not be overwritten.")
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @app.get("/list_files")
    def list_files(self, skip: int = 0, limit: int = 10) -> BaseResponse:
        """
        List files with pagination.
        """
        files = FileRecord.filter_by(skip=skip, limit=limit)
        files = [repr(file) for file in files]
        return BaseResponse(data=files)

    @app.get("/get_file")
    def get_file(self, file_id: int):
        """
        Download a file by its ID.
        """
        file_record = FileRecord.first(id=file_id)
        
        if not file_record:
            raise HTTPException(status_code=404, detail="File not found")

        if file_record.file_type != FORDER_TYPE:
            cache_file_path = os.path.join(FILE_STORAGE_DIR, ".cache", file_record.file_name)
            fs.get_file(file_record.file_path, cache_file_path)
            return FileResponse(cache_file_path, filename=file_record.file_name)  # 只接收文件路径
        else:
            files = FileRecord.filter_by_conditions(FileRecord.file_path.startswith(file_record.file_path))
            return self._build_file_tree(files)

    @app.get("/delete_file")
    def delete_file(self, file_id: int) -> BaseResponse:
        """
        Delete a file by its ID.
        """
        try:
            self._delete_file(file_id)
        except Exception as e:
            raise HTTPException(status_code=404, detail=str(e))
        return BaseResponse(msg=f"File ID {file_id} deleted successfully.")
    
    @app.get("/get_file_tree")
    def get_file_tree(self):
        all_files = FileRecord.all()  # Get all file information
        data=self._build_file_tree(all_files)
        return BaseResponse(data=data)
    