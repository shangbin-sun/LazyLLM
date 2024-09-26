from typing import List, Optional, Union, Tuple, Callable

from fastapi import Body, UploadFile, HTTPException
from fastapi.responses import RedirectResponse 

import lazyllm
from lazyllm import ServerModule
from lazyllm import FastapiApp as app
from .file_server import FileManageBase
from ..utils import BaseResponse, save_files_in_threads
from ..db import KBInfoRecord, KBFileRecord, FileRecord, FileState
from ..document import Document

DocCreater = Callable[[str], Document]

class KBServerBase(FileManageBase):
    """
    Document server for managing knowledge bases and file uploads.
    """
    def __init__(self) -> None:
        super().__init__()

    @app.get("/", response_model=BaseResponse, summary="docs")
    def document(self):
        """
        Redirects to the documentation page.
        """
        return RedirectResponse(url="/docs")

    @app.get("/list_knowledge_bases")
    def list_knowledge_bases(self):
        """
        Lists all knowledge bases.
        """
        kbs = KBInfoRecord.all()
        kbs = [repr(kb) for kb in kbs]
        return BaseResponse(data=kbs)

    @app.post("/upload_files")
    def upload_files(self, files: List[UploadFile], kb_name: str, override: bool):
        """
        Uploads files to a knowledge base.
        """
        # 获取已经存在的文件
        files_exists, files_new = [], []
        exists_nodes = []
        for file in files:
            file_node = FileRecord.filter_by_conditions(FileRecord.file_path.startswith(f"_kb/{kb_name}"), FileRecord.file_name==file.filename)
            if file_node:
                files_exists.append(file)
                exists_nodes.extend(file_node)
            else:
                files_new.append(file)
        # 重写则删除已存在文件
        if override:
            for node in exists_nodes:
                KBFileRecord.del_node(file_id=node.id)
            files_add = files_new + files_exists
        else:
            files_add = files_new
        
        # 处理需要添加的文件
        for file in files_add:
            # 拷贝到文件系统，写入文件表格
            node = self._upload_file(file=file, forder_path=f"_kb/{kb_name}", is_overwrite=override)
            # 写入数据库表格
            if node:
                KBFileRecord.create(kb_name=kb_name, file_id=node.id, file_name=node.file_name)

        return BaseResponse(
            data={
                "already_exist_files": [file.filename for file in files_exists],
                "new_add_files": [file.filename for file in files_add],
                "if_overwritten": override,
            }
        )
    
    @app.get("/list_files")
    def list_files(self, kb_name: str):
        """
        Lists all files in a knowledge base.
        """
        file_list = KBFileRecord.all(kb_name=kb_name)
        file_list = [repr(file) for file in file_list]
        return BaseResponse(data=file_list)

    @app.post("/delete_file")
    def delete_file(self, kb_name: str, file_id: str):
        """
        Deletes a file from a knowledge base.
        """
        KBFileRecord.update(
            fun=lambda x: x.set(state=FileState.WAIT_DELETE), 
            kb_name=kb_name, 
            file_id=file_id
        )
        try:
            self._delete_file(file_id)
        except Exception as e:
            raise HTTPException(status_code=404, detail=str(e))
                
        KBFileRecord.del_node(kb_name=kb_name,file_id=file_id)
        return BaseResponse(msg=f"delete {file_id} success")

    def __repr__(self):
        """
        String representation of the DocumentServer instance.
        """
        return lazyllm.make_repr("Module", "DocManager")
    
    @classmethod
    def start_server(cls, launcher=None, port=None,**kwargs):
        launcher = launcher if launcher else lazyllm.launchers.remote(sync=False)
        doc_server = ServerModule(cls(**kwargs), port=port, launcher=launcher)
        doc_server.start()

class KBServer(KBServerBase):
    """
    Document server for managing knowledge bases and file uploads.
    """
    def __init__(self, doc_creater: DocCreater) -> None:
        super().__init__()
        self._doc_creater = doc_creater
        self._doc_dict = {}

    @app.post("/create_knowledge_base")
    def create_knowledge_base(
        self, 
        kb_name: str = Body(..., examples=["samples"]),
        kb_info: str = Body(..., examples=["samples info"])
    ):
        """
        Creates a new knowledge base.
        """
        assert KBInfoRecord.all(kb_name=kb_name) == [], f"kb_name {kb_name} already exist"

        doc = self._doc_creater(kb_name)
        self._doc_dict[kb_name] = doc
        
        self._create_folder(f"_kb/{kb_name}")
        KBInfoRecord.create(kb_name=kb_name, kb_info=kb_info)
        return BaseResponse(msg=f"create {kb_name} success")

    @app.post("/delete_knowledge_base")
    def delete_knowledge_base(self, kb_name: str):
        """
        Deletes an existing knowledge base.
        """
        KBInfoRecord.del_node(kb_name=kb_name)

        files = KBFileRecord.all(kb_name=kb_name)
        for file in files:
            self.delete_file(kb_name=kb_name, file_id=file.id)
        return BaseResponse(msg=f"delete {kb_name} success")