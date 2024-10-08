import os
import sys
curt_file_path = os.path.realpath(__file__) if "__file__" in globals() else os.getcwd()
sys.path.append(curt_file_path[:curt_file_path.index("LazyLLM") + len("LazyLLM")])

from .table_user import User
from .db_manager import DBManager
from lazyllm.tools.rag.db import KBFileRecord, FileRecord, FileState

DBManager.create_db_tables()

user = User.create(username='alice', email='alice@example.com')
first = User.first(username="alice")
User.update(fun=lambda x: x.set(email='EMAIL'), username="alice")
item = User.first(username="alice")
tweets = User.all(username="alice")
for tweet in tweets:
    print(tweet)

def test_del_nodes(): # 删除
    User.create(username='a1', email='email_del_node')
    User.create(username='a2', email='email_del_node')
    User.create(username='a3', email='email_del_node')
    User.del_node(username=["a1","a2","a3"])
    assert User.all(username="email_del_node") == []
    
    User.create(username='a1', email='email_del_node')
    User.del_node(username='a1')
    assert User.all(username="a1") == []
    
def test_add_node(): # 添加节点
    User.del_node(username='add_node')
    user = User(username='add_node', email='-')
    User.add_node(user)
    assert User.first(username='add_node').email == '-'
    
def test_add_or_replace_node(): # 新增或替换节点
    User.del_node(username='add_node')
    User.del_node(email='email_add_or_replace_node')
    
    User.create(username='add_node', email='email_add_or_replace_node')
    assert User.first(email='email_add_or_replace_node').username == 'add_node'
    
    user = User(username='replace_node', email='email_add_or_replace_node')
    User.add_or_replace_node(user, email='email_add_or_replace_node')
    
    assert User.first(email='email_add_or_replace_node').username == 'replace_node'
    

def test_update(): # 更新节点
    User.del_node(username='update_node')
    User.del_node(email='email_update_node')
    
    User.create(username='update_node', email='email_update_node')
    User.update(fun=lambda node:node.set(email='email_new'), username='update_node')
    assert User.first(username='update_node').email == 'email_new'

def test_filter_order_by(): # orderby limit skip等功能
    User.del_node(username=["a1","a2","a3"])
    User.del_node(email='email_order')
    User.create(username='a3', email='email_order')
    User.create(username='a1', email='email_order')
    User.create(username='a2', email='email_order')
    
    nodes = User.filter_by(email='email_order')
    assert [node.username for node in nodes] == ['a3','a1','a2']
    
    nodes = User.filter_by(order_by = "username", email='email_order')
    assert [node.username for node in nodes] == ['a1','a2','a3']
    
    nodes = User.filter_by(order_by = User.username.asc(), email='email_order')
    assert [node.username for node in nodes] == ['a1','a2','a3']
    
    nodes = User.filter_by(order_by = User.username.desc(), email='email_order')
    assert [node.username for node in nodes] == ['a3','a2','a1']
    
    nodes = User.filter_by(limit=2, order_by = "username", email='email_order')
    assert len(nodes) == 2
    
    nodes = User.filter_by(skip=2, order_by = "username", email='email_order')
    assert len(nodes) == 1 and nodes[0].username == 'a3'
    
    

def test_multi_value_filter(): # 多值查询
    User.del_node(username=["a1","a2","a3"])
    User.create(username='a3', email='email_order')
    User.create(username='a2', email='email_order')
    User.create(username='a1', email='email_order')
    assert len(User.filter_by(username=["a1","a2","a3"])) == 3
    assert len(User.all(username=["a1","a2","a3"])) == 3


def test_get_file_path_by_kb_name():
    FileRecord.del_node(file_path = "test_kbname1")
    KBFileRecord.del_node(kb_name="test_kbname2")
    
    FileRecord.create(id=110, file_name = "test_file_name", file_path = "path1", file_type = "TXT", file_size = 1)
    KBFileRecord.create(kb_name="test_kbname1", file_id=110, state=FileState.PARSED)
    
    FileRecord.create(id=120, file_name = "test_file_name2", file_path = "path2", file_type = "TXT", file_size = 1)
    KBFileRecord.create(kb_name="test_kbname2", file_id=120, state=FileState.WAIT_PARSE)

    assert KBFileRecord.get_file_path_by_kb_name(kb_name="test_kbname1") == ['path1']

def test_get_file_id_by_kb_name():
    FileRecord.del_node(file_path = "path")
    KBFileRecord.del_node(kb_name="test_kbname")
    
    FileRecord.create(id=110, file_name = "test_file_name", file_path = "path", file_type = "txt", file_size = 1)
    KBFileRecord.create(kb_name="test_kbname", file_id=110, state=FileState.PARSED)
    
    FileRecord.create(id=120, file_name = "test_file_name2", file_path = "path", file_type = "txt", file_size = 1)
    KBFileRecord.create(kb_name="test_kbname", file_id=120, state=FileState.PARSED)

    KBFileRecord.get_file_id_by_kb_name(kb_name="test_kbname") == [110, 120]
    
if __name__ == "__main__": 
    
    test_del_nodes()
    test_add_node()
    test_add_or_replace_node()
    test_update()
    test_filter_order_by()
    test_multi_value_filter()
    
    test_get_file_id_by_kb_name()
    test_get_file_path_by_kb_name()
