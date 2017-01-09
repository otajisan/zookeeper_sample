# coding:utf-8
from kazoo.client import KazooClient
from kazoo.client import KazooState
from kazoo.client import KeeperState
from kazoo.handlers.gevent import SequentialGeventHandler
import logging
logging.basicConfig()

# znodeのルート
root = '/root'

# zookeeperクライアント
zk = KazooClient(hosts='127.0.0.1:2181', read_only=True, handler=SequentialGeventHandler())

# 非同期モードで起動
event = zk.start_async()
event.wait(timeout=3)

# zookeeperサーバに接続できない場合、処理を中断
if not zk.connected:
    zk.stop()
    raise Exception("Unable to connect.")

def listener(state):
    '''
    State変更時のリスナー
    '''
    print('current state is ' + state)

zk.add_listener(listener)

@zk.add_listener
def watch_for_ro(state):
    if state == KazooState.CONNECTED:
        if zk.client_state == KeeperState.CONNECTED_RO:
            print('state is read_only')
        else:
            print('state is writable')

def print_status(znode):
    '''
    ノードの状態を取得
    '''
    print('#####[DEBUG]#####')
    # バージョンと登録データを確認
    data, stat = zk.get(znode)
    print('Version: %s, data: %s' % (stat.version, data.decode('utf-8')))
    # rootの子ノード一覧を取得
    children = zk.get_children(root)
    print("There are %s children with names %s" % (len(children), children))

def doAsync(async_obj):
    '''
    非同期処理のコールバック関数(処理内容に特に意味は無い)
    '''
    znodes = async_obj.get()
    try:
        children = async_obj.get()
        # すべての子ノードの名称を出力
        print('#####[print child znodes]#####')
        for child in children:
            print(child)
    except (ConnectionLossException, NoAuthException):
        print("ERROR!!!")
        sys.exit(1)

if __name__ == '__main__':
    # トランザクションの開始
    tx = zk.transaction()
    ## 基本的な使い方を確認
    # パスの生成
    zk.ensure_path(root)
    # znodeが未作成であれば作成
    znode = root + '/sample_znode'
    if zk.exists(znode) is None:
        zk.create(znode, b'sample_data')
    print_status(znode)
    # データの更新
    zk.set(znode, b'updated_data')
    print_status(znode)
    # 子ノードの追加
    znode2 = root + '/sample_znode2'
    if zk.exists(znode2) is None:
        zk.create(znode2, b'sample_data2')
    print_status(znode2)
    ## 非同期処理はこうやって使う
    async_obj = zk.get_children_async(root)
    async_obj.rawlink(doAsync)
    # ノードの削除
    zk.delete(root, recursive=True)
    # コミット
    results = tx.commit()
    print('#####[Result]#####')
    print(results)
