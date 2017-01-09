#!/usr/bin/env python
# -*- coding: utf-8 -*-

from kazoo.client import KazooClient
from kazoo.protocol.states import KazooState


# デフォルトでは localhost:2181 に接続する
zk = KazooClient(hosts='127.0.0.1:2181')
zk.start()

# ZooKeeper サーバとの接続状態の変更に対するコールバック
# コールバックは別のスレッドから呼び出される点に注意する
# つまり、別のスレッドとオブジェクトを共有する場合には必ずスレッドセーフに作る
@zk.add_listener
def state_listener(state):
    if state == KazooState.LOST:
        print('LOST')
    elif state == KazooState.SUSPENDED:
        print('SUSPENDED')
    else:
        print('CONNECTED')


# 特定ノードのデータが変更されたことに対するコールバック
@zk.DataWatch('/foo/bar/baz')
def watch_node(data, stat):
    if stat:
        print('Changed!: %s, data: %s' % (stat.version, data.decode('utf-8')))


# 特定ノードの子ノードが変更されたことに対するコールバック
@zk.ChildrenWatch('/foo/bar')
def watch_children(children):
    print('Children are now: %s' % children)


if __name__ == '__main__':
    # トランザクションを開始する
    print("start trx")
    tx = zk.transaction()
    # パスが無ければ作る
    zk.ensure_path('/foo/bar')
    # ノードを作る
    zk.create('/foo/bar/baz', b'hoge')
    # トランザクションをコミットするa
    tx.commit()
    # ノード情報を取得する
    data, stat = zk.get('/foo/bar/baz')
    print('Version: %s, data: %s' % (stat.version, data.decode('utf-8')))
    # 子ノード一覧を取得する
    children = zk.get_children('/foo/bar')
    print('Children: %s' % children)
    # ノードを更新する
    zk.set('/foo/bar/baz', b'fuga')
    # ノード情報を取得する
    data, stat = zk.get('/foo/bar/baz')
    print('Version: %s, data: %s' % (stat.version, data.decode('utf-8')))
    # ノードを削除する
    zk.delete('/foo/bar/baz')
