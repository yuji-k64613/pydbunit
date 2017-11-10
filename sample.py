# -*- coding: utf-8 -*-
import sqlite3
import unittest
from pydbutil import DBUtil

def createTalbe(util):
    cur = util.cursor
    cur.execute("DROP TABLE IF EXISTS sample")
    cur.execute(
        "CREATE TABLE IF NOT EXISTS sample (id INTEGER PRIMARY KEY, name TEXT)")

class TestFoo(unittest.TestCase):
    def setUp(self):
        # データベースファイルのパス
        dbpath = './sample_db.sqlite'
        # データベース接続とカーソル生成
        connection = sqlite3.connect(dbpath)

        # DBUtil作成
        self.util = DBUtil(connection)

        # テーブル作成
        createTalbe(self.util)

    def tearDown(self):
        # クローズ
        self.util.close()

    def test_foo001(self):
        # データ投入
        self.util.insertTo('./sample.xml', True)

        # コミット
        self.util.commit()

        #################################
        # テスト実施
        #################################

        # 内容確認(DEBUG)
        #cur = self.util.cursor
        #it = cur.execute("select * from sample")
        #for row in it:
        #    print(row)

        # 比較
        ret = self.util.compareTo('./compare.xml', "id")
        self.assertEqual(True, ret, "テーブルとの比較")

    def test_foo002(self):
        pass
