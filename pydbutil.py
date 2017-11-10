# -*- coding: utf-8 -*-
import xml.etree.ElementTree as ET
import sqlite3
from functools import reduce

class TableInfo:
    def __init__(self, tableName):
        self._tableName = tableName
        self._columns = {}

    @property
    def tableName(self):
        return self._tableName

    #@tableName.setter
    #def tableName(self, tableName):
    #    self._tableName = tableName

    @property
    def columns(self):
        return self._columns

    #@columns.setter
    #def columns(self, columns):
    #    self._columns = columns

class DBUtilException(Exception):
    def __init__(self, message):
        self._message = message

    @property
    def message(self):
        return self._message

class DBUtil:
    def __init__(self, connection):
        self._connection = connection
        self._cursor = connection.cursor()

    @property
    def cursor(self):
        return self._cursor

    def close(self):
        self._cursor.close()
        self._connection.close()

    def commit(self):
        self._connection.commit()

    def getTableInfosFromXml(self, xmlfile):
        list = []
        tree = ET.parse(xmlfile)
        root = tree.getroot()
        if root.tag != "dataset":
            message = "トップのタグ名は、{0}である必要があります。{1}".format(
                "dataset", root.tag)
            raise DBUtilException(message)
        for n in root:
            t = TableInfo(n.tag)
            for attr in n.attrib:
                t.columns[attr] = n.attrib[attr]
            list.append(t)
        return list

    def getInsertSQL(self, tableInfo):
        template = "INSERT INTO {0} ({1}) VALUES ({2})"

        keys = list(tableInfo.columns.keys())
        cols = reduce(
            lambda x, y: x + ", " + y,
            keys)
            #list(map(lambda x: ":" + x, keys)))

        values = list(tableInfo.columns.values())
        vals = reduce(
            lambda x, y: x + ", " + y,
            list(map(lambda x: "'" + x + "'", values)))

        return template.format(tableInfo.tableName, cols, vals)

    def insertTo(self, xmlfile, isClear):
        l = self.getTableInfosFromXml(xmlfile)

        if isClear:
            ti = l[0]
            tableName = ti.tableName
            sql = "DELETE FROM " + tableName
            self._cursor.execute(sql)

        sqls =  list(map(lambda x: self.getInsertSQL(x), l))
        list(map(lambda x: self._cursor.execute(x), sqls))

    def getTableInfosFromTable(self, srcTI, orderBy):
        tis = []
        template = "SELECT {0} from {1} ORDER BY {2}"

        keys = list(srcTI.columns.keys())
        cols = reduce(
            lambda x, y: x + ", " + y,
            keys)

        tableName = srcTI.tableName
        sql = template.format(cols, tableName, orderBy)
        it = self._cursor.execute(sql)
        for row in it:
            ti = TableInfo(tableName)
            cols = ti.columns
            for i, key in enumerate(keys):
                cols[key] = row[i]
            tis.append(ti)

        return tis

    def compareTo(self, xmlfile, orderBy):
        l = self.getTableInfosFromXml(xmlfile)
        tis = self.getTableInfosFromTable(l[0], orderBy)
        if len(l) != len(tis):
            message = "データ数が異なります。{0}, {1}".format(
                len(l), len(tis))
            raise DBUtilException(message)
        tableName = l[0].tableName
        keys = list(l[0].columns.keys())
        for i, srcTI in enumerate(l):
            if srcTI.tableName != tableName:
                message = "テーブル名が異なります。{0}, {1}".format(
                    tableName, srcTI.tableName)
                raise DBUtilException(message)
            dstTI = tis[i]
            srcColumns = srcTI.columns
            dstColumns = dstTI.columns
            for key in keys:
                if not key in srcColumns:
                    message = "{0}.{1}の定義が存在しません".format(
                        tableName, key)
                    raise DBUtilException(message)
                srcVal = srcColumns[key]
                dstVal = dstColumns[key]
                if str(srcVal) != str(dstVal):
                    return False
        return True
