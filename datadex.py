# Copyright 2017 Douglas G. Moore, Harrison Smith. All rights reserved.
# Use of this source code is governed by a MIT
# license that can be found in the LICENSE file.
import os
import os.path as path
import sqlite3

class DataDex(object):
    """
    A DataDex is a database mapping parameters to filenames.
    """
    def __init__(self, dex):
        """
        Initialize the DataDex with a database file.
        :param dex: a path to a sqlite3 database
        """
        if path.exists(dex) and not path.isfile(dex):
            raise ValueError("dex is not a database file")
        elif not path.exists(dex):
            try:
                os.makedirs(path.dirname(dex), exist_ok=True)
            except OSError:
                pass
        self.__database = dex
        self.__conn = None
        self.connect()

        self.__headers = self.get_headers()
        if self.headers is not None and self.headers[-1].lower() != 'filename':
            raise RuntimeError("filename column is missing from library")

    def __dealloc__(self):
        """
        Deallocate the DataDex.
        """
        if self.is_connected():
            self.commit()
            self.disconnect()

    def connect(self):
        """
        Connect to the database.
        """
        self.__conn = sqlite3.connect(self.__database)

    def is_connected(self):
        """
        Is the database connected?
        """
        return self.__conn is not None

    def disconnect(self):
        """
        Disconnect from the database.
        """
        if self.is_connected():
            self.__conn.close()
            self.__conn = None

    def create_library(self, *column_names):
        """
        Create a library with the provided parameter names
        """
        column_names = list(map(lambda x: x.lower(), column_names))
        column_names.append("filename")
        if self.headers is None:
            cursor, disconnect_after = self.get_cursor()
            column_headers = '({})'.format(','.join(column_names))
            query = "CREATE TABLE IF NOT EXISTS LIBRARY" + column_headers
            cursor.execute(query)
            self.commit()
            self.__headers = column_names
            if disconnect_after:
                self.disconnect()
        elif self.headers != column_names:
            msg = 'library already exists with column names {}'
            raise RuntimeError(msg.format(column_headers[:-1]))

    @property
    def headers(self):
        """
        Cache the headers from the library and return them.
        """
        if self.__headers is None:
            self.__headers = self.get_headers()
        return self.__headers

    def get_headers(self):
        """
        Get the headers from the library.
        """
        headers = None
        cursor, disconnect_after = self.get_cursor()
        try:
            cursor.execute("SELECT * FROM LIBRARY")
            headers = list(map(lambda x: x[0], cursor.description))
        except sqlite3.OperationalError:
            pass
        if disconnect_after:
            self.disconnect()
        return headers

    def get_cursor(self):
        """
        Get a cursor into the library.
        """
        established_connection = False
        if not self.is_connected():
            self.connect()
            established_connection = True
        return self.__conn.cursor(), established_connection

    def commit(self):
        """
        Save any changes made to the library.
        """
        if self.is_connected():
            self.__conn.commit()

    def search(self, *conditions):
        """
        Query the database, returning any files that match
        """
        query = "SELECT FILENAME FROM LIBRARY WHERE " + " AND ".join(conditions)
        print(query)
        cursor, disconnect_after = self.get_cursor()
        cursor.execute(query)
        filenames = list(map(lambda x: x[0], cursor.fetchall()))
        if disconnect_after:
            self.disconnect()
        return filenames

    def add(self, entry):
        """
        Add a row to the database
        """
        values = entry.values()
        fields = "({})".format(", ".join(map(lambda x: x.upper(), entry.keys())))
        values = "({})".format(", ".join(map(repr, entry.values())))
        query = "INSERT INTO LIBRARY {} values {}".format(fields, values)
        print(query)
        cursor, disconnect_after = self.get_cursor()
        cursor.execute(query)
        if disconnect_after:
            self.disconnect()
