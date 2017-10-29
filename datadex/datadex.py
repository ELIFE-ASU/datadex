# Copyright 2017 Douglas G. Moore, Harrison Smith. All rights reserved.
# Use of this source code is governed by a MIT
# license that can be found in the LICENSE file.
import os
import os.path as path
import re
import sqlite3
import hashlib
import json

def hash_directory(root_dir, hasher=None):
    """
    Hash a directory structure.
    """
    top_level = False
    if hasher is None:
        hasher = hashlib.sha256()
        cwd = os.getcwd()
        os.chdir(root_dir)
        root_dir = "."
        top_level = True
    for root, directories, filenames in os.walk(root_dir):
        for filename in filenames:
            filepath = path.normpath(path.join(root, filename))
            hasher.update(filepath.encode())
            with open(filepath, 'rb') as handle:
                hasher.update(handle.read())
        for directory in directories:
            hash_directory(path.join(root, directory), hasher=hasher)
    if top_level:
        os.chdir(cwd)
    return hasher

PARAMS_FILENAME = 'params.json'
HEADER_FILENAME = 'headers.json'

class DataDex(object):
    """
    A DataDex is a database mapping parameters to filenames.
    """
    def __init__(self, dex=None, verbose=False, hash_dir=False):
        """
        Initialize the DataDex with a database file.
        :param dex: a path to a sqlite3 database
        """
        if dex is None:
            dex = "dex.db"
        else:
            if path.exists(dex) and not path.isfile(dex):
                raise ValueError("dex is not a database file")
            elif not path.exists(dex):
                try:
                    os.makedirs(path.dirname(dex), exist_ok=True)
                except OSError:
                    pass
        self.__database = dex
        self.verbose = verbose
        self.hash_dir = hash_dir

        self.__conn = None
        self.connect()

        self.__headers = self.get_headers()
        if self.headers is not None and self.headers[-1].lower() != 'filename':
            raise RuntimeError("filename column is missing from library")

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
        if len(column_names) == 0:
            column_names = self.__parse_headers()
        else:
            column_names = list(map(lambda x: x.lower(), column_names))

        if "filename" not in column_names:
            column_names.append("filename")

        if self.headers is None:
            cursor, _ = self.get_cursor()
            column_headers = '({})'.format(','.join(column_names))
            query = "CREATE TABLE IF NOT EXISTS LIBRARY" + column_headers
            cursor.execute(query)
            self.commit()
            self.__headers = column_names
        elif self.headers != column_names:
            msg = 'library already exists with column names {}'
            raise RuntimeError(msg.format(column_headers[:-1]))

    def drop_library(self):
        """
        Drop the library from the database.
        """
        cursor, _ = self.get_cursor()
        cursor.execute("DROP TABLE IF EXISTS LIBRARY")
        self.commit()

    def reset_library(self):
        """
        Reset the library to the empty state.
        """
        try:
            self.drop_library()
        except sqlite3.OperationalError:
            pass
        cursor, _ = self.get_cursor()
        column_headers = '({})'.format(','.join(self.headers))
        query = "CREATE TABLE IF NOT EXISTS LIBRARY" + column_headers
        cursor.execute(query)
        self.commit()

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
        cursor, _ = self.get_cursor()
        try:
            cursor.execute("SELECT * FROM LIBRARY")
            headers = list(map(lambda x: x[0], cursor.description))
        except sqlite3.OperationalError:
            pass
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

    def query(self, query):
        """
        Run an arbitrary SQLite query.
        """
        cursor, _ = self.get_cursor()
        cursor.execute(query)
        return cursor.fetchall()

    def search(self, *conditions):
        """
        Query the database, returning any files that match
        """
        query = "SELECT FILENAME FROM LIBRARY"
        if len(conditions) != 0:
            query += " WHERE " + " AND ".join(conditions)
        raw_response = self.query(query)
        return list(map(lambda x: path.normpath(x[0]), raw_response))

    def lookup(self, entry=None, ignore_filename=True, enforce_null=True):
        """
        Look for an exact entry in the database
        """
        query = "SELECT * FROM LIBRARY"
        if entry is not None and len(query) != 0:
            conditions = []
            for field in entry:
                if ignore_filename and field.lower() == "filename":
                    continue
                value = entry[field]
                conditions.append("{} IS {}".format(field.upper(), repr(value)))
            if enforce_null:
                for field in self.headers:
                    if field.lower() == "filename" and ignore_filename:
                        continue
                    elif field not in entry:
                        conditions.append("{} IS NULL".format(field.upper()))
            query += " WHERE " + " AND ".join(conditions)
        return self.query(query)

    def add(self, entry, ignore_filename=True, enforce_null=True):
        """
        Add a row to the database
        """
        if len(self.lookup(entry, ignore_filename, enforce_null)) == 0:
            values = entry.values()
            fields = "({})".format(", ".join(map(lambda x: x.upper(), entry.keys())))
            values = "({})".format(", ".join(map(repr, entry.values())))
            self.query("INSERT INTO LIBRARY {} values {}".format(fields, values))
            return True
        return False

    def add_dir(self, dirname, ignore_filename=True, enforce_null=True):
        """
        Add a directory to the index.
        """
        if not path.exists(dirname):
            raise RuntimeError('"{}" does not exist'.format(dirname))
        elif not path.isdir(dirname):
            raise RuntimeError('"{}" is not a directory'.format(dirname))

        param_file = path.join(dirname, PARAMS_FILENAME)
        if not path.exists(param_file) or not path.isfile(param_file):
            return False, False

        if self.hash_dir:
            name = hash_directory(dirname).hexdigest()
            name = path.normpath(path.join(path.dirname(dirname), name))
            if path.exists(name) and len(self.lookup({"filename": name}, False, False)) != 0:
                return True, False
        else:
            name = path.normpath(dirname)

        file_found, file_added = True, False
        params = DataDex.__parse_params(param_file)
        if len(params) != 0:
            params["filename"] = name
            file_added = self.add(params, ignore_filename, enforce_null)
        else:
            msg = 'empty params file found {}'
            raise RuntimeError(msg.format(param_filepath))

        if (file_added or (file_found and not path.exists(name))) and self.hash_dir:
            os.rename(dirname, name)

        return file_found, file_added

    def index(self, root_dir, ignore_filename=True, enforce_null=True):
        """
        Index a directory
        """
        something_was_indexed = False
        for root, dirs, _ in os.walk(root_dir):
            for directory in dirs:
                dirname = os.path.join(root, directory)
                found, added = self.add_dir(dirname, ignore_filename, enforce_null)
                if self.verbose:
                    if found and added:
                        status = "indexed"
                    elif found:
                        status = "already indexed"
                    else:
                        status = "skipped (no params file)"
                    print("* Directory {} {}".format(dirname, status))
                something_was_indexed = something_was_indexed or added
        if something_was_indexed:
            self.commit()
        return something_was_indexed

    def reindex(self, root_dir, ignore_filename=True, enforce_null=True):
        """
        Reset the library and index a directory
        """
        self.reset_library()
        return self.index(root_dir, ignore_filename, enforce_null)

    def prune(self):
        """
        Remove nonexistent datasets from the library.
        """
        something_was_pruned = False
        for dataset in self.search():
            if not path.exists(dataset):
                query = 'DELETE FROM LIBRARY WHERE FILENAME = {}'
                self.query(query.format(repr(dataset)))
                something_was_pruned = True
        if something_was_pruned:
            self.commit()
        return something_was_pruned

    def __parse_headers(self):
        """
        Parse a header file
        """
        try:
            with open(HEADER_FILENAME, 'r') as header_file:
                column_names = json.load(header_file)
        except:
            column_names = []

        if len(column_names) == 0:
            raise RuntimeError("cannot create library; no headings provided")

        return column_names

    @staticmethod
    def __parse_params(filename):
        """
        Parse a parameters file.
        """
        params = dict()
        try:
            with open(filename, 'r') as params_file:
                params = json.load(params_file)
            return params
        except ValueError:
            raise ValueError('invalid JSON in "{}"'.format(filename))

