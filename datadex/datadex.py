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

def sql_escape(old):
    """
    Escape quotations in a string in an SQLite3 compatible way.
    """
    return old.replace('"','""')


PARAMS_FILENAME = 'params.json'
HEADERS_FILENAME = 'headers.json'

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

    def __drop_tables(self):
        """
        Drop the library and header descriptions tables
        """
        self.query('DROP TABLE IF EXISTS LIBRARY')
        self.query('DROP TABLE IF EXISTS HEADERS')
        self.commit()

    def __create_tables(self, headers):
        """
        Create the library and header descriptions tables
        """
        column_names = list(headers.keys())

        column_headers = u'({})'.format(u','.join(column_names))
        self.query(u'CREATE TABLE IF NOT EXISTS LIBRARY ' + column_headers)

        self.query(u'CREATE TABLE IF NOT EXISTS HEADERS (HEADER, DESCRIPTION)')

        for header in headers:
            values = u'("{}","{}")'.format(header, sql_escape(headers[header]))
            self.query(u'INSERT INTO HEADERS (HEADER, DESCRIPTION) VALUES {}'.format(values))

        self.commit()

        return column_names

    def create_library(self, headers_filename=HEADERS_FILENAME, force=False):
        """
        Create a library with the provided parameter names
        """
        headers = DataDex.parse(headers_filename)

        if len(headers) == 0:
            raise ValueError("no column headers provided")
        elif not isinstance(headers, dict):
            raise ValueError("headers.json must be a dict")

        if 'filename' not in headers:
            headers['filename'] = 'The data directory'

        invalid_dirs = None
        if self.headers is None:
            self.__headers = self.__create_tables(headers)
        elif self.headers != list(headers.keys()) or force:
            entry_dirs = set(self.search())
            self.__drop_tables()
            self.__headers = self.__create_tables(headers)
            for dir in entry_dirs:
                try:
                    self.add_dir(dir)
                except sqlite3.OperationalError as e:
                    print(u'unexpected parameters in directory "{}" ({})'.format(dir,e))
                    if invalid_dirs is None:
                        invalid_dirs = []
                    invalid_dirs.append(dir)
        return invalid_dirs

    def query(self, query):
        """
        Run an arbitrary SQLite query.
        """
        cursor, _ = self.get_cursor()
        cursor.execute(query)
        return cursor.fetchall()

    def select(self, fields=None, conditions=None):
        """
        Query the data, returning tuples of matched entries
        """
        if fields is None or fields == [] or fields == '':
            field_query = u'*'
        elif isinstance(fields, (str,unicode)):
            field_query = fields.upper()
        else:
            field_query = u','.join(fields).upper()

        if conditions is None or conditions == [] or conditions == '':
            condition_query = u''
        elif isinstance(conditions, (str,unicode)):
            condition_query = u'WHERE ' + conditions.upper()
        else:
            condition_query = u'WHERE ' + u' AND '.join(conditions)

        query = u'SELECT {} FROM LIBRARY {}'.format(field_query, condition_query)
        return self.query(query)

    def search(self, *conditions):
        """
        Query the database, returning any files that match
        """
        if len(conditions) != 0:
            raw_response = self.select(fields="FILENAME", conditions=conditions)
        else:
            raw_response = self.select(fields="FILENAME")
        return list(map(lambda x: path.normpath(x[0]), raw_response))

    def lookup(self, entry=None, ignore_filename=True, enforce_null=True):
        """
        Look for an exact entry in the database
        """
        conditions = []
        if entry is not None:
            for field in entry:
                if ignore_filename and field.lower() == "filename":
                    continue
                value = entry[field]
                if isinstance(value, (str,unicode)):
                    conditions.append(u'{} IS "{}"'.format(field.upper(), sql_escape(value)))
                else:
                    conditions.append(u'{} IS {}'.format(field.upper(), value))
            if enforce_null:
                for field in self.headers:
                    if field.lower() == "filename" and ignore_filename:
                        continue
                    elif field not in entry:
                        conditions.append("{} IS NULL".format(field.upper()))
        return self.select(conditions=conditions)

    def add(self, entry):
        """
        Add a row to the database
        """
        if len(self.lookup(entry, True, True)) == 0:
            values = entry.values()
            fields = u'({})'.format(u', '.join(map(lambda x: x.upper(), entry.keys())))
            values = []
            for value in entry.values():
                if isinstance(value, (str,unicode)):
                    values.append('"{}"'.format(sql_escape(value)))
                else:
                    values.append(repr(value))
            values = u', '.join(values)
            self.query(u'INSERT INTO LIBRARY {} VALUES ({})'.format(fields, values))
            return True
        return False

    def add_dir(self, dirname):
        """
        Add a directory to the index.
        """
        if not path.exists(dirname):
            raise RuntimeError(u'"{}" does not exist'.format(dirname))
        elif not path.isdir(dirname):
            raise RuntimeError(u'"{}" is not a directory'.format(dirname))

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
        params = DataDex.parse(param_file)
        if len(params) != 0:
            params["filename"] = name
            file_added = self.add(params)
        else:
            msg = u'empty params file found {}'
            raise RuntimeError(msg.format(param_filepath))

        if (file_added or (file_found and not path.exists(name))) and self.hash_dir:
            os.rename(dirname, name)

        return file_found, file_added

    def index(self, root_dir='.', truncate=False):
        """
        Index a directory
        """
        if truncate:
            self.query('DELETE FROM LIBRARY')
            self.commit()
        something_was_indexed = False
        for root, dirs, _ in os.walk(root_dir):
            for directory in dirs:
                dirname = os.path.join(root, directory)
                found, added = self.add_dir(dirname)
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

    def prune(self):
        """
        Remove nonexistent datasets from the library.
        """
        something_was_pruned = False
        for dataset in self.search():
            if not path.exists(dataset):
                query = u'DELETE FROM LIBRARY WHERE FILENAME = "{}"'
                self.query(query.format(dataset))
                something_was_pruned = True
        if something_was_pruned:
            self.commit()
        return something_was_pruned

    def describe(self, header=None):
        """
        Return a description of a header
        """
        if header is None:
            return dict(self.query(u'SELECT * FROM HEADERS'))
        else:
            query =u'SELECT DESCRIPTION FROM HEADERS WHERE HEADER="{}"'.format(header)
            description = self.query(query)
            if len(description) == 0:
                return None
            elif len(description) == 1:
                return description[0][0]
            else:
                return list(map(lambda d: d[0], description))

    def has_header(self, header):
        """
        Does the index have a given header?
        """
        return header in self.headers

    @staticmethod
    def parse(filename):
        try:
            with open(filename, 'r') as file:
                return json.load(file)
        except ValueError:
            raise ValueError(u'invalid JSON in "{}"'.format(filename))
