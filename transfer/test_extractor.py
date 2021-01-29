import unittest
from unittest import TestCase

from transfer.extractor import extractor


class Testextractor(TestCase):

    def setUp(self):
        self.db_name = "c##jong"
        self.password = "guswhd12"
        self.host_name = "127.0.0.1"
        self.data_dir_path = "c:\\oratest"
        self.count_query = """ select count(*) from %s  """ % self.table_name

    # @unittest.skip("skip")
    def test_fetch(self):
        col_name = "*"
        table_name = "adid-test"
        file_name = "output_adid_test"
        execute_query = """ select %s from %s """ % (col_name, table_name)
        extractor(count_query=self.count_query, execute_query=execute_query, thread_count=4, fetch_size=1000,
                  db_name=self.db_name, password=self.password, host_name=self.host_name,
                  data_dir_path=self.data_dir_path,
                  file_name=file_name)
