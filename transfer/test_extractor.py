import unittest
from unittest import TestCase

from transfer.extractor import extractor


class Testextractor(TestCase):

    def setUp(self):
        self.db_name = "c##jong"
        self.password = "guswhd12"
        self.host_name = "127.0.0.1"
        self.data_dir_path = "c:\\oratest"


    # @unittest.skip("skip")
    def test_fetch_138MB_4000_1THREAD(self):
        col_name = "*"
        table_name = "adid_test"
        file_name = "output_adid_test"
        execute_query = """ select %s from %s """ % (col_name, table_name)
        count_query = """ select count(*) from %s  """ % table_name
        extractor(count_query=count_query, execute_query=execute_query, thread_count=1, fetch_size=4000,
                  db_name=self.db_name, password=self.password, host_name=self.host_name,
                  data_dir_path=self.data_dir_path,
                  file_name=file_name)

    def test_fetch_1GB_4000_1THREAD(self):
        col_name = "*"
        table_name = "adid_test_1GB"
        file_name = "output_%s" % table_name
        execute_query = """ select %s from %s """ % (col_name, table_name)
        count_query = """ select count(*) from %s  """ % table_name
        extractor(count_query=count_query, execute_query=execute_query, thread_count=1, fetch_size=1000,
                  db_name=self.db_name, password=self.password, host_name=self.host_name,
                  data_dir_path=self.data_dir_path,
                  file_name=file_name)



