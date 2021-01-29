import os
import cx_Oracle
import pandas as pd
import time as clocktime
from threading import Thread, active_count
import math


class extractor:

    def __init__(self, count_query, execute_query, thread_count, fetch_size,
                 db_name, password, host_name, data_dir_path, file_name):
        self._encoding = "UTF-8"
        self.execute_query = execute_query
        self.count_query = count_query
        self.thread_count = thread_count
        self.db_name = db_name
        self.password = password
        self.host_name = host_name
        self.data_dir_path = data_dir_path
        self.inc_file_name = file_name
        self._fetch_size = fetch_size
        self.total_rows = self.__get_total_rows__()
        self.cursor_list = self.__create_cursor_list__()
        self.column_names = self.__get_column_names__()
        self.file_list = self.__get_temp_file_list__()
        self.__merge_temp_files__()

    def __get_total_rows__(self):
        ora_connection = cx_Oracle.connect(self.db_name, self.password, self.host_name, encoding=self._encoding)
        cur = ora_connection.cursor()
        print('count statement: ' + str(self.count_query))

        cur.execute(self.count_query)
        return cur.fetchone()[0]

    def __get_connection_and_cursor__(self):
        ora_connection = cx_Oracle.connect(self.db_name, self.password, self.host_name, encoding=self._encoding)
        return [ora_connection.cursor(), ora_connection]

    def __merge_temp_files__(self):
        combined_csv = pd.concat([pd.read_csv(f) for f in self.file_list])
        csv_file = open(os.path.join(self.data_dir_path, '{}.csv').format(self.inc_file_name), "w")

        combined_csv.to_csv(csv_file, header=False, index=False, line_terminator='\n')
        csv_file.close()
        for file in self.file_list:
            os.remove(os.path.join(self.data_dir_path, '{}').format(file))

    def __get_column_names__(self):
        cur = self.__get_connection_and_cursor__()
        cur[0].execute(self.execute_query)
        col_names = [row[0] for row in cur[0].description]
        return col_names

    def __get_temp_file_list__(self):
        temp_file_path_list = []
        inc_value = 0
        thread_list = []
        for cursor in self.cursor_list:
            inc_value += 1
            filename = self.inc_file_name + str(inc_value)
            temp_file_path = os.path.join(self.data_dir_path, '{}.csv').format(filename)
            temp_file_path_list.append(temp_file_path)
            t = Thread(target=self.__fetch_each_thread__, args=(cursor, temp_file_path,))
            thread_list.append(t)
            t.start()
            while active_count() > self.thread_count:
                print('\n == 활성 스레드 수 ==: ' + str(active_count() - 1))
                clocktime.sleep(1)
        for thread in thread_list:
            thread.join()
        return temp_file_path_list

    def __fetch_each_thread__(self, cursor, temp_file_path):
        csv_file = open(temp_file_path, "w")
        n = 0
        while True:
            n = n + 1
            df = pd.DataFrame(cursor[0].fetchmany(self._fetch_size))
            if len(df) == 0:
                break
            else:
                df.to_csv(csv_file, header=False, index=False, line_terminator='\n')
        csv_file.close()
        cursor[0].close()
        cursor[1].close()

    def __create_cursor_list__(self):
        cursor_list = []
        offset = 0
        total_chunk_count = math.ceil(self.total_rows / self.thread_count)
        now_chunk = 0
        while now_chunk != self.thread_count:
            print('전체 청크: ' + str(total_chunk_count))
            print('오프셋: ' + str(offset))
            print('전체 레코드 수: ' + str(self.total_rows))
            print('청크: ' + str(now_chunk))
            now_chunk += 1
            conn = cx_Oracle.connect(self.db_name, self.password, self.host_name, encoding=self._encoding)
            cur = conn.cursor()
            print(self.execute_query + ' offset ' + str(offset) + ' rows fetch next ' + str(
                total_chunk_count) + ' rows only;')
            cur.execute(self.execute_query + ' offset ' + str(offset) + ' rows fetch next ' + str(
                total_chunk_count) + ' rows only')
            cursor_list.append([cur, conn])
            if offset < self.total_rows:
                offset = offset + total_chunk_count
            else:
                offset = self.total_rows
        return cursor_list
