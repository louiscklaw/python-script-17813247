#!/usr/bin/env python
# coding:utf-8


import sys
import re

# for develop
# import os
# import logging
# import traceback

from pprint import pprint
from collections import OrderedDict

# LOG_FIELD_POS
LOG_ID_POS = 0
LOG_LOGDATETIME1 = 1
LOG_LOGDATETIME2 = 2
LOG_ID1 = 3
LOG_HOSTNAME = 4
LOG_IPADDR = 5
LOG_UNKNOWN = 6
LOG_SERVERITY = 7
LOG_HEROKU_INSANCE = 8
LOG_BODY = 9


HTTP_STATUS_404_SIGNATURE = 'status=404'
HTTP_STATUS_200_SIGNATURE = 'status=200'
HTTP_STATUS_301_SIGNATURE = 'status=301'
HTTP_STATUS_302_SIGNATURE = 'status=302'

HTTP_STATUS_NEED_TO_COUNT = [
    HTTP_STATUS_404_SIGNATURE,
    HTTP_STATUS_200_SIGNATURE,
    HTTP_STATUS_301_SIGNATURE,
    HTTP_STATUS_302_SIGNATURE]

SQL_OPERATION_TYPE = ['SELECT', 'INSERT', 'UPDATE', 'DELETE']


class LogElements():
    def __init__(self, element_name, init_value=0):
        self.__element_name = element_name
        self.__value = init_value

    @property
    def value(self): return self.__value

    @value.setter
    def value(self, value): self.__value = value


class LogStatistics():
    log_has_error = []
    average_time_to_page = float('inf')
    d_heroku_instance = {}
    list_http_status = {}
    list_db_operation = {}

    def __init__(self, log_row):
        self.__log_row = log_row

    def count_logs(self):
        # process statistics
        self.count_error_log()
        self.count_different_heroku_instance()
        self.count_http_status()
        self.count_db_operation()

    def count_error_log(self):
        LogStatistics.log_has_error.append(self.log_id)

    def count_different_heroku_instance(self):
        if (self.heroku_instance in LogStatistics.d_heroku_instance.keys()):
            LogStatistics.d_heroku_instance[self.heroku_instance] += 1

        else:
            LogStatistics.d_heroku_instance[self.heroku_instance] = 1

    @staticmethod
    def initialize_stats():
        LogStatistics.list_http_status = {n: {}
                                          for n in HTTP_STATUS_NEED_TO_COUNT}

        # initialize for status 200
        LogStatistics.list_http_status[HTTP_STATUS_200_SIGNATURE] = {
            'service_call_ms': []}

    @staticmethod
    def get_heroku_instance():
        print('heroku instances:')
        for instance in LogStatistics.d_heroku_instance.keys():
            print(
                '%20s : %12s' %
                (instance, LogStatistics.d_heroku_instance[instance]))
        print('')

    @staticmethod
    def get_http_status_count():
        d_http_status_count = LogStatistics.list_http_status
        print("http status count:")

        def get_http_status_404_count():
            print('http status 404 count')
            active_count = d_http_status_count[HTTP_STATUS_404_SIGNATURE]
            for error_path, d_value in active_count.items():
                print('error http path %-48s=>count:%5d' %
                      (error_path, len(d_value['log_ids'])))

        def get_http_status_301_count():
            print('http status 301 count')
            active_count = d_http_status_count[HTTP_STATUS_301_SIGNATURE]
            for fwd_path, d_value in active_count.items():
                print('error http path %-48s=>count:%5d' %
                      (fwd_path, len(d_value['log_ids'])))

        def get_http_status_302_count():
            print('http status 302 count')
            active_count = d_http_status_count[HTTP_STATUS_302_SIGNATURE]
            for fwd_path, d_value in active_count.items():
                print('forwarding http path %-48s=>count:%5d' %
                      (fwd_path, len(d_value['log_ids'])))

        def get_http_status_200_count():
            print('http status 200 count')
            active_count = d_http_status_count[HTTP_STATUS_200_SIGNATURE]
            # average_duration=sum(active_count['service_call_ms'])/len(active_count['service_call_ms'])
            average_duration = LogStatistics.get_average_from_list(
                active_count['service_call_ms'])

            print('service call average time %.2f ' % average_duration)

        print('404 count')
        get_http_status_404_count()

        print('200 count')
        get_http_status_200_count()

        print('301/302 forwarding count')
        get_http_status_301_count()
        get_http_status_302_count()

    # 1. List of URLs that were not found (404 error), including number of
    # times each URL was requested
    def get_http_value_from_log_body(self, param_name):
        try:
            match = re.findall(r'%s=(.+?)ms ' % param_name, self.log_body)
            return match[0]
        except Exception as err:
            # cannout get the captioned parameter, return 0
            return 0

    def get_space_seperated_value(self, param_name):
        try:
            m = re.findall(r'%s=(.+?) ' % param_name, self.log_body)
            return m[0]
        except Exception as err:
            # cannout get the captioned parameter, return 0
            pprint(self.log_body)
            return 0

    def get_http_path(self):
        return self.get_space_seperated_value('path')

    def get_http_call_duration(self):
        return float(self.get_http_value_from_log_body('service'))

    def count_http_status(self):
        # status=404
        d_http_status_count = LogStatistics.list_http_status

        for http_status_loookinto in d_http_status_count.keys():
            if (self.log_body.find(http_status_loookinto) > 0):
                if http_status_loookinto in [
                        HTTP_STATUS_404_SIGNATURE,
                        HTTP_STATUS_301_SIGNATURE,
                        HTTP_STATUS_302_SIGNATURE]:
                    error_path = self.get_http_path()
                    if (error_path in d_http_status_count[http_status_loookinto].keys(
                    )):
                        d_http_status_count[http_status_loookinto][error_path]['log_ids'].append(
                            self.log_id)
                    else:
                        d_error_path = {'log_ids': [self.log_id]}
                        d_http_status_count[http_status_loookinto][error_path] = d_error_path
                if http_status_loookinto in [HTTP_STATUS_200_SIGNATURE]:
                    d_http_status_count[http_status_loookinto]['service_call_ms'].append(
                        self.get_http_call_duration())

    # 2. Average time to serve a page
    @staticmethod
    def get_average_from_list(list_input):
        """to get the average from the int list"""
        return sum(list_input) / len(list_input)

    # 3. Which database table is most frequently loaded?
    def get_SQL_table_operation(self):
        """to get the SQL operation and the table from log message"""
        # DELETE FROM "TABLE"
        # UPDATE "TABLE"
        # INSERT INTO "TABLE"
        # SELECT .+ FROM "TABLE"

        msg = self.log_body
        result = re.findall(
            r'((%s) .*?"(.+?)")' %
            '|'.join(SQL_OPERATION_TYPE), msg)
        return result

    @staticmethod
    def initialize_value_if_not_exist(target_dictionary, path, init_value=0):
        """to initialize the deep key and assign value if not exist

        Args:
            d_target: the target dictionary
            l_path: the key in terms of list

        Returns:
            The return value. True for success, False otherwise.

        """

        # return criteria
        if len(path) <= 0:
            return
        elif target_dictionary is None:
            return
        else:
            if len(path) > 1:
                active_key = path.pop(0)
                if active_key not in target_dictionary.keys():
                    target_dictionary[active_key] = {}
                LogStatistics.initialize_value_if_not_exist(
                    target_dictionary[active_key], path, init_value)
            elif len(path) == 1:
                active_key = path.pop(0)
                if active_key not in target_dictionary.keys():
                    target_dictionary[active_key] = init_value
        return

    def count_db_operation(self):
        """count the operation (SELECT/INSERT/UPDATE/DELETE) with reference to the table"""
        db_operations = self.get_SQL_table_operation()
        if db_operations is not None:
            for db_operation in db_operations:
                _, operation, table = db_operation
                self.initialize_value_if_not_exist(
                    LogStatistics.list_db_operation, [
                        table, operation])
                LogStatistics.list_db_operation[table][operation] += 1

    @staticmethod
    def get_most_frequent_loaded_table():
        # table SELECT INSERT UPDATE DELETE
        STRING_FORMAT = '%24s'
        count_db_operation = LogStatistics.list_db_operation

        def print_banner():
            print('frequency of sql table access')

        def print_header():
            for col_head in ['TABLE'] + SQL_OPERATION_TYPE:
                print(STRING_FORMAT % col_head, end='')
            print()

        def print_body():
            for table in count_db_operation.keys():
                print(STRING_FORMAT % table, end='')
                for print_sql_operation in SQL_OPERATION_TYPE:
                    if print_sql_operation in count_db_operation[table].keys():
                        print(
                            STRING_FORMAT % str(
                                count_db_operation[table][print_sql_operation]),
                            end='')
                    else:
                        print(STRING_FORMAT % '0', end='')
                print()
                # print(STRING_FORMAT % )
        print()
        print_banner()
        print_header()
        print_body()
        print()

    # 4. Is any URL redirection taking place?

    # 5. Are there any server errors? Ideas about possible causes?


class LogEntry(LogStatistics):
    """to store the log entry into python"""

    def __init__(self, log_id):
        self.log_id = log_id
        self.logdatetime1 = ''
        self.logdatetime2 = ''
        self.id1 = ''
        self.hostname = ''
        self.ipaddr = ''
        self.unknown = ''
        self.log_serverity = ''
        self.heroku_instance = ''
        self.log_body = ''

    def insert_log_body(self, log_row):
        try:
            self.logdatetime1 = log_row[LOG_LOGDATETIME1]
            self.logdatetime2 = log_row[LOG_LOGDATETIME2]
            self.id1 = log_row[LOG_ID1]
            self.hostname = log_row[LOG_HOSTNAME]
            self.ipaddr = log_row[LOG_IPADDR]
            self.unknown = log_row[LOG_UNKNOWN]
            self.log_serverity = log_row[LOG_SERVERITY]
            self.heroku_instance = log_row[LOG_HEROKU_INSANCE]
            self.log_body = log_row[LOG_BODY]

            # # statistics
            self.count_logs()

            # self.count_different_heroku_instance()

        except Exception as err:
            # pprint(log_row[LOG_ID_POS])
            raise err


class LogParser():
    def __init__(self):
        self.__parsed_log = OrderedDict()

    def get_tsv_pos(self, raw_log_line):
        try:
            output = (raw_log_line.split('\t'))
            return output

        except Exception as err:
            raise err

    def parse_all(self, log_contents):
        """parse all the log file"""
        LogStatistics.initialize_stats()

        try:
            for log_content in log_contents:
                log_values = self.get_tsv_pos(log_content)
                log_values = list(log_values)

                log_id = log_values[LOG_ID_POS]
                new_log_entry = LogEntry(log_id)
                new_log_entry.insert_log_body(log_values)

                self.__parsed_log[log_id] = new_log_entry

        except Exception as err:
            pprint(log_content)
            raise err

        return self.__parsed_log

    def get_statistics(self):
        """output final statistics."""

        print('statistics :')
        LogStatistics.get_heroku_instance()
        LogStatistics.get_http_status_count()
        LogStatistics.get_most_frequent_loaded_table()


def main():
    """main function of workable_log_parse.py.

    Args:
        sys.argv[1]: workable log file to parse

    """

    temp_logparser = LogParser()
    with open(sys.argv[1], 'r') as f_log_fle:
        raw_log_lines = f_log_fle.readlines()
        temp_logparser.parse_all(raw_log_lines)

    # pprint(parsed_entries.keys())

    temp_logparser.get_statistics()

    print('done')


if __name__ == '__main__':
    main()
