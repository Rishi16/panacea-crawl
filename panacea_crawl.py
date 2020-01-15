import threading
from queue import Queue
from random import randint
import general
from general import Crawl_path
import os
import socket
import logging
import codecs
import sys
import json
import datetime
import time
import base64
import psycopg2
import pandas as pd


class spider:
    current_path = ''
    properties_file = ''
    property = ''
    input_file = ''
    input_crawled_file = ''
    final_file = ''
    proxy_file = ''
    proxies = ''
    NUMBER_OF_THREADS = 0
    num_of_attempts = 0
    time_out = 0
    queue = ''
    data_queue = ''
    input_url = []
    input_crawled_url = []
    worker_function = ''
    push_data_value = ''
    browser = []
    log_report = ""
    batch_name = ""
    host_name = ""
    team_name = ""
    logger = ""
    start_time = ""
    end_time = ""

    def __init__(self, current_path):
        self.current_path = current_path
        self.properties_file = self.current_path + '\\properties.pbf'
        self.property = general.read_properties(self.properties_file)
        self.input_file = self.current_path + '\\input_file.txt'
        self.input_crawled_file = self.current_path + '\\input_crawled.txt'
        self.final_file = self.current_path + '\\final_data.txt'
        self.proxy_file = self.current_path + '\\proxy.pbf'
        nt = self.property['number_of_threads'].split('-')
        self.NUMBER_OF_THREADS = int(nt[0])
        self.browser_persistence = int(nt[1]) if len(nt) > 1 else 0
        self.num_of_attempts = int(self.property['num_of_attempts'])
        self.time_out = int(self.property['time_out'])
        self.proxies = general.read_proxies(self.proxy_file)
        self.encoding = self.property['encoding']
        self.push_data_value = ''

        self.queue = Queue()
        self.data_queue = Queue()
        self.crawl_path = Crawl_path(self.current_path, self.num_of_attempts, self.NUMBER_OF_THREADS, self.time_out,
                                     self.proxies,
                                     self.encoding, self.get_input_headers())
        self.input_url = []
        self.input_crawled_url = []
        self.host_name = socket.gethostname()
        self.logger = logging.getLogger(str(self.host_name))

    def debug(self, debug_value):
        Crawl_path.debug = debug_value
        if len(sys.argv) > 1:
            Crawl_path.batch_id = str(sys.argv[1])
        else:
            Crawl_path.batch_id = 9999
        if len(sys.argv) > 3:
            self.batch_name = str(sys.argv[2])
            self.team_name = str(sys.argv[3])
            self.log_report = "\\\\10.100.20.40\\e$\\Panacea\\team_data\\" + str(self.team_name) + "\\Batches\\" + str(
                self.batch_name) + "\\logs\\" + str(self.host_name) + ".log"
        else:
            self.batch_name = "Test_batch"
            self.team_name = "Test_team"
            self.log_report = os.path.join(self.current_path, self.batch_name + str(".log"))
        logging.basicConfig(filename=self.log_report,
                            filemode='a',
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt='%H:%M:%S',
                            level=logging.DEBUG)

        self.logger = logging.getLogger(str(self.host_name))
        print(Crawl_path.batch_id)
        print(self.batch_name)
        print(self.team_name)
        print(self.log_report)

    def get_input_headers(self, encoding=None):
        encoding = encoding if encoding is not None else self.encoding
        with codecs.open(self.input_file, encoding=encoding) as f:
            headers = f.readline().replace('\r', '').replace('\n', '').split('\t')
        return headers

    def delete_file(self, file_path):
        if os.path.exists(file_path) and os.path.isfile(file_path):
            os.remove(file_path)

    def output_path(self, file_path):
        self.final_file = file_path

    def input_path(self, file_path):
        self.input_file = file_path

    def get_proxies_from_tool(self):
        return self.proxies

    # Create worker threads (will die when main exits)
    def create_workers(self):
        for i in range(self.NUMBER_OF_THREADS):
            t = threading.Thread(target=self.work, name='Thread-' + str(i))
            t.daemon = True
            t.start()
            # cat_threads.append(t)

    # Do the next job in the self.queue
    def work(self):
        while True:
            url = self.queue.get()
            print(str(threading.current_thread().name) + " is now crawling - " + str(url))
            try:
                self.initiate(url, self.property['region'], self.proxies, threading.current_thread().name)
            except Exception as e:
                try:
                    print(e)
                    if Crawl_path.debug == False:
                        self.push_data2('other_exception', [url])
                    else:
                        self.push_data('other_exception', [url])
                except Exception as e:
                    self.logger.error("Error in work function section-1 for thread - " + str(
                        threading.current_thread().name) + " - " + str(e))
            self.add_count()
            try:
                for browser_thread, browsers in self.crawl_path.browser.items():
                    if browser_thread == str(threading.current_thread().name):
                        if browsers['persistence'] == self.browser_persistence or self.queue.qsize() < self.NUMBER_OF_THREADS:
                            browsers['persistence'] = 0
                            browsers['proxy'] = ""
                            if browsers['driver'].service.process:
                                browsers['driver'].quit()
                        else:
                            browsers['persistence'] += 1
            except Exception as e:
                self.logger.error("Error in work function section-2 for thread - " + str(
                    threading.current_thread().name) + " - " + str(e))
            general.write_csv(self.input_crawled_file, [url])
            print(str(threading.current_thread().name) + " has completed crawling - " + str(url))
            self.queue.task_done()

    # Each self.queued link is a new job
    def create_jobs(self):
        self.logger.info("starting create jobs")
        self.logger.info("putting " + str(len(self.input_url)) + " inputs in main queue")
        for url in self.input_url:
            if url not in self.input_crawled_url:
                self.queue.put(url)
        self.logger.info("waiting for main queue to exhaust")
        self.queue.join()
        self.logger.info("main queue exhausted")

    # Check if there are items in the self.queue, if so crawl them
    def crawl(self):
        if len(self.input_url) > 0:
            print(str(len(self.input_url)) + ' link(s) in the self.queue')
            self.create_jobs()

    def push_data(self, tag, data, encoding=None, use_df=False):
        encoding = encoding if encoding is not None else str(Crawl_path.encoding)
        current_path = Crawl_path.current_path
        input_headers = Crawl_path.input_headers
        try:
            if tag.lower() == 'found':
                self.push_data_value = 'found'
                data_file_path = os.path.join(current_path, 'final_data.txt')
            elif tag.lower() == 'pnf':
                self.push_data_value = 'pnf'
                data_file_path = os.path.join(current_path, 'pnf.txt')
            elif tag.lower() == 'tag_failed':
                self.push_data_value = 'tag_failed'
                data_file_path = os.path.join(current_path, 'tag_failed.txt')
            elif tag.lower() == 'proxy_blocked':
                self.push_data_value = 'proxy_blocked'
                data_file_path = os.path.join(current_path, 'proxy_blocked.txt')
            else:
                self.push_data_value = 'other_exception'
                data_file_path = os.path.join(current_path, 'other_exception.txt')

            if Crawl_path.debug == False:
                # print("debug false")
                if tag == 'found':
                    input_headers_val = Crawl_path.output_header
                else:
                    input_headers_val = Crawl_path.input_headers
                for temp_data in data:
                    data_to_push = dict(zip(input_headers_val, temp_data))
                    data_to_push = json.dumps(data_to_push, ensure_ascii=False).replace("\u0000", "")
                    # data_to_push = data_to_push.encode(encoding)
                    if use_df:
                        temp_df = pd.DataFrame.from_records(
                            [[str(Crawl_path.batch_id), str(datetime.datetime.now()), data_to_push]],
                            columns=['Batch_id', 'Creation_date', 'Data'])
                        general.write_dataframe(temp_df, data_file_path, header=False, encoding=encoding)
                    else:
                        general.write_csv(data_file_path,
                                          [[str(Crawl_path.batch_id), str(datetime.datetime.now()), data_to_push]],
                                          encoding=encoding)
                        # write_json(data_file_path, data_to_push)
            else:
                # print("debug true")
                general.write_csv(data_file_path, data, encoding=encoding)
        except Exception as e:
            print(e)
            general.write_file(os.path.join(current_path, 'exception.txt'), "Exception in self.push_data-" + str(e))

    def get_thread_num(self, thread_name):
        return int(thread_name.split("-")[1])

    def push_data2(self, tag, data, encoding=None, use_df=False):
        try:
            while (Crawl_path.file_locker2 == True):
                time.sleep(1)
            Crawl_path.file_locker2 = True
            encoding = encoding if encoding is not None else str(Crawl_path.encoding)
            current_path = Crawl_path.current_path
            input_headers = Crawl_path.input_headers

            if tag.lower() == 'found':
                self.push_data_value = 'found'
                data_file_path = os.path.join(current_path, 'final_data.txt')
                table_name = "crawl_datasource_found"
            elif tag.lower() == 'pnf':
                self.push_data_value = 'pnf'
                data_file_path = os.path.join(current_path, 'pnf.txt')
                table_name = "crawl_datasource_pnf"
            elif tag.lower() == 'tag_failed':
                self.push_data_value = 'tag_failed'
                data_file_path = os.path.join(current_path, 'tag_failed.txt')
                table_name = "crawl_datasource_tag_failed"
            elif tag.lower() == 'proxy_blocked':
                self.push_data_value = 'proxy_blocked'
                data_file_path = os.path.join(current_path, 'proxy_blocked.txt')
                table_name = "crawl_datasource_proxy_blocked"
            else:
                self.push_data_value = 'other_exception'
                data_file_path = os.path.join(current_path, 'other_exception.txt')
                table_name = "crawl_datasource_other_exception"

            if Crawl_path.debug == False:
                # print("debug false")
                if tag == 'found':
                    input_headers_val = Crawl_path.output_header
                else:
                    input_headers_val = Crawl_path.input_headers
                for temp_data in data:
                    data_to_push = dict(zip(input_headers_val, temp_data))
                    data_to_push = json.dumps(data_to_push)
                    self.data_queue.put([data_to_push, table_name])
                    # general.write_csv(data_file_path,
                    #           [[str(Crawl_path.batch_id), str(datetime.datetime.now()), data_to_push]],
                    #           encoding=encoding)
                    # query = "INSERT INTO " + str(table_name) + " (creation_date, batch_run_id, data) VALUES (%s, %s, %s);"
                    # data = (str(datetime.datetime.now()), str(Crawl_path.batch_id), str(data_to_push))
                    # self.cur[self.get_thread_num(threading.current_thread().name)].execute(query, data)
                    # self.conn[self.get_thread_num(threading.current_thread().name)].commit()

                    # write_json(data_file_path, data_to_push)
            else:
                # print("debug true")
                general.write_csv(data_file_path, data, encoding=encoding)

            Crawl_path.file_locker2 = False
        except Exception as e:
            print(e)
            self.logger.error("Error in push_data function for thread - " + str(
                threading.current_thread().name) + " - " + str(e))

    def add_count(self, encoding=None):
        if self.push_data_value == '':
            return
        encoding = encoding if encoding is not None else str(Crawl_path.encoding)
        prod_count_path = os.path.join(self.current_path, 'crawling_status.pbf')
        while (Crawl_path.file_locker == True):
            time.sleep(1)
        Crawl_path.file_locker = True
        if self.push_data_value == 'found':
            found = 1;
            pnf = 0;
            tag_failed = 0;
            proxy_blocked = 0;
            other = 0
            data_file_path = os.path.join(self.current_path, 'final_data.txt')
        elif self.push_data_value == 'pnf':
            found = 0;
            pnf = 1;
            tag_failed = 0;
            proxy_blocked = 0;
            other = 0
            data_file_path = os.path.join(self.current_path, 'pnf.txt')
        elif self.push_data_value == 'tag_failed':
            found = 0;
            pnf = 0;
            tag_failed = 1;
            proxy_blocked = 0;
            other = 0
            data_file_path = os.path.join(self.current_path, 'tag_failed.txt')
        elif self.push_data_value == 'proxy_blocked':
            found = 0;
            pnf = 0;
            tag_failed = 0;
            proxy_blocked = 1;
            other = 0
            data_file_path = os.path.join(self.current_path, 'proxy_blocked.txt')
        elif self.push_data_value == 'other_exception':
            found = 0;
            pnf = 0;
            tag_failed = 0;
            proxy_blocked = 0;
            other = 1
            data_file_path = os.path.join(self.current_path, 'other_exception.txt')
        else:
            Crawl_path.file_locker = False
            return
        if not os.path.isfile(prod_count_path):
            data_to_write = str(found) + '\r\n' + str(pnf) + '\r\n' + str(tag_failed) + '\r\n' + str(
                proxy_blocked) + '\r\n' + str(other)
            with codecs.open(prod_count_path, 'w', encoding=encoding) as f:
                f.write(str(data_to_write) + '\r\n')
        else:
            prod_count = general.file_to_list(prod_count_path)
            data_to_write = str(int(prod_count[0]) + int(found)) + '\r\n' + str(
                int(prod_count[1]) + int(pnf)) + '\r\n' + str(int(prod_count[2]) + int(tag_failed)) + '\r\n' + str(
                int(prod_count[3]) + int(proxy_blocked)) + '\r\n' + str(int(prod_count[4]) + int(other))
            with codecs.open(prod_count_path, 'w', encoding=encoding) as f:
                f.write(str(data_to_write) + '\r\n')

        Crawl_path.file_locker = False

    def initiate(self, input_row, region, proxies_from_tool, thread_name):
        rand_num = randint(0, 2)
        if rand_num == 0:
            self.push_data("found", [[base64.decodebytes(b'SGVsbG8gdXNlci4uIQ==\n').decode("utf-8")]])
        elif rand_num == 1:
            self.push_data("found", [[base64.decodebytes(
                b'SSBhbSBhIHVuaXF1ZSBBSSBiYXNlZCBjb21wdXRlciBwcm9ncmFtLiBXaG8gaGFzIHRyYWluZWQg\naGltc2VsZiB0byB0YWxrIHRvIHlvdSBndXlz\n').decode(
                "utf-8")]])

    def ext_connect_postgre(self):
        # conn = psycopg2.connect("dbname = 'panacea' user = 'postgres' host = '192.168.4.49' password = 'eclerx##123'")
        conn = psycopg2.connect("dbname = 'panacea' user = 'postgres' host = '10.100.20.40' password = 'eclerx##123'")
        cur = conn.cursor()
        return (cur, conn)

    def store_data(self, cur, conn):
        while (True):
            data_raw = self.data_queue.get()
            data_to_push = data_raw[0]
            table_name = data_raw[1]
            query = "INSERT INTO " + str(table_name) + " (creation_date, batch_run_id, data) VALUES (%s, %s, %s);"
            data = (str(datetime.datetime.now()), str(Crawl_path.batch_id), str(data_to_push))
            cur.execute(query, data)
            conn.commit()
            self.data_queue.task_done()

    def start(self, worker_function=None, input_list=None):
        try:
            self.start_time = datetime.datetime.now()
            self.logger.info("Batch_start_time - " + str(self.start_time))
            # if worker_function is not None:
            #     self.worker_function = worker_function
            if Crawl_path.debug == False:
                self.logger.info("debug-False, creating db queue")
                # Code to implement efficient db interaction
                cur, conn = self.ext_connect_postgre()
                db_thread = threading.Thread(target=self.store_data, name='Thread-store_data', args=[cur, conn])
                db_thread.daemon = True
                db_thread.start()
                # self.data_queue.put(url)
            if not os.path.isfile(self.input_crawled_file):
                self.logger.info("creating crawled file")
                general.write_file(self.input_crawled_file, '')
                f = open(self.input_crawled_file, 'w+')
                f.close()
            self.logger.info("checking input list")
            if input_list is None:
                self.logger.info("reading new inputs from file")
                self.input_url = general.read_csv(self.input_file, skip_header=True)
            else:
                self.logger.info("reading inputs provided by user")
                self.input_url = input_list
            if str(self.property['resume_crawl']).lower() == 'off':
                self.logger.info("resume crawl off. deleting- crawling_status, pnf, proxy_blocked and tag_failed")
                self.delete_file(self.input_crawled_file)
                self.delete_file(self.current_path + '\\crawling_status.pbf')
                self.delete_file(self.current_path + '\\pnf.txt')
                self.delete_file(self.current_path + '\\proxy_blocked.txt')
                self.delete_file(self.current_path + '\\tag_failed.txt')
                self.delete_file(self.current_path + '\\other_exception.txt')
            else:
                self.logger.info("resume crawl on")
                self.input_crawled_url = general.read_csv(self.input_crawled_file)

            self.logger.info("creating workers")
            self.create_workers()
            self.logger.info("Initiating crawl")
            self.crawl()
            if Crawl_path.debug == False:
                self.logger.info("waiting for push data to db")
                self.data_queue.join()
                cur.close()
                conn.close()
            # if (self.property['crawler_type'] == 'second'):
            #     try:
            #         os.system("taskkill /im chrome.exe /f")
            #     except Exception as e:
            #         print(e)
            #     try:
            #         os.system("taskkill /im chromedriver.exe /f")
            #     except Exception as e:
            #         print(e)

            self.end_time = datetime.datetime.now()
            time_taken = self.end_time - self.start_time
            self.logger.info("Time taken to run the batch - " + str(time_taken))
            self.logger.info("Batch_end_time - " + str(self.end_time))
            print("Crawling completed successfully")
        except Exception as e:
            print(e)
            self.logger.error("Error in start method - " + str(e))
