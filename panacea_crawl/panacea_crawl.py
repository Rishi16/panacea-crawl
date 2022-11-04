import base64
import codecs
import datetime
import gc
import json
import logging
import os
import shutil
import socket
import sys
import threading
import time
from queue import Queue, Empty
from random import randint

import psutil

try:
    import psycopg2
except Exception:
    pass

import panacea_crawl.general as general
from panacea_crawl.general import Crawl_path

plog = False


class plogger:
    def __init__(self, logger):
        self.logit = logger

    def info(self, log):
        if plog:
            self.logit.info(log)

    def error(self, log):
        if plog:
            self.logit.error(log)


class Spider:
    master = ""
    common_path = ""
    current_path = ""
    properties_file = ""
    property = ""
    input_file = ""
    input_crawled_file = ""
    final_file = ""
    proxy_file = ""
    proxies = ""
    NUMBER_OF_THREADS = 0
    num_of_attempts = 0
    time_out = 0
    queue = ""
    data_queue = ""
    input_url = []
    input_crawled_url = []
    worker_function = ""
    push_data_value = {}
    browser = []
    log_report = ""
    batch_name = ""
    host_name = ""
    host_address = ""
    team_name = ""
    start_time = ""
    end_time = ""
    tag_failed_recrawl = False
    proxy_blocked_recrawl = False

    def __init__(self, current_path, object=None):
        if object:
            self.master = object.master
            self.common_path = object.common_path
            self.current_path = object.current_path
            self.properties_file = object.properties_file
            self.property = object.property
            self.input_file = object.input_file
            self.input_crawled_file = object.input_crawled_file
            self.final_file = object.final_file
            self.proxy_file = object.proxy_file
            self.tag_failed_file = object.tag_failed_file
            self.proxy_blocked_file = object.proxy_blocked_file
            self.NUMBER_OF_THREADS = object.NUMBER_OF_THREADS
            self.browser_persistence = object.browser_persistence
            self.num_of_attempts = object.num_of_attempts
            self.time_out = object.time_out
            self.proxies = object.proxies
            self.encoding = object.encoding
            self.push_data_value = object.push_data_value
            self.queue = object.queue
            self.data_queue = object.data_queue
            self.crawl_path = Crawl_path(
                self.current_path,
                self.num_of_attempts,
                self.NUMBER_OF_THREADS,
                self.time_out,
                self.proxies,
                self.encoding,
                self.get_input_headers(),
                self.browser_persistence,
            )
            self.crawl_path.master = self.master
            self.input_url = object.input_url
            self.input_crawled_url = object.input_crawled_url
            self.host_name = object.host_name
            self.host_address = object.host_address
            self.input_crawled_lock = object.input_crawled_lock
            self.push_data_lock = object.push_data_lock
            self.crawling_status_lock = object.crawling_status_lock
            self.property_lock = object.property_lock
            self.tag_failed_recrawl = object.tag_failed_recrawl
            self.proxy_blocked_recrawl = object.proxy_blocked_recrawl
            self.crawling_status_first = object.crawling_status_first
            self.found = object.found
            self.tag_failed = object.tag_failed
            self.other = object.other
            self.proxy_blocked = object.proxy_blocked
            self.pnf = object.pnf
        else:
            self.master = ""
            self.common_path = ""
            self.current_path = current_path
            self.properties_file = self.current_path + "\\properties.pbf"
            self.property = general.read_properties(self.properties_file)
            for key in ["completed", "stop", "paused"]:
                if key in self.property:
                    del self.property[key]
                    general.write_properties(self.properties_file, self.property)

            self.input_file = self.current_path + "\\input_file.txt"
            self.input_crawled_file = self.current_path + "\\input_crawled.txt"
            self.final_file = self.current_path + "\\final_data.txt"
            self.proxy_file = self.current_path + "\\proxy.pbf"
            self.tag_failed_file = self.current_path + "\\tag_failed.txt"
            self.proxy_blocked_file = self.current_path + "\\proxy_blocked.txt"
            nt = self.property["number_of_threads"].split("-")
            self.NUMBER_OF_THREADS = int(nt[0])
            self.browser_persistence = int(nt[1]) if len(nt) > 1 else 100
            self.num_of_attempts = int(self.property["num_of_attempts"])
            self.time_out = int(self.property["time_out"])
            self.proxies = general.read_proxies(self.proxy_file)
            self.encoding = self.property["encoding"]
            self.push_data_value = {}

            self.queue = Queue()
            self.data_queue = Queue()
            self.crawl_path = Crawl_path(
                self.current_path,
                self.num_of_attempts,
                self.NUMBER_OF_THREADS,
                self.time_out,
                self.proxies,
                self.encoding,
                self.get_input_headers(),
                self.browser_persistence,
            )
            self.input_url = []
            self.input_crawled_url = []
            self.host_name = socket.gethostname()
            self.host_address = socket.gethostbyname(socket.gethostname())
            self.input_crawled_lock = threading.Lock()
            self.push_data_lock = threading.Lock()
            self.crawling_status_lock = threading.Lock()
            self.property_lock = threading.Lock()
            self.tag_failed_recrawl = self.proxy_blocked_recrawl = False
            self.crawling_status_first = True
            self.found = (
                self.tag_failed
            ) = self.other = self.proxy_blocked = self.pnf = 0
            print("PID:", os.getpid())

    def proxy_mode(self, proxy_mode):
        Crawl_path.proxy_mode = proxy_mode
        # if proxy_mode == "sequential_rotational":
        #     for i in range(len(self.proxies)):
        #         Crawl_path.working_proxies[i] = ''

    def print_requests(self, mode):
        Crawl_path.print_requests = mode

    def cooling_period(self, period):
        Crawl_path.cooling_period = period

    def cache(self):
        Crawl_path.cache_path = general.check_create_dir(
            os.path.join(Crawl_path.current_path, "cache")
        )

    def debug(self, debug_value):
        Crawl_path.debug = debug_value
        if len(sys.argv) > 1:
            Crawl_path.batch_id = str(sys.argv[1])
        else:
            Crawl_path.batch_id = 9999
        if len(sys.argv) > 3:
            Crawl_path.debug = False
            self.batch_name = str(sys.argv[2])
            self.team_name = str(sys.argv[3])
            self.master = str(sys.argv[4])
            Crawl_path.master = self.master
            Crawl_path.batch_name = self.batch_name
            Crawl_path.team_name = self.team_name
            # self.log_report = "\\\\" + self.master + "\\e$\\Panacea\\team_data\\" + str(self.team_name) + "\\Batches\\" + str(
            #     self.batch_name) + "\\logs\\" + str(self.host_name) + ".log"
            self.log_report = os.path.join(
                self.current_path, self.batch_name + str(".log")
            )
            self.common_path = self.current_path
        else:
            Crawl_path.debug = True
            self.batch_name = "Test_batch"
            self.team_name = "Test_team"
            self.log_report = os.path.join(
                self.current_path, self.batch_name + str(".log")
            )
            self.common_path = self.current_path
        self.logger = plogger("")
        if plog:
            logging.basicConfig(
                filename=self.log_report,
                filemode="a",
                format="%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s",
                datfmt="%H:%M:%S",
                level=logging.DEBUG,
            )

            self.logger = plogger(logging.getLogger(str(self.host_name)))
        # print(Crawl_path.batch_id)
        # print(self.batch_name)
        # print(self.team_name)
        # print(self.log_report)

    def get_input_headers(self, encoding=None):
        encoding = encoding if encoding is not None else self.encoding
        with codecs.open(self.input_file, encoding=encoding) as f:
            headers = f.readline().replace("\r", "").replace("\n", "").split("\t")
        f.close()
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
            t = threading.Thread(target=self.work, name="Thread-" + str(i))
            t.daemon = True
            t.start()
            self.push_data_value["Thread-" + str(i)] = ""
            # cat_threads.append(t)

    # Do the next job in the self.queue
    def work(self):
        while True:
            self.property = general.read_properties(self.properties_file)
            if "stop" in self.property:
                if self.property["stop"] == "1":
                    try:
                        if (
                            str(threading.current_thread().name)
                            in self.crawl_path.browser
                        ):
                            browser = self.crawl_path.browser[
                                str(threading.current_thread().name)
                            ]
                            if browser["driver"].service.process:
                                general.close_chrome(
                                    browser["driver"], browser["profile_path"]
                                )
                            del self.crawl_path.browser[
                                str(threading.current_thread().name)
                            ]
                    except Exception as e:
                        print(e)
                    self.logger.info(
                        "properties.pbf: stop is on. exausting the input queue."
                    )
                    print("properties.pbf: stop is on")
                    while not self.queue.empty():
                        try:
                            self.queue.get(False)
                        except Empty:
                            continue
                        self.queue.task_done()
                    break
            self.property_lock.acquire()
            if "proxy_update" in self.property:
                try:
                    del self.property["proxy_update"]
                    general.write_properties(self.properties_file, self.property)
                    self.proxies = general.read_proxies(self.proxy_file)
                except Exception as e:
                    print(str(e))
            self.property_lock.release()
            url = self.queue.get()
            if Crawl_path.debug:
                print(
                    str(threading.current_thread().name)
                    + " is now crawling - "
                    + str(url)
                )
            try:
                self.initiate(
                    url,
                    self.property["region"],
                    self.proxies,
                    threading.current_thread().name,
                )
                gc.collect()
            except Exception as e:
                try:
                    general.write_file(
                        "panacea_errors.txt", str(general.get_error_line(e))
                    )
                    self.push_data("other_exception", [url])
                except Exception as e:
                    print(e)
                    general.write_file(
                        "panacea_errors.txt", str(general.get_error_line(e))
                    )
                    self.logger.error(
                        "Error in work function section-1 for thread - "
                        + str(threading.current_thread().name)
                        + " - "
                        + str(e)
                    )
            self.add_count()
            try:
                if str(threading.current_thread().name) in self.crawl_path.browser:
                    browser = self.crawl_path.browser[
                        str(threading.current_thread().name)
                    ]
                    if (
                        browser["persistence"] == self.crawl_path.browser_persistence
                        or self.queue.qsize() < self.NUMBER_OF_THREADS
                    ):
                        if browser["driver"].service.process:
                            general.close_chrome(
                                browser["driver"], browser["profile_path"]
                            )
                        del self.crawl_path.browser[
                            str(threading.current_thread().name)
                        ]
            except Exception as e:
                print(e)
                general.write_file("panacea_errors.txt", str(general.get_error_line(e)))
                self.logger.error(
                    "Error in work function section-2 for thread - "
                    + str(threading.current_thread().name)
                    + " - "
                    + str(e)
                )
            self.input_crawled_lock.acquire()
            general.write_csv(self.input_crawled_file, [url])
            self.input_crawled_lock.release()
            if Crawl_path.debug:
                print(
                    str(threading.current_thread().name)
                    + " has completed crawling - "
                    + str(url)
                )
            self.queue.task_done()

    # Each self.queued link is a new job
    def create_jobs(self):
        self.logger.info("starting create jobs")
        self.logger.info(
            "putting " + str(len(self.input_url)) + " inputs in main queue"
        )
        for url in self.input_url:
            if url not in self.input_crawled_url:
                self.queue.put(url)
        self.logger.info("waiting for input_file input to exhaust")
        del self.input_url
        self.queue.join()
        if "stop" in self.property:
            for k, v in self.crawl_path.browser.items():
                try:
                    if "driver" in v:
                        if v["driver"].service.process:
                            general.close_chrome(v["driver"], v["profile_path"])
                except Exception as e:
                    print(e)
            if self.property["stop"] == "1":
                del self.property["stop"]
                self.property["paused"] = 1
                general.write_properties(self.properties_file, self.property)
                print("Force Stopped Crawl.")
                sys.exit(0)
        recrawl = False
        try:
            if self.property["recrawl"] == "True":
                recrawl = True
            elif bool(int(self.property["recrawl"])):
                recrawl = True
        except:
            pass
        try:
            if recrawl:
                self.tag_failed_recrawl = True
                if os.path.exists(self.tag_failed_file):
                    for url in general.read_csv(self.tag_failed_file, skip_header=True):
                        self.queue.put(url)
                    self.logger.info("waiting for tag_failed input to exhaust")
                    self.queue.join()
                if os.path.exists(self.proxy_blocked_file):
                    self.proxy_blocked_recrawl = True
                    self.tag_failed_recrawl = False
                    for url in general.read_csv(
                        self.proxy_blocked_file, skip_header=True
                    ):
                        self.queue.put(url)
                    self.logger.info("waiting for proxy_blocked input to exhaust")
                    self.queue.join()
            shutil.rmtree(os.path.join(self.current_path, "selenium"))
        except Exception as e:
            pass
            # print(e)
        self.property["completed"] = 1
        general.write_properties(self.properties_file, self.property)
        self.logger.info("queue exhausted")

    #
    # Check if there are items in the self.queue, if so crawl them
    def crawl(self):
        if len(self.input_url) > 0:
            print(str(len(self.input_url)) + " link(s) in the self.queue")
            self.create_jobs()

    def push_data(self, tag, data, encoding=None, use_df=False):
        self.push_data2(tag, data, encoding, use_df)

    def get_thread_num(self, thread_name):
        return int(thread_name.split("-")[1])

    def push_data2(self, tag, data, encoding=None, use_df=False):
        try:
            encoding = encoding if encoding is not None else str(Crawl_path.encoding)
            current_path = Crawl_path.current_path
            if tag.lower() == "found":
                self.push_data_value[threading.current_thread().name] = "found"
                data_file_path = os.path.join(current_path, "final_data.txt")
                table_name = "crawl_datasource_found"
            elif tag.lower() == "pnf":
                self.push_data_value[threading.current_thread().name] = "pnf"
                data_file_path = os.path.join(current_path, "pnf.txt")
                table_name = "crawl_datasource_pnf"
            elif tag.lower() == "tag_failed":
                self.push_data_value[threading.current_thread().name] = "tag_failed"
                data_file_path = os.path.join(current_path, "tag_failed.txt")
                table_name = "crawl_datasource_tag_failed"
            elif tag.lower() == "proxy_blocked":
                self.push_data_value[threading.current_thread().name] = "proxy_blocked"
                data_file_path = os.path.join(current_path, "proxy_blocked.txt")
                table_name = "crawl_datasource_proxy_blocked"
            elif tag.lower() == "other_exception":
                self.push_data_value[
                    threading.current_thread().name
                ] = "other_exception"
                data_file_path = os.path.join(current_path, "other_exception.txt")
                table_name = "crawl_datasource_other_exception"
            else:
                data_file_path = os.path.join(current_path, f"{tag}.txt")
                table_name = None
            if not Crawl_path.debug and table_name:
                # print("debug false")
                if tag == "found":
                    input_headers_val = Crawl_path.output_header
                else:
                    input_headers_val = Crawl_path.input_headers
                for temp_data in data:
                    data_to_push = dict(zip(input_headers_val, temp_data))
                    data_to_push = json.dumps(data_to_push)
                    self.data_queue.put([data_to_push, table_name])
                if tag in ["proxy_blocked", "tag_failed"]:
                    self.push_data_lock.acquire()
                    general.write_csv(data_file_path, data, encoding=encoding)
                    self.push_data_lock.release()
            else:
                # print("debug true")
                self.push_data_lock.acquire()
                general.write_csv(data_file_path, data, encoding=encoding)
                self.push_data_lock.release()

        except Exception as e:
            print(e)
            self.logger.error(
                "Error in push_data function for thread - "
                + str(threading.current_thread().name)
                + " - "
                + str(e)
            )

    def add_count(self, encoding=None):
        if self.push_data_value[threading.current_thread().name] == "":
            self.push_data_value[threading.current_thread().name] = "other_exception"
        encoding = encoding if encoding is not None else str(Crawl_path.encoding)
        crawling_status_path = os.path.join(self.current_path, "crawling_status.pbf")
        self.crawling_status_lock.acquire()
        try:
            if self.push_data_value[threading.current_thread().name] == "found":
                self.found += 1
                if self.tag_failed_recrawl:
                    self.tag_failed -= 1
                if self.proxy_blocked_recrawl:
                    self.proxy_blocked -= 1
            elif self.push_data_value[threading.current_thread().name] == "pnf":
                if self.tag_failed_recrawl:
                    self.tag_failed -= 1
                if self.proxy_blocked_recrawl:
                    self.proxy_blocked -= 1
                self.pnf += 1
            elif self.push_data_value[threading.current_thread().name] == "tag_failed":
                if not self.tag_failed_recrawl:
                    self.tag_failed += 1
                if self.proxy_blocked_recrawl:
                    self.proxy_blocked -= 1
            elif (
                self.push_data_value[threading.current_thread().name] == "proxy_blocked"
            ):
                if self.tag_failed_recrawl:
                    self.tag_failed -= 1
                if not self.proxy_blocked_recrawl:
                    self.proxy_blocked += 1
            elif (
                self.push_data_value[threading.current_thread().name]
                == "other_exception"
            ):
                if self.tag_failed_recrawl:
                    self.tag_failed -= 1
                if self.proxy_blocked_recrawl:
                    self.proxy_blocked -= 1
                self.other += 1
            else:
                return
            if not os.path.isfile(crawling_status_path):
                self.crawling_status_first = False
                data_to_write = (
                    str(self.found)
                    + "\n"
                    + str(self.pnf)
                    + "\n"
                    + str(self.tag_failed)
                    + "\n"
                    + str(self.proxy_blocked)
                    + "\n"
                    + str(self.other)
                    + "\n"
                )
                with open(crawling_status_path, "w") as f:
                    f.write(str(data_to_write))
                f.close()
                if not Crawl_path.debug:
                    with open(crawling_status_path + "2", "w") as f:
                        f.write(str(data_to_write))
                    f.close()
            else:
                if self.crawling_status_first:
                    self.crawling_status_first = False
                    prod_count = []
                    try:
                        prod_count = general.file_to_list(crawling_status_path)
                    except Exception as e:
                        general.write_file(
                            "panacea_errors.txt", str(general.get_error_line(e))
                        )
                    if len(prod_count) >= 5:
                        try:
                            self.found += int(prod_count[0])
                            self.pnf += int(prod_count[1])
                            self.tag_failed += int(prod_count[2])
                            self.proxy_blocked += int(prod_count[3])
                            self.other += int(prod_count[4])
                        except:
                            self.read_crawling_status2(crawling_status_path)
                    else:
                        self.read_crawling_status2(crawling_status_path)
                self.crawling_status_first = False
                data_to_write = (
                    str(self.found)
                    + "\n"
                    + str(self.pnf)
                    + "\n"
                    + str(self.tag_failed)
                    + "\n"
                    + str(self.proxy_blocked)
                    + "\n"
                    + str(self.other)
                    + "\n"
                )
                with open(crawling_status_path, "w") as f:
                    f.write(str(data_to_write))
                f.close()
                if not Crawl_path.debug:
                    with open(crawling_status_path + "2", "w") as f:
                        f.write(str(data_to_write))
                    f.close()

        except Exception as e:
            general.write_file("panacea_errors.txt", str(e))
        self.crawling_status_lock.release()

    def read_crawling_status2(self, crawling_status_path):
        if not Crawl_path.debug:
            try:
                prod_count = general.file_to_list(crawling_status_path + "2")
                if len(prod_count) >= 5:
                    self.found += int(prod_count[0])
                    self.pnf += int(prod_count[1])
                    self.tag_failed += int(prod_count[2])
                    self.proxy_blocked += int(prod_count[3])
                    self.other += int(prod_count[4])
            except Exception as e:
                general.write_file("panacea_errors.txt", str(e))

    def initiate(self, input_row, region, proxies_from_tool, thread_name):
        rand_num = randint(0, 2)
        if rand_num == 0:
            self.push_data(
                "found",
                [[base64.decodebytes(b"SGVsbG8gdXNlci4uIQ==\n").decode("utf-8")]],
            )
        elif rand_num == 1:
            self.push_data(
                "found",
                [
                    [
                        base64.decodebytes(
                            b"SSBhbSBhIHVuaXF1ZSBBSSBiYXNlZCBjb21wdXRlciBwcm9ncmFtLiBXaG8gaGFzIHRyYWluZWQg\naGltc2VsZiB0byB0YWxrIHRvIHlvdSBndXlz\n"
                        ).decode("utf-8")
                    ]
                ],
            )

    def ext_connect_postgre(self):
        conn = psycopg2.connect(
            "dbname = 'panacea' user = 'postgres' host = "
            + self.master
            + " password = 'eclerx##123'"
        )
        cur = conn.cursor()
        return (cur, conn)

    def store_data(self, cur, conn):
        while True:
            data_raw = self.data_queue.get()
            data_to_push = data_raw[0]
            table_name = data_raw[1]
            query = (
                "INSERT INTO "
                + str(table_name)
                + " (creation_date, batch_run_id, data) VALUES (%s, %s, %s);"
            )
            data = (
                str(datetime.datetime.now()),
                str(Crawl_path.batch_id),
                str(data_to_push),
            )
            cur.execute(query, data)
            conn.commit()
            self.data_queue.task_done()

    def monitor_memory(self):
        try:
            while True:
                time.sleep(5)
                p = psutil.Process(os.getpid())
                m = p.memory_info().rss
                for c in p.children(recursive=True):
                    m += c.memory_info().rss
                t = m / float(2 ** 30)
                cpu = p.cpu_percent()
                for c in p.children(recursive=True):
                    cpu += c.cpu_percent()
                with open("memory.log", "w") as fw:
                    fw.write(f"{str(round(t, 2))} GB\t{cpu}%")
                    fw.close()
        except:
            pass

    def start(self, worker_function=None, input_list=None):
        try:
            self.start_time = datetime.datetime.now()
            self.logger.info("Batch_start_time - " + str(self.start_time))
            if not Crawl_path.debug:
                self.logger.info("debug-False, creating db queue")
                cur, conn = self.ext_connect_postgre()
                db_thread = threading.Thread(
                    target=self.store_data, name="Thread-store_data", args=[cur, conn]
                )
                db_thread.daemon = True
                db_thread.start()
                mm_thread = threading.Thread(
                    target=self.monitor_memory, name="Thread-monitor_memory"
                )
                mm_thread.daemon = True
                mm_thread.start()
                # self.data_queue.put(url)
            if not os.path.isfile(self.input_crawled_file):
                self.logger.info("creating crawled file")
                general.write_file(self.input_crawled_file, "")
                f = open(self.input_crawled_file, "w+")
                f.close()
            self.logger.info("checking input list")
            if input_list is None:
                self.logger.info("reading new inputs from file")
                self.input_url = general.read_csv(self.input_file, skip_header=True)
            else:
                self.logger.info("reading inputs provided by user")
                self.input_url = input_list
            if str(self.property["resume_crawl"]).lower() == "off":
                self.logger.info(
                    "resume crawl off. deleting- crawling_status, pnf, proxy_blocked and tag_failed"
                )
                self.delete_file(self.input_crawled_file)
                self.delete_file(self.current_path + "\\crawling_status.pbf")
                self.delete_file(self.current_path + "\\pnf.txt")
                self.delete_file(self.current_path + "\\proxy_blocked.txt")
                self.delete_file(self.current_path + "\\tag_failed.txt")
                self.delete_file(self.current_path + "\\other_exception.txt")
            else:
                self.logger.info("resume crawl on")
                self.input_crawled_url = general.read_csv(self.input_crawled_file)

            self.logger.info("creating workers")
            self.create_workers()
            self.logger.info("Initiating crawl")
            self.crawl()
            if not Crawl_path.debug:
                self.logger.info("waiting for push data to db")
                self.data_queue.join()
                cur.close()
                conn.close()

            self.end_time = datetime.datetime.now()
            time_taken = self.end_time - self.start_time
            self.logger.info("Time taken to run the batch - " + str(time_taken))
            self.logger.info("Batch_end_time - " + str(self.end_time))
            print("Crawling completed successfully")
            logging.shutdown()

        except Exception as e:
            print(e)
            self.logger.error("Error in start method - " + str(e))
