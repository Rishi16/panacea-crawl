import codecs
import csv
import json
import socket
import threading
import zipfile
from zlib import compress

import wget
from fake_headers import Headers
from selenium.common.exceptions import TimeoutException

try:
    from amazoncaptcha import AmazonCaptcha
    from seleniumwire import webdriver as wirewebdriver
except:
    pass
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from random import randint
from lxml import html
import os
import sys
import io
import shutil
import time
import subprocess
import pandas as pd
import datetime
import unicodedata as un
import html as ht
import re

try:
    from stem import Signal
    from stem.control import Controller
except:
    pass
import requests


class Crawl_path:
    current_path = ""
    num_of_attempts = 5
    time_out = ""
    proxies = []
    current_proxy = {}
    blocked_proxies = {}
    working_proxies = {}
    proxy_mode = "random"
    proxy_lock = threading.Lock()
    requests_log_lock = threading.Lock()
    proxy_counter = -1
    proxy_loop_count = 0
    input_header = []
    output_header = []
    encoding = ""
    batch_id = None
    proxy_change = False
    print_requests = False
    debug = True
    tor = ""
    browser = {}
    browser_persistence = 0
    cooling_period = 0
    cache_path = ""
    host_name = ""
    host_address = ""
    batch_name = ""
    team_name = ""
    master = ""

    def __init__(
            self,
            current_path,
            num_of_attempts,
            num_of_threads,
            time_out,
            proxies,
            encoding,
            input_headers,
            browser_persistence,
            clear_selenium_session,
    ):
        debug = True
        batch_id = None
        Crawl_path.current_path = current_path
        Crawl_path.proxy_change = False
        Crawl_path.num_of_attempts = num_of_attempts
        Crawl_path.time_out = time_out
        Crawl_path.proxies = proxies
        Crawl_path.blocked_proxies = {}
        Crawl_path.working_proxies = {}
        Crawl_path.proxy_mode = "random"
        Crawl_path.num_of_threads = num_of_threads
        Crawl_path.current_proxy = {}
        Crawl_path.encoding = encoding
        Crawl_path.proxy_counter = -1
        Crawl_path.proxy_loop_count = 0
        Crawl_path.input_headers = input_headers
        Crawl_path.browser = {}
        Crawl_path.tor = ""
        Crawl_path.browser_persistence = browser_persistence
        Crawl_path.proxy_lock = threading.Lock()
        Crawl_path.requests_log_lock = threading.Lock()
        Crawl_path.print_requests = False
        Crawl_path.cooling_period = 0
        Crawl_path.host_name = socket.gethostname()
        Crawl_path.host_address = socket.gethostbyname(socket.gethostname())
        Crawl_path.cache_path = ""
        Crawl_path.batch_name = ""
        Crawl_path.master = ""
        Crawl_path.team_name = ""
        Crawl_path.clear_selenium_session = clear_selenium_session
        Crawl_path.binary_location = "C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe"
        self.setup_chromedriver()

    def setup_chromedriver(self):
        if os.path.exists(f'{os.getenv("ProgramFiles")}\Google\Chrome\Application\chrome.exe'):
            Crawl_path.binary_location = f'{os.getenv("ProgramFiles")}\Google\Chrome\Application\chrome.exe'
        if not Crawl_path.binary_location and os.path.exists(
                f'{os.getenv("ProgramFiles(x86)")}\Google\Chrome\Application\chrome.exe'):
            Crawl_path.binary_location = f'{os.getenv("ProgramFiles(x86)")}\Google\Chrome\Application\chrome.exe'

        if not os.path.exists('chromedriver.exe'):
            # get the latest chrome driver version number
            url = 'https://chromedriver.storage.googleapis.com/LATEST_RELEASE'
            response = requests.get(url)
            version_number = response.text

            # build the donwload url
            download_url = "https://chromedriver.storage.googleapis.com/" + version_number + \
                           "/chromedriver_win32.zip"

            # download the zip file using the url built above
            latest_driver_zip = wget.download(download_url, 'chromedriver.zip')

            # extract the zip file
            with zipfile.ZipFile(latest_driver_zip, 'r') as zip_ref:
                zip_ref.extractall()  # you can specify the destination folder path here
            # delete the zip file downloaded above
            os.remove(latest_driver_zip)

    def get_browser(self):
        return Crawl_path.browser

    # Each website is a separate project (folder)


def check_create_dir(directory):
    return create_project_dir(directory)


def create_project_dir(directory):
    if not os.path.exists(directory):
        # print('Creating directory ' + directory)
        os.makedirs(directory)
    return directory


# Create queue and crawled files (if not created)
def create_data_files(project_name, base_url):
    queue = os.path.join(project_name, "queue.txt")
    crawled = os.path.join(project_name, "crawled.txt")
    if not os.path.isfile(queue):
        write_file(queue, base_url)
    if not os.path.isfile(crawled):
        write_file(crawled, "")


# Create a new file
# def write_file(path, data):
#     with open(path, 'w') as f:
#         f.write(data)


# Add data onto an existing file
def append_to_file(path, data):
    with open(path, "a") as file:
        file.write(data + "\n")
    file.close()


# Delete the contents of a file
def delete_file_contents(path):
    open(path, "w").close()


# Read data from file
def file_to_set(file_name):
    results = set()
    with open(file_name, "rt") as f:
        for line in f:
            line = line.replace("\n", "")
            # values = line.split("|")
            results.add(line)
    f.close()
    return results


def file_to_list(file_name):
    results = []
    with open(file_name, "rt") as f:
        for line in f:
            line = line.replace("\n", "")
            # values = line.split("|")
            results.append(line)
    return results


# Write data to file
def write_file(path, data, mode="a", encoding=None):
    try:
        encoding = encoding if encoding is not None else str(Crawl_path.encoding)
    except:
        encoding = ""
    if encoding:
        with codecs.open(path, mode, encoding=encoding) as f:
            if type(data) == type([]):
                f.writelines(data)
            else:
                f.write(data + "\r\n")
        f.close()
    else:
        with codecs.open(path, mode) as f:
            if type(data) == type([]):
                f.writelines(data)
            else:
                f.write(data + "\r\n")
        f.close()


# Read data from to file
def read_file(path, encoding=None):
    encoding = encoding if encoding is not None else str(Crawl_path.encoding)
    with codecs.open(path, "r", encoding=encoding) as f:
        data = f.read()
    f.close()
    return data


# Overwrite data to file
def over_write_file(path, data, encoding=None):
    encoding = encoding if encoding is not None else str(Crawl_path.encoding)
    with codecs.open(path, "w", encoding=encoding) as f:
        f.write(data + "\r\n")
    f.close()


# Write data to file
def write_json(path, data, encoding=None):
    encoding = encoding if encoding is not None else str(Crawl_path.encoding)
    with codecs.open(path, "a+", encoding=encoding) as f:
        f.write(json.dumps(data) + "\r\n")
    f.close()


def get_cookies(file_name):
    with open(file_name, "r") as myfile:
        data = myfile.read().replace("\n", "")
    myfile.close()
    return data


def read_properties(file_name):
    property = {}
    with open(file_name, "rt") as f:
        for line in f:
            if "=" in line:
                line = line.replace("\n", "")
                values = line.split("=")
                property[values[0]] = values[1]
    f.close()
    return property


def write_properties(file_name, data):
    property = ""
    if not file_name:
        file_name = "./properties.pbf"
    for k, v in data.items():
        property = property + f"{k}={v}\n" if property else f"{k}={v}\n"
    with open(file_name, "w") as f:
        f.write(property)
    f.close()
    return property


def read_proxies(file_name):
    proxies = []
    each_proxy = {}
    if os.path.exists(file_name):
        with open(file_name, "rt", encoding="utf-8-sig") as f:
            validator = 0
            for line in f:
                if "=" in line:
                    line = line.replace("\n", "")
                    values = line.split("=")
                    validator += 1
                    if validator == 1:
                        each_proxy["host"] = values[1]
                    elif validator == 2:
                        each_proxy["username"] = values[1]
                    elif validator == 3:
                        each_proxy["password"] = values[1]
                    if validator == 4:
                        each_proxy["port"] = values[1]
                    if validator == 4:
                        validator = 0
                        proxies.append(each_proxy)
                        each_proxy = {}
                elif "\t" in line:
                    (
                        each_proxy["host"],
                        each_proxy["username"],
                        each_proxy["password"],
                        each_proxy["port"],
                    ) = line.replace("\n", "").split("\t")
                    proxies.append(each_proxy)
                    each_proxy = {}
    return proxies


def write_csv(file_path, data, delimiter="\t", encoding=None):
    encoding = encoding if encoding else str(Crawl_path.encoding)
    csv.register_dialect("myDialect", delimiter=delimiter)
    myFile = codecs.open(file_path, "a+", encoding=encoding)
    with myFile:
        writer = csv.writer(myFile, dialect="myDialect")
        writer.writerows(data)
    myFile.close()


def read_csv(file_path, skip_header=False, delimiter="\t", encoding=None):
    encoding = encoding if encoding is not None else str(Crawl_path.encoding)
    data = []
    csv.register_dialect("myDialect", delimiter=delimiter)
    myFile = codecs.open(file_path, "r", encoding=encoding)
    row_num = 0
    with myFile:
        read_data = csv.reader(myFile, dialect="myDialect")
        for row in read_data:
            if row_num == 0 and skip_header:
                row_num += 1
                continue
            if row:
                data.append(row)
            row_num += 1
    myFile.close()
    return data


def read_dataframe(
        file_path, sep=",", header=True, names=[], encoding=None, error_bad_lines=False
):
    try:
        if str(Crawl_path.encoding):
            encoding = encoding if encoding else str(Crawl_path.encoding)
    except:
        encoding = "utf8"
    if header == True:
        df = pd.read_csv(
            file_path,
            sep=sep,
            encoding=encoding,
            error_bad_lines=error_bad_lines,
            na_values=["_"],
            keep_default_na=False,
        )
    else:
        df = pd.read_csv(
            file_path,
            sep=sep,
            names=names,
            header=None,
            encoding=encoding,
            error_bad_lines=error_bad_lines,
            na_valuse=["_"],
            keep_default_na=False,
        )
    return df


def write_dataframe(
        df, file_path, seperator="\t", index=False, header=True, encoding=None
):
    encoding = encoding if encoding is not None else str(Crawl_path.encoding)
    df.to_csv(file_path, sep=seperator, encoding=encoding, index=index, header=header)


def create_session(
        proxies_from_file=None,
        proxies=None,
        user_name="panacea",
        use_proxy=True,
        visible=True,
        images=False,
        all_requests=False,
        cloak=False,
        proxy_auth_plugin="proxy_auth_plugin",
):
    proxy_to_return = {}
    options = Options()
    options.add_argument("start-maximized")
    profile_path = check_create_dir(
        os.path.join(
            Crawl_path.current_path,
            "selenium",
            "profile",
            threading.current_thread().name,
        )
    )
    options.add_argument("user-data-dir=" + profile_path)
    if cloak:
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)
        options.add_argument("--disable-blink-features=AutomationControlled")
    if not images:
        prefs = {"profile.managed_default_content_settings.images": 2}
        options.add_experimental_option("prefs", prefs)
    capabilities = dict(DesiredCapabilities.CHROME)
    options.binary_location = Crawl_path.binary_location
    if not visible:
        options.add_argument("--headless")
    if not images:
        prefs = {
            "profile.managed_default_content_settings.images": 2,
            "disk-cache-size": 209715200,
        }
        options.add_experimental_option("prefs", prefs)

    if proxies_from_file is None:
        proxies_from_file = Crawl_path.proxies
        if proxies != False:
            if proxies:
                proxy = proxies.get("https","").replace("https://", "")
                proxy = proxy.split("@")
                pr_username, pr_password = proxy[0].split(":")
                pr_host, pr_port = proxy[1].split(":")
            elif proxies is None:
                strPOS = randint(0, len(proxies_from_file) - 1)
                pr_host = str(proxies_from_file[strPOS]["host"])
                pr_port = str(proxies_from_file[strPOS]["port"])
                pr_username = proxies_from_file[strPOS]["username"]
                pr_password = proxies_from_file[strPOS]["password"]
            proxy_to_return = {
                "https": "https://"
                         + str(pr_username)
                         + ":"
                         + str(pr_password)
                         + "@"
                         + str(pr_host)
                         + ":"
                         + str(pr_port)
            }

            manifest_json = """
            {
                "version": "1.0.0",
                "manifest_version": 2,
                "name": "Chrome Proxy",
                "permissions": [
                    "proxy",
                    "tabs",
                    "unlimitedStorage",
                    "storage",
                    "<all_urls>",
                    "webRequest",
                    "webRequestBlocking"
                ],
                "background": {
                    "scripts": ["background.js"]
                },
                "minimum_chrome_version":"22.0.0"
            }
            """

            background_js = """
            var config = {
                    mode: "fixed_servers",
                    rules: {
                    singleProxy: {
                        scheme: "http",
                        host: "%s",
                        port: parseInt(%s)
                    },
                    bypassList: ["localhost"]
                    }
                };
        
            chrome.proxy.settings.set({value: config, scope: "regular"}, function() {});
        
            function callbackFn(details) {
                return {
                    authCredentials: {
                        username: "%s",
                        password: "%s"
                    }
                };
            }
        
            chrome.webRequest.onAuthRequired.addListener(
                        callbackFn,
                        {urls: ["<all_urls>"]},
                        ['blocking']
            );
            """ % (
                pr_host,
                pr_port,
                pr_username,
                pr_password,
            )

        if use_proxy:
            # print('user proxy')
            ext_path = check_create_dir(
                os.path.join(
                    Crawl_path.current_path,
                    "selenium",
                    "extensions",
                    threading.current_thread().name,
                )
            )
            write_file(
                os.path.join(ext_path, "manifest.json"),
                manifest_json,
                mode="w",
                encoding="utf8",
            )
            write_file(
                os.path.join(ext_path, "background.js"),
                background_js,
                mode="w",
                encoding="utf8",
            )
            options.add_argument("--load-extension=" + ext_path)
            # pluginfile = os.path.join(str(Crawl_path.current_path), str(proxy_auth_plugin) + '.zip')
            # with zipfile.ZipFile(pluginfile, 'w') as zp:
            #     zp.writestr("manifest.json", manifest_json)
            #     zp.writestr("background.js", background_js)
            # # prefs = {"profile.managed_default_content_settings.images": 2}
            # chrome_options.add_extension(pluginfile)
            # chrome_options.add_experimental_option("prefs", prefs)
            # chrome_options.add_experimental_option("prefs",
            # {'profile.managed_default_content_settings.javascript': 2})
            # chrome_options.binary_location = 'C:\\Users\\' + str(
            #     user_name) + '\\AppData\\Local\\Google\\Chrome SxS\\Application\\chrome.exe'
        else:
            capabilities["proxy"] = {
                "proxyType": "MANUAL",
                "httpProxy": pr_host + ":" + str(pr_port),
                "ftpProxy": pr_host + ":" + str(pr_port),
                "sslProxy": pr_host + ":" + str(pr_port),
                "noProxy": "",
                "class": "org.openqa.selenium.Proxy",
                "autodetect": False,
            }
            capabilities["proxy"]["socksUsername"] = pr_username
            capabilities["proxy"]["socksPassword"] = pr_password
            # prefs = {"profile.managed_default_content_settings.images": 2}
            # chrome_options.add_experimental_option("prefs", prefs)
            # chrome_options.add_experimental_option("prefs",
            # {'profile.managed_default_content_settings.javascript': 2})
            options.add_argument("--headless")
            capabilities["pageLoadStrategy"] = "none"
            driver = webdriver.Chrome(
                "chromedriver.exe",
                chrome_options=options,
                desired_capabilities=capabilities,
            )

    # capabilities["pageLoadStrategy"] = "none"
    if not all_requests:
        driver = webdriver.Chrome(
            "chromedriver.exe",
            chrome_options=options,
            desired_capabilities=capabilities,
        )
    else:
        driver = wirewebdriver.Chrome(
            "chromedriver.exe",
            chrome_options=options,
            desired_capabilities=capabilities,
        )
    enable_console_log = True
    try:
        if driver.execute_script("return navigator.webdriver"):
            driver.execute_cdp_cmd(
                "Page.addScriptToEvaluateOnNewDocument",
                {
                    "source": """
                                            Object.defineProperty(window, 'navigator', {
                                                value: new Proxy(navigator, {
                                                has: (target, key) => (key === 'webdriver' ? 
                                                false : 
                                                key in target),
                                                get: (target, key) =>
                                                    key === 'webdriver'
                                                    ? undefined
                                                    : typeof target[key] === 'function'
                                                    ? target[key].bind(target)
                                                    : target[key]
                                                })
                                            });
                                        """
                              + (
                                  "console.log = console.dir = console.error = function(){};"
                                  if not enable_console_log
                                  else ""
                              )
                },
            )
        driver.execute_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
        driver.execute_cdp_cmd(
            "Network.setUserAgentOverride",
            {
                "userAgent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ("
                             "KHTML, like Gecko) Chrome/83.0.4103.53 Safari/537.36"
            },
        )
    except:
        pass
    # capabilities['proxy'] = {'proxyType': 'MANUAL',
    #                          'httpProxy': pr_host + ':' + pr_port,
    #                          'ftpProxy': pr_host + ':' + pr_port,
    #                          'sslProxy': pr_host + ':' + pr_port,
    #                          'noProxy': '',
    #                          'class': "org.openqa.selenium.Proxy",
    #                          'autodetect': False}
    # capabilities['proxy']['socksUsername'] = pr_username
    # capabilities['proxy']['socksPassword'] = pr_password
    return driver, proxy_to_return, profile_path


def patch_chrome_driver_binary(binary_path):
    linect = 0
    with io.open(binary_path, "r+b") as fh:
        for line in iter(lambda: fh.readline(), b""):
            if b"cdc_" in line:
                fh.seek(-len(line), 1)
                newline = re.sub(b"cdc_.{22}", b"xxx_undetectedRISHIePATtch", line)
                fh.write(newline)
                linect += 1
        return linect


def wait_driver(driver, tag, wait_time, by="XPATH"):
    try:
        if by == "XPATH":
            WebDriverWait(driver, int(wait_time)).until(
                EC.presence_of_element_located((By.XPATH, tag))
            )
        elif by == "CSS":
            WebDriverWait(driver, int(wait_time)).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, tag))
            )

        print("Page loaded.")
        return True
    except TimeoutException:
        print("Tag not found:", tag)
        return False


def find_elements_driver(driver, tag):
    try:
        return driver.find_elements(By.XPATH, tag)
    except:
        return None


# Used in old scripts. Keeping it for backwards compatibiltiy
def random_proxy(proxies_from_file):
    return get_proxy(proxies_from_file)


def get_proxy(proxies_from_file, strPOS=-1):
    if strPOS == -1:
        strPOS = randint(0, len(proxies_from_file) - 1)
    proxies = {
        "http": "http://"
                + str(proxies_from_file[strPOS]["username"])
                + ":"
                + str(proxies_from_file[strPOS]["password"])
                + "@"
                + str(proxies_from_file[strPOS]["host"])
                + ":"
                + str(proxies_from_file[strPOS]["port"]),
        "https": "https://"
                 + str(proxies_from_file[strPOS]["username"])
                 + ":"
                 + str(proxies_from_file[strPOS]["password"])
                 + "@"
                 + str(proxies_from_file[strPOS]["host"])
                 + ":"
                 + str(proxies_from_file[strPOS]["port"]),
    }
    return proxies, strPOS


def convert_selenium_cookies(cookies):
    final_cookies = {}
    for cookie in cookies:
        final_cookies[cookie["name"]] = cookie["value"]
    return final_cookies


# def push_data(tag, data, encoding=None, use_df=False):
#     encoding = encoding if encoding is not None else str(Crawl_path.encoding)
#     current_path = Crawl_path.current_path
#     input_headers = Crawl_path.input_headers
#     prod_count = ''
#     prod_count_path = os.path.join(current_path, 'crawling_status.pbf')
#     try:
#
#         if tag.lower() == 'found':
#             found = 1;
#             pnf = 0;
#             tag_failed = 0;
#             proxy_blocked = 0;
#             other = 0
#             data_file_path = os.path.join(current_path, 'final_data.txt')
#         elif tag.lower() == 'pnf':
#             found = 0;
#             pnf = 1;
#             tag_failed = 0;
#             proxy_blocked = 0;
#             other = 0
#             data_file_path = os.path.join(current_path, 'pnf.txt')
#         elif tag.lower() == 'tag_failed':
#             found = 0;
#             pnf = 0;
#             tag_failed = 1;
#             proxy_blocked = 0;
#             other = 0
#             data_file_path = os.path.join(current_path, 'tag_failed.txt')
#         elif tag.lower() == 'proxy_blocked':
#             found = 0;
#             pnf = 0;
#             tag_failed = 0;
#             proxy_blocked = 1;
#             other = 0
#             data_file_path = os.path.join(current_path, 'proxy_blocked.txt')
#         else:
#             found = 0;
#             pnf = 0;
#             tag_failed = 0;
#             proxy_blocked = 0;
#             other = 1
#             data_file_path = os.path.join(current_path, 'other_exception.txt')
#         if not os.path.isfile(prod_count_path):
#             data_to_write = str(found) + '\r\n' + str(pnf) + '\r\n' + str(tag_failed) + '\r\n'
#             + str(
#                 proxy_blocked) + '\r\n' + str(other)
#             with codecs.open(prod_count_path, 'w', encoding=encoding) as f:
#                 f.write(str(data_to_write) + '\r\n')
#         else:
#             prod_count = file_to_list(prod_count_path)
#             data_to_write = str(int(prod_count[0]) + int(found)) + '\r\n' + str(
#                 int(prod_count[1]) + int(pnf)) + '\r\n' + str(int(prod_count[2]) + int(
#                 tag_failed)) + '\r\n' + str(
#                 int(prod_count[3]) + int(proxy_blocked)) + '\r\n' + str(int(prod_count[4]) +
#                 int(other))
#             with codecs.open(prod_count_path, 'w', encoding=encoding) as f:
#                 f.write(str(data_to_write) + '\r\n')
#         if Crawl_path.debug == False:
#             # print("debug false")
#             if tag == 'found':
#                 input_headers_val = Crawl_path.output_header
#             else:
#                 input_headers_val = Crawl_path.input_headers
#             for temp_data in data:
#                 data_to_push = dict(zip(input_headers_val, temp_data))
#                 data_to_push = json.dumps(data_to_push, ensure_ascii=False).replace("\u0000", "")
#                 # data_to_push = data_to_push.encode(encoding)
#                 if use_df:
#                     temp_df = pd.DataFrame.from_records(
#                         [[str(Crawl_path.batch_id), str(datetime.datetime.now()), data_to_push]],
#                         columns=['Batch_id', 'Creation_date', 'Data'])
#                     write_dataframe(temp_df, data_file_path, header=False, encoding=encoding)
#                 else:
#                     write_csv(data_file_path, [[str(Crawl_path.batch_id),
#                     str(datetime.datetime.now()), data_to_push]],
#                               encoding=encoding)
#                     # write_json(data_file_path, data_to_push)
#         else:
#             # print("debug true")
#             write_csv(data_file_path, data, encoding=encoding)
#         Crawl_path.file_locker = False
#     except Exception as e:
#         print(e)
#         write_file(os.path.join(current_path, 'exception.txt'), str(prod_count))
#         Crawl_path.file_locker = False


def header_values(data):
    current_path = Crawl_path.current_path
    Crawl_path.output_header = data
    # current_path = os.path.dirname(os.path.abspath(__file__))
    data_file_path = os.path.join(current_path, "final_data.txt")
    pnf_file_path = os.path.join(current_path, "pnf.txt")
    proxy_blocked_file_path = os.path.join(current_path, "proxy_blocked.txt")
    tag_failed_file_path = os.path.join(current_path, "tag_failed.txt")
    other_file_path = os.path.join(current_path, "other_exception.txt")
    if Crawl_path.debug == True:
        if not os.path.isfile(data_file_path):
            write_csv(data_file_path, [data])
        if not os.path.isfile(pnf_file_path):
            write_csv(pnf_file_path, [Crawl_path.input_headers])
        if not os.path.isfile(proxy_blocked_file_path):
            write_csv(proxy_blocked_file_path, [Crawl_path.input_headers])
        if not os.path.isfile(tag_failed_file_path):
            write_csv(tag_failed_file_path, [Crawl_path.input_headers])
        if not os.path.isfile(other_file_path):
            write_csv(other_file_path, [Crawl_path.input_headers])
    else:
        if not os.path.isfile(data_file_path):
            write_csv(data_file_path, [["batch_run_id", "creation_date", "data"]])
        if not os.path.isfile(pnf_file_path):
            write_csv(pnf_file_path, [["batch_run_id", "creation_date", "data"]])
        if not os.path.isfile(proxy_blocked_file_path):
            write_csv(
                proxy_blocked_file_path, [["batch_run_id", "creation_date", "data"]]
            )
        if not os.path.isfile(tag_failed_file_path):
            write_csv(tag_failed_file_path, [["batch_run_id", "creation_date", "data"]])
        if not os.path.isfile(other_file_path):
            write_csv(other_file_path, [["batch_run_id", "creation_date", "data"]])


def send_sms(msg, no):
    url = "https://www.fast2sms.com/dev/bulk"
    payload = f"sender_id=FSTSMS&message={msg}&language=english&route=p&numbers={no}"
    headers = {
        "authorization":
            "jh3F6yEw58RDZuLvMX9mx4KzkPAa7gJlrBcWsUYTQ1SfqnpbioaKBIEPwfGjzCZvpWqOL3eQiSluAx1r",
        "Content-Type": "application/x-www-form-urlencoded",
        "Cache-Control": "no-cache",
    }
    data, session = post_url(url, post_data=payload, headers=headers)
    print(data["text"])


def get_url(
        url,
        session=None,
        headers=None,
        cookies=None,
        params=None,
        encoding=None,
        proxies=False,
        verify=True,
        keep_proxy=False,
        blockwords=[],
        flagwords=[],
        request_type="get",
        keep_session=True,
        post_data=None,
        close_session=None,
):
    res_text = res_url = res_cookies = res_code = source = response = res_time = ""
    data = {}
    number_of_attempts = strPOS = 0
    Crawl_path.proxy_change = False
    no_proxy = True if proxies == False else False
    if headers is None:
        headers = {
            "Connection": "keep-alive",
            "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, "
                          "like Gecko) Chrome/63.0.3239.132 Safari/537.36",
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
        }
    elif headers == "random":
        headers = Headers(os="windows", headers=True).generate()
    if session is None and keep_session:
        session = requests.Session()
    if params is None:
        params = {}
    if cookies is None:
        cookies = {}
    current_path = Crawl_path.current_path
    log_path = os.path.join(current_path, "request.log")
    user_proxy = True
    retrying = False
    while number_of_attempts < Crawl_path.num_of_attempts:
        blockword = title = rescode = ""
        blocked = False
        try:
            number_of_attempts += 1
            if (proxies is None or not user_proxy) and not keep_proxy and not no_proxy:
                user_proxy = False
                if Crawl_path.proxy_mode == "random":
                    proxies, strPOS = get_proxy(Crawl_path.proxies)
                elif Crawl_path.proxy_mode == "sequential_exhaustive":
                    # print(str(threading.current_thread().name), Crawl_path.current_proxy)
                    if (
                            str(threading.current_thread().name) in Crawl_path.current_proxy
                            and not Crawl_path.current_proxy[
                        threading.current_thread().name
                    ]["blocked"]
                            and not retrying
                    ):
                        proxies = Crawl_path.current_proxy[
                            threading.current_thread().name
                        ]["proxy"]
                        strPOS = Crawl_path.current_proxy[
                            threading.current_thread().name
                        ]["position"]
                    else:
                        Crawl_path.proxy_lock.acquire()
                        while True:
                            if len(Crawl_path.proxies) - 1 > Crawl_path.proxy_counter:
                                Crawl_path.proxy_counter += 1
                            else:
                                Crawl_path.proxy_counter = 0
                            if Crawl_path.proxy_counter in Crawl_path.blocked_proxies:
                                if (
                                        datetime.datetime.now()
                                        - Crawl_path.blocked_proxies[
                                            Crawl_path.proxy_counter
                                        ]
                                ).seconds > Crawl_path.cooling_period:
                                    del Crawl_path.blocked_proxies[
                                        Crawl_path.proxy_counter
                                    ]
                                    break
                            else:
                                break
                        proxies, strPOS = get_proxy(
                            Crawl_path.proxies, Crawl_path.proxy_counter
                        )
                        Crawl_path.current_proxy[threading.current_thread().name] = {
                            "proxy": proxies,
                            "blocked": False,
                            "position": strPOS,
                        }
                        Crawl_path.proxy_lock.release()
                elif Crawl_path.proxy_mode == "sequential_rotational":
                    Crawl_path.proxy_lock.acquire()
                    while True:
                        if len(Crawl_path.proxies) - 1 > Crawl_path.proxy_counter:
                            Crawl_path.proxy_counter += 1
                        else:
                            Crawl_path.proxy_counter = 0
                            Crawl_path.proxy_loop_count += 1
                            if Crawl_path.proxy_loop_count % 10 == 0:
                                Crawl_path.blocked_proxies = {}
                                Crawl_path.proxy_loop_count = 0
                        if Crawl_path.proxy_counter in Crawl_path.blocked_proxies:
                            if (
                                    datetime.datetime.now()
                                    - Crawl_path.blocked_proxies[Crawl_path.proxy_counter]
                            ).seconds > Crawl_path.cooling_period:
                                del Crawl_path.blocked_proxies[Crawl_path.proxy_counter]
                                break
                        else:
                            break
                    proxies, strPOS = get_proxy(
                        Crawl_path.proxies, Crawl_path.proxy_counter
                    )
                    Crawl_path.current_proxy[threading.current_thread().name] = {
                        "proxy": proxies,
                        "blocked": False,
                        "position": strPOS,
                    }
                    Crawl_path.proxy_lock.release()
                    # print(str(threading.current_thread().name) in Crawl_path.current_proxy,
                    #       Crawl_path.current_proxy[threading.current_thread().name]['blocked'])
                elif Crawl_path.proxy_mode == "tor":
                    proxies = {
                        "http": "socks5://127.0.0.1:9050",
                        "https": "socks5://127.0.0.1:9050",
                    }
                    strPOS = 0
                    if "blocked" in Crawl_path.current_proxy:
                        if Crawl_path.current_proxy["blocked"]:
                            Crawl_path.proxy_lock.acquire()
                            if (
                                    Crawl_path.current_proxy[
                                        threading.current_thread().name
                                    ]
                                    == Crawl_path.proxy_counter
                            ):
                                Crawl_path.proxy_counter += 1
                                Crawl_path.current_proxy[
                                    threading.current_thread().name
                                ] += 1
                                with Controller.from_port(port=9051) as c:
                                    c.authenticate(password="eclerx#123")
                                    c.signal(Signal.NEWNYM)
                                Crawl_path.current_proxy["blocked"] = False
                                # Crawl_path.proxy_change = True
                            else:
                                Crawl_path.current_proxy[
                                    threading.current_thread().name
                                ] = Crawl_path.proxy_counter
                            Crawl_path.proxy_lock.release()
                    else:
                        Crawl_path.current_proxy["blocked"] = False
                        Crawl_path.current_proxy[threading.current_thread().name] = -1
                elif Crawl_path.proxy_mode == "eclerx_api":
                    ot = datetime.datetime.now()
                    r = requests.get(
                        "http://10.100.22.120:5000/api/v1/proxies?website=mpp_4"
                    ).json()
                    proxies = {"https": "https://" + r, "http": "http://" + r}
            if user_proxy:
                strPOS = 0
            if no_proxy:
                if request_type == "get":
                    if not keep_session:
                        response = requests.get(
                            url,
                            headers=headers,
                            params=params,
                            cookies=cookies,
                            timeout=Crawl_path.time_out,
                            verify=verify,
                        )
                    else:
                        response = session.get(
                            url,
                            headers=headers,
                            params=params,
                            cookies=cookies,
                            timeout=Crawl_path.time_out,
                            verify=verify,
                        )
                else:
                    if not keep_session:
                        response = requests.post(
                            url,
                            headers=headers,
                            params=params,
                            cookies=cookies,
                            timeout=Crawl_path.time_out,
                            data=post_data,
                            verify=verify,
                        )
                    else:
                        response = session.post(
                            url,
                            headers=headers,
                            params=params,
                            cookies=cookies,
                            timeout=Crawl_path.time_out,
                            data=post_data,
                            verify=verify,
                        )

            else:
                if request_type == "get":
                    if not keep_session:
                        response = requests.get(
                            url,
                            headers=headers,
                            proxies=proxies,
                            params=params,
                            cookies=cookies,
                            timeout=Crawl_path.time_out,
                            verify=verify,
                        )
                    else:
                        response = session.get(
                            url,
                            headers=headers,
                            proxies=proxies,
                            params=params,
                            cookies=cookies,
                            timeout=Crawl_path.time_out,
                            verify=verify,
                        )
                else:
                    if not keep_session:
                        response = requests.post(
                            url,
                            headers=headers,
                            proxies=proxies,
                            params=params,
                            cookies=cookies,
                            timeout=Crawl_path.time_out,
                            data=post_data,
                            verify=verify,
                        )
                    else:
                        response = session.post(
                            url,
                            headers=headers,
                            proxies=proxies,
                            params=params,
                            cookies=cookies,
                            timeout=Crawl_path.time_out,
                            data=post_data,
                            verify=verify,
                        )
            if encoding is not None:
                response.encoding = str(encoding)
            res_text = response.text
            res_url = response.url
            res_cookies = response.cookies.get_dict()
            res_code = response.status_code
            res_time = response.elapsed.total_seconds()
            if Crawl_path.proxy_mode == "eclerx_api":
                ct = datetime.datetime.now()
                t = ct - ot
                res_time = t.total_seconds()
            source = html.fromstring(res_text)
            title = xpath(source, "//title/text()")
            for word in blockwords:
                if word in res_text:
                    if (
                            Crawl_path.proxy_mode == "sequential_exhaustive"
                            and not no_proxy
                    ):
                        Crawl_path.current_proxy[threading.current_thread().name][
                            "blocked"
                        ] = True
                        Crawl_path.blocked_proxies[
                            Crawl_path.current_proxy[threading.current_thread().name][
                                "position"
                            ]
                        ] = datetime.datetime.now()
                    elif (
                            Crawl_path.proxy_mode == "sequential_rotational"
                            and not no_proxy
                    ):
                        Crawl_path.blocked_proxies[
                            Crawl_path.current_proxy[threading.current_thread().name][
                                "position"
                            ]
                        ] = datetime.datetime.now()
                    blockword = word
                    blocked = True
                    break
            for word in flagwords:
                if word in res_text:
                    blockword = word
            if not res_text:
                blockword = "Empty"
            Crawl_path.requests_log_lock.acquire()
            if no_proxy:
                write_csv(
                    log_path,
                    [
                        [
                            time.strftime("%Y:%m:%d %H:%M:%S"),
                            number_of_attempts,
                            url,
                            res_url,
                            res_code,
                            "No proxy",
                            blockword,
                            title,
                            "",
                            res_time,
                        ]
                    ],
                )
            else:
                write_csv(
                    log_path,
                    [
                        [
                            time.strftime("%Y:%m:%d %H:%M:%S"),
                            number_of_attempts,
                            url,
                            res_url,
                            res_code,
                            proxies.get("https",""),
                            blockword,
                            title,
                            "",
                            res_time,
                        ]
                    ],
                )
            Crawl_path.requests_log_lock.release()
            if Crawl_path.print_requests:
                print(number_of_attempts, strPOS, url, res_code, title, blockword)
            # if res_text and not blocked and res_code and int(res_code) < 500:
            if res_text and not blocked and res_code:
                break
            else:
                if not no_proxy and not keep_proxy:
                    proxies = None
                retrying = True
        except Exception as e:
            if proxies is None and not no_proxy and not keep_proxy:
                proxies = {}
                proxies["https"] = {}
            Crawl_path.requests_log_lock.acquire()
            if no_proxy:
                write_csv(
                    log_path,
                    [
                        [
                            time.strftime("%Y:%m:%d %H:%M:%S"),
                            number_of_attempts,
                            url,
                            "Exception",
                            "",
                            "No proxy",
                            blockword,
                            title,
                            str(e),
                            res_time,
                        ]
                    ],
                )
            else:
                write_csv(
                    log_path,
                    [
                        [
                            time.strftime("%Y:%m:%d %H:%M:%S"),
                            number_of_attempts,
                            url,
                            "Exception",
                            "",
                            proxies.get("https",""),
                            blockword,
                            title,
                            str(e),
                            res_time,
                        ]
                    ],
                )
            Crawl_path.requests_log_lock.release()
            if Crawl_path.print_requests:
                print(number_of_attempts, strPOS, url, rescode, "Exception", str(e))
            if not keep_proxy and not no_proxy:
                proxies = None
            retrying = True

    data["url"] = res_url
    data["raw"] = response
    data["text"] = res_text
    data["cookies"] = res_cookies
    data["status_code"] = res_code
    data["proxies"] = proxies
    data["source"] = source

    return data, session


def get_url2(
        url,
        tag_to_find="",
        proxies=False,
        session=None,
        close_session=False,
        visible=True,
        images=False,
        blockwords=[],
        flagwords=[],
        timeout=None,
        all_requests=False,
        plugin_file="proxy_auth_plugin",
        cloak=False,
):
    response = res_url = res_time = ""
    data = {}
    number_of_attempts = 0
    current_path = Crawl_path.current_path
    log_path = os.path.join(current_path, "request.log")
    retrying = False
    driver = profile_path = ""
    # try:
    #     with open('memory.csv','a') as fw:
    #         p = psutil.Process(os.getpid())
    #         m = p.memory_percent()
    #         for c in p.children(recursive=True):
    #             m += c.memory_percent()
    #         t = (psutil.virtual_memory().total / float(2 ** 30)) * m / 100
    #         fw.write(''.join([threading.current_thread().name,',',str(t),'\n']))
    #         fw.close()
    # except:
    #     pass

    while number_of_attempts < Crawl_path.num_of_attempts:
        number_of_attempts += 1
        blocked = False
        status_code = 200
        blockword = source = res_url = title = driver = ""
        try:
            if session:
                driver, proxies = session
            else:
                if (
                        str(threading.current_thread().name) in Crawl_path.browser
                        and not retrying
                ):
                    if (
                            Crawl_path.browser[str(threading.current_thread().name)][
                                "persistence"
                            ]
                            < Crawl_path.browser_persistence
                    ):
                        Crawl_path.browser[str(threading.current_thread().name)][
                            "persistence"
                        ] += 1
                        driver = Crawl_path.browser[
                            str(threading.current_thread().name)
                        ]["driver"]
                        proxies = Crawl_path.browser[
                            str(threading.current_thread().name)
                        ]["proxy"]
                        profile_path = Crawl_path.browser[
                            str(threading.current_thread().name)
                        ]["profile_path"]
                    else:
                        if str(threading.current_thread().name) in Crawl_path.browser:
                            close_chrome(
                                Crawl_path.browser[
                                    str(threading.current_thread().name)
                                ]["driver"],
                                Crawl_path.browser[
                                    str(threading.current_thread().name)
                                ]["profile_path"],
                            )
                        driver, proxies, profile_path = create_session(
                            Crawl_path.proxies,
                            proxies=proxies,
                            visible=visible,
                            images=images,
                            all_requests=all_requests,
                            cloak=cloak,
                            proxy_auth_plugin="proxy_auth_plugin_"
                                              + str(threading.current_thread().name),
                        )
                        Crawl_path.browser[str(threading.current_thread().name)][
                            "driver"
                        ] = driver
                        Crawl_path.browser[str(threading.current_thread().name)][
                            "profile_path"
                        ] = profile_path
                        Crawl_path.browser[str(threading.current_thread().name)][
                            "proxy"
                        ] = proxies
                        Crawl_path.browser[str(threading.current_thread().name)][
                            "persistence"
                        ] = 0
                else:
                    if (
                            retrying
                            and str(threading.current_thread().name) in Crawl_path.browser
                    ):
                        close_chrome(
                            Crawl_path.browser[str(threading.current_thread().name)][
                                "driver"
                            ],
                            Crawl_path.browser[str(threading.current_thread().name)][
                                "profile_path"
                            ],
                        )
                    driver, proxies, profile_path = create_session(
                        Crawl_path.proxies,
                        proxies=proxies,
                        visible=visible,
                        images=images,
                        all_requests=all_requests,
                        cloak=cloak,
                        proxy_auth_plugin="proxy_auth_plugin_"
                                          + str(threading.current_thread().name),
                    )
                    Crawl_path.browser[str(threading.current_thread().name)] = {}
                    Crawl_path.browser[str(threading.current_thread().name)][
                        "persistence"
                    ] = 0
                    Crawl_path.browser[str(threading.current_thread().name)][
                        "proxy"
                    ] = proxies
                    Crawl_path.browser[str(threading.current_thread().name)][
                        "profile_path"
                    ] = profile_path
                    Crawl_path.browser[str(threading.current_thread().name)][
                        "driver"
                    ] = driver
            ot = datetime.datetime.now()
            driver.get(url)
            ct = datetime.datetime.now()
            if tag_to_find:
                time_out = timeout if timeout else Crawl_path.time_out
                wait_driver(driver, tag_to_find, int(time_out))
            response = driver.page_source
            errors = [
                "ERR_TIMED_OUT</div>",
                "ERR_INTERNET_DISCONNECTED</div>",
                "ERR_CONNECTION_TIMED_OUT</div>",
                "ERR_CONNECTION_REFUSED</div>",
                "ERR_TOO_MANY_RETRIES</div>",
                "ERR_NETWORK_CHANGED</div>",
                "ERR_FAILED</div>",
                "ERR_ABORTED</div>",
                "ERR_UNEXPECTED</div>",
                "ERR_ACCESS_DENIED</div>",
                "ERR_BLOCKED_BY_CLIENT</div>",
                "ERR_BLOCKED_BY_ADMINISTRATOR</div>",
                "ERR_BLOCKED_BY_RESPONSE</div>",
                "ERR_CONNECTION_CLOSED</div>",
                "ERR_CONNECTION_RESET</div>",
                "ERR_CONNECTION_ABORTED</div>",
                "ERR_CONNECTION_FAILED</div>",
                "ERR_INTERNET_DISCONNECTED</div>",
                "ERR_CERT_END</div>",
                "ERR_INVALID_REDIRECT</div>",
                "ERR_EMPTY_RESPONSE</div>",
                "ERR_INVALID_AUTH_CREDENTIALS</div>",
                "ERR_PROXY_CONNECTION_FAILED</div>",
            ]
            if any(error in response for error in errors):
                response = ""
            res_url = driver.current_url
            if not response:
                res_url = "Exception"
            try:
                source = html.fromstring(response)
                title = xpath(source, "//title/text()")
            except:
                source = title = ""
            for word in blockwords:
                if word in response:
                    blockword = word
                    blocked = True
                    status_code = 503
            for word in flagwords:
                if word in response:
                    blockword = word
            if blocked:
                if "https://www.amazon" in url:
                    if "/errors/validateCaptcha" in response:
                        try:
                            if not images:
                                close_chrome(driver, profile_path)
                                driver, proxies, profile_path = create_session(
                                    Crawl_path.proxies,
                                    proxies=proxies,
                                    visible=visible,
                                    images=True,
                                    all_requests=all_requests,
                                    cloak=cloak,
                                    proxy_auth_plugin="proxy_auth_plugin_"
                                                      + str(threading.current_thread().name),
                                )
                                driver.get(url)
                            solution = AmazonCaptcha.from_webdriver(driver).solve()
                            driver.find_element_by_xpath(
                                "//input[@id='captchacharacters']"
                            ).send_keys(solution)
                            driver.find_element_by_xpath(
                                '//form[@action="/errors/validateCaptcha"]//button'
                            ).click()
                            wait_driver(driver, '//*[@id="nav-belt"]', 20)
                            ct = datetime.datetime.now()
                            response = driver.page_source
                            if not images:
                                close_chrome(driver, profile_path)
                                driver, proxies, profile_path = create_session(
                                    Crawl_path.proxies,
                                    proxies=proxies,
                                    visible=visible,
                                    images=images,
                                    all_requests=all_requests,
                                    cloak=cloak,
                                    proxy_auth_plugin="proxy_auth_plugin_"
                                                      + str(threading.current_thread().name),
                                )
                                Crawl_path.browser[
                                    str(threading.current_thread().name)
                                ]["driver"] = driver
                                Crawl_path.browser[
                                    str(threading.current_thread().name)
                                ]["proxy"] = proxies
                                Crawl_path.browser[
                                    str(threading.current_thread().name)
                                ]["profile_path"] = profile_path
                            wait_driver(driver, '//*[@id="nav-logo"]', 20)
                            response = driver.page_source
                            blocked = False
                            blockword = ""
                            status_code = 200
                            for word in blockwords:
                                if word in response:
                                    blockword = word
                                    blocked = True
                                    status_code = 503
                        except Exception as e:
                            print(get_error_line(e))

            if not response:
                blockword = "Empty"
            t = ct - ot
            res_time = t.total_seconds()
            Crawl_path.requests_log_lock.acquire()
            write_csv(
                log_path,
                [
                    [
                        time.strftime("%Y:%m:%d %H:%M:%S"),
                        number_of_attempts,
                        url,
                        res_url,
                        status_code,
                        proxies.get("https",""),
                        blockword,
                        title,
                        "",
                        res_time,
                    ]
                ],
            )
            Crawl_path.requests_log_lock.release()
            if Crawl_path.print_requests:
                print(number_of_attempts, url, title, blockword, proxies.get("https",""))
            if response and not blocked:
                break
            else:
                retrying = True
                proxies = None

        except Exception as e:
            close_chrome(driver, profile_path)
            if not proxies:
                proxies = {"https": ""}
            Crawl_path.requests_log_lock.acquire()
            write_csv(
                log_path,
                [
                    [
                        time.strftime("%Y:%m:%d %H:%M:%S"),
                        number_of_attempts,
                        url,
                        "Exception",
                        "",
                        proxies.get("https",""),
                        blockword,
                        title,
                        str(e),
                        res_time,
                    ]
                ],
            )
            # exc_type, exc_obj, exc_tb = sys.exc_info()
            # fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            # write_csv('general_errors.txt',
            #           [[str(e), exc_type, fname, exc_tb.tb_lineno]])
            Crawl_path.requests_log_lock.release()
            proxies = None
            retrying = True

    try:
        data["requests"] = driver.requests
    except:
        data["requests"] = None
    if close_session and driver and driver.service.process:
        close_chrome(driver, profile_path)

    data["text"] = response
    data["url"] = res_url
    data["proxies"] = proxies
    data["status_code"] = status_code
    data["source"] = source
    # data['cookies'] = driver.get_cookies()
    return data, driver


def get_url3(url, cache_page_link, thread_name, tag_to_detect="", encoding="utf-8"):
    data = {}
    number_of_attempts = 0

    while number_of_attempts < Crawl_path.num_of_attempts:
        try:
            # if not Crawl_path.current_proxy and str(thread_name).lower() == str(
            # 'Thread-0').lower():
            #     Crawl_path.current_proxy, Crawl_path.proxy_number = change_proxy(
            #     Crawl_path.proxies,
            #                                                                      Crawl_path.proxy_number, first=True)
            # elif str(thread_name).lower() == str('Thread-0').lower():
            #     Crawl_path.current_proxy, Crawl_path.proxy_number = change_proxy(
            #     Crawl_path.proxies,
            #                                                                      Crawl_path.proxy_number)
            # elif not Crawl_path.current_proxy:
            #     time.sleep(10)
            number_of_attempts += 1

            # Hiting the page
            p = subprocess.Popen(
                r"cscript c:\python36\lib\site-packages\vbpy_crawl.vbs "
                + str(url)
                + " "
                + str(cache_page_link),
                cwd=r"C:",
            )
            p.wait()
            data = read_file(cache_page_link, encoding=encoding)
            # data, session = [{'text': crawler.debug(), 'proxy': 'sample_proxy'}, session]
            if tag_to_detect != "" and tag_to_detect in data:
                continue
            if data:
                break
            else:
                pass
                # Crawl_path.current_proxy, Crawl_path.proxy_number = change_proxy(
                # Crawl_path.proxies,
                #                                                                  Crawl_path.proxy_number, blocked=True)
        except Exception:
            pass
            # Crawl_path.current_proxy, Crawl_path.proxy_number = change_proxy(Crawl_path.proxies,
            #                                                                  Crawl_path.proxy_number, blocked=True)

    return data


def clear_session_get_url3():
    p = subprocess.Popen(
        r"cscript c:\python36\lib\site-packages\vbpy_crawl_clear_session.vbs ",
        cwd=r"C:",
    )
    p.wait()


# def change_proxy(proxies_from_tool, proxy_number, blocked=False, first=False):
#     if not first:
#         try:
#             keyring.delete_password(str(Crawl_path.current_proxy['host']),
#             str(Crawl_path.current_proxy['username']))
#         except Exception as e:
#             pass
#     if blocked == True:
#         if Crawl_path.current_proxy in proxies_from_tool:
#             proxies_from_tool.remove(Crawl_path.current_proxy)
#     if len(proxies_from_tool) > 1:
#         if proxy_number < len(proxies_from_tool) - 1:
#             proxy_number += 1
#         else:
#             proxy_number = 0
#         proxy_to_change = proxies_from_tool[proxy_number]
#
#         # changing proxy on system
#         host_port = str(proxy_to_change['host']) + ':' + str(proxy_to_change['port'])
#         INTERNET_SETTINGS = winreg.OpenKey(winreg.HKEY_CURRENT_USER,
#                                            r'Software\Microsoft\Windows\CurrentVersion\Internet
#                                            Settings',
#                                            0, winreg.KEY_ALL_ACCESS)
#
#         def set_key(name, value):
#             _, reg_type = winreg.QueryValueEx(INTERNET_SETTINGS, name)
#             winreg.SetValueEx(INTERNET_SETTINGS, name, 0, reg_type, value)
#
#         set_key('ProxyEnable', 1)
#         set_key('ProxyOverride', u'*.local;<local>')  # Bypass the proxy for localhost
#         set_key('ProxyServer', str(host_port))
#
#         # Storing credentials for proxy
#         keyring.set_password(proxy_to_change['host'], proxy_to_change['username'],
#         proxy_to_change['password'])
#
#         return proxy_to_change, proxy_number
#     else:
#         print('Not enough proxies left')
#         os._exit(1)


def post_url(
        url,
        session=None,
        headers=None,
        cookies=None,
        params=None,
        encoding=None,
        proxies=None,
        verify=True,
        keep_proxy=False,
        blockwords=[],
        flagwords=[],
        request_type="post",
        keep_session=True,
        post_data=None,
):
    return get_url(
        url,
        session=session,
        headers=headers,
        cookies=cookies,
        params=params,
        encoding=encoding,
        proxies=proxies,
        verify=verify,
        keep_proxy=keep_proxy,
        blockwords=blockwords,
        flagwords=flagwords,
        request_type=request_type,
        keep_session=keep_session,
        post_data=post_data,
    )
    # res_text = ''
    # res_url = ''
    # res_cookies = ''
    # res_code = ''
    # data = {}
    # number_of_attempts = 0
    # if headers is None:
    #     headers = {
    #         'Connection': 'keep-alive',
    #         'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML,
    #         like Gecko) Chrome/63.0.3239.132 Safari/537.36',
    #         'Accept': '*/*',
    #         'Accept-Encoding': 'gzip, deflate, br',
    #         'Accept-Language': 'en-US,en;q=0.9'}
    # if session is None:
    #     session = requests.Session()
    # if params is None:
    #     params = {}
    # if cookies is None:
    #     cookies = {}
    # if post_data is None:
    #     post_data = {}
    # while (number_of_attempts < Crawl_path.num_of_attempts):
    #     try:
    #         number_of_attempts += 1
    #         proxies, strPOS = get_proxy(Crawl_path.proxies)
    #         session = requests.Session()
    #         response = session.post(url, headers=headers, proxies=proxies, params=params,
    #         cookies=cookies,
    #                                 timeout=Crawl_path.time_out, data=post_data, verify=verify)
    #         if encode is not None:
    #             response.encoding = str(encode)
    #         res_text = response.text
    #         res_url = response.url
    #         res_cookies = response.cookies.get_dict()
    #         res_code = response.status_code
    #         if res_text:
    #             break
    #     except Exception as e:
    #         pass
    # data['url'] = res_url
    # data['text'] = res_text
    # data['cookies'] = res_cookies
    # data['status_code'] = res_code
    #
    # return data, session


def start_tor():
    Crawl_path.tor = subprocess.Popen(
        r'C:\python36\Tor\Tor\tor.exe -f "C:\python36\Tor\Data\Tor\torrc"',
        cwd=r"C:\python36\Tor\Tor",
    )


def stop_tor():
    Crawl_path.tor.kill()


def get_input():
    current_path = Crawl_path.current_path
    input_file = current_path + "\\input_file.txt"
    input_url = read_csv(input_file, skip_header=True)
    return input_url


def get_number_of_threads():
    return Crawl_path.num_of_threads


def get_proxies():
    return Crawl_path.proxies


def midtext(main_text, start_text, end_text, pos=0):
    start_pos = -1
    end_pos = -1
    if start_text == "":
        start_pos = 0
    elif main_text.find(start_text, pos) != -1:
        start_pos = main_text.find(start_text, pos) + len(start_text)

    if end_text == "":
        end_pos = len(main_text)
    elif main_text.find(end_text, start_pos) != -1:
        end_pos = main_text.find(end_text, start_pos)

    if start_pos != -1 and end_pos != -1:
        return main_text[start_pos:end_pos]
    return ""


def midtext_copy(main_text, start_text, end_text, pos=0):
    if main_text.find(start_text, pos) != -1:
        start_pos = main_text.find(start_text, pos) + len(start_text)
        if main_text.find(end_text, start_pos) != -1:
            end_pos = main_text.find(end_text, start_pos)
            return main_text[start_pos:end_pos]
    return ""


def midtextall(main_text, start_text, end_text, pos=0):
    start_pos = pos
    end_pos = start_pos + 1
    list_of_found = []
    while end_pos > start_pos:
        if main_text.find(start_text, pos) != -1:
            start_pos = main_text.find(start_text, pos) + len(start_text)
            if main_text.find(end_text, start_pos) != -1:
                end_pos = main_text.find(end_text, start_pos)
                list_of_found.append(main_text[start_pos:end_pos])
                pos = end_pos
            else:
                break
        else:
            break
    return list_of_found


def clean(text):
    text = re.sub("\s+|\\\\t|\\t", " ", str(text).strip())
    text = re.sub("\\\\n|\\\\r|\\n|\\r", "", text.strip())
    text = ht.unescape(un.normalize("NFKD", text))
    return text


def xpath(source, xpath, loc=0, mode="t", sep="|"):
    text = ""
    try:
        element = source.xpath(xpath)
        if len(element):
            if mode == "tc":
                text = clean(element[loc].text_content())
                for tag in ["//script", "//style"]:
                    try:
                        junk_list = source.xpath(xpath + tag)
                        for junk in junk_list:
                            junk_text = clean(junk.text_content())
                            text = text.replace(junk_text, "")
                    except:
                        pass
            elif mode == "set":
                text = element
                if xpath.endswith("text()"):
                    text = [clean(x) for x in text]
            elif mode == "set_tc":
                text = sep.join(
                    [
                        clean(x.text_content())
                        for x in source.xpath(xpath)
                        if clean(x.text_content())
                    ]
                )
            else:
                text = clean(element[loc])
    except Exception as e:
        # print(e, xpath)
        pass
    return text


def json(value, *keys):
    try:
        for key in keys:
            value = value[key]
    except Exception as e:
        print("Key not found: " + str(e))
        return ""
    if isinstance(value, int):
        return str(value)
    elif isinstance(value, float):
        return str(value)
    elif not value:
        return ""
    elif value == "":
        return ""
    else:
        return value


def get_error_line(e):
    exc_type, exc_obj, exc_tb = sys.exc_info()
    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    return " | ".join([str(e), str(exc_type), str(fname), str(exc_tb.tb_lineno)])


def close_chrome(driver, profile_path):
    try:
        driver.quit()
    except:
        pass
    try:
        if Crawl_path.clear_selenium_session != "0":
            shutil.rmtree(profile_path)
    except Exception as e:
        pass


def send_text(driver, text, xpath, loc=0, click=False):
    try:
        driver.find_elements(By.XPATH, xpath)[loc].send_keys(text)
        if click:
            driver.find_elements(By.XPATH, xpath)[loc].send_keys(Keys.RETURN)
    except Exception as e:
        print(e)
        pass


def click(driver, xpath, loc=0):
    try:
        driver.find_elements(By.XPATH, xpath)[loc].click()
    except Exception as e:
        print(e)
        pass


def save_cache(data):
    file_name = (
            datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            + "_"
            + str(randint(0, 1000))
            + ".html"
    )
    cache_page = os.path.join(Crawl_path.cache_path, file_name)
    if Crawl_path.debug:
        write_file(cache_page, data, mode="w", encoding="utf8")
        return cache_page
    else:
        try:
            data = compress(data.encode("utf-8"), 9)
        except Exception as e:
            data = e.encode("utf-8")
        with open(cache_page, "wb") as fw:
            fw.write(data)
            fw.close()
        return f"http://{Crawl_path.master}:8000/crawl/cache_page?host={Crawl_path.host_address}&team_name={Crawl_path.team_name}&batch_name={Crawl_path.batch_name}&file_name={file_name}"


if __name__ == "__main__":
    Crawl_path(1,1,1,1,1,1,1,1).setup_chromedriver()