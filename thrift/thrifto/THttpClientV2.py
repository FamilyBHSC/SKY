#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#

from io import BytesIO
import os
import socket
import sys
import warnings
import base64
import time
import urllib3
from six.moves import urllib
from six.moves import http_client
from urllib3 import HTTPConnectionPool, HTTPSConnectionPool
from .TTransport import TTransportBase
import six


class THttpClient(TTransportBase):
    def __init__(self, uri_or_host, port=None, path=None, maxconnections=1):
        self.maxconnections = maxconnections
        urllib3.disable_warnings()
        self.__custom_headers = {}
        self.__headers = {}
        if port is not None:
            warnings.warn(
                "Please use the THttpClient('http://host:port/path') syntax",
                DeprecationWarning,
                stacklevel=2)
            self.host = uri_or_host
            self.port = port
            assert path
            self.path = path
            self.scheme = 'http'
        else:
            parsed = urllib.parse.urlsplit(uri_or_host)
            self.scheme = parsed.scheme
            assert self.scheme in ('http', 'https')
            self.port = parsed.port
            self.host = parsed.hostname
            self.path = parsed.path
            if parsed.query:
                self.path += '?%s' % parsed.query
        try:
            proxy = urllib.request.getproxies()[self.scheme]
        except KeyError:
            proxy = None
        else:
            if urllib.request.proxy_bypass(self.host):
                proxy = None
        if proxy:
            parsed = urllib.parse.urlparse(proxy)
            self.realhost = self.host
            self.realport = self.port
            self.host = parsed.hostname
            self.port = parsed.port
            self.proxy_auth = self.basic_proxy_auth_header(parsed)
        else:
            self.realhost = self.realport = self.proxy_auth = None
        if self.scheme == 'http':
            pool_class = HTTPConnectionPool
        elif self.scheme == 'https':
            pool_class = HTTPSConnectionPool
        self.__pool = pool_class(self.host, self.port, maxsize=self.maxconnections)
        self.__wbuf = BytesIO()
        self.__http_response = None
        self.start = time.time()

    def using_proxy(self):
        return self.realhost is not None

    def open(self):
        if self.scheme == 'http':
            pool_class = HTTPConnectionPool
        elif self.scheme == 'https':
            pool_class = HTTPSConnectionPool
        self.__pool = pool_class(self.host, self.port, maxsize=self.maxconnections)

    def close(self):
        self.__resp = None

    def closeOnly(self):
        self.__http.close()

    def getHeaders(self):
        return self.headers

    def isOpen(self):
        return self.__resp is not None

    def setTimeout(self, ms):
        if ms is None:
            self.__pool.timeout = None
        else:
            self.__pool.timeout = ms / 1000.0

    def setCustomHeaders(self, headers):
        self.__custom_headers.update(headers)

    def read(self, sz):
        return self.__resp.read(sz)

    def write(self, buf):
        self.__wbuf.write(buf)

    def flush(self):
        # Pull data out of buffer
        data = self.__wbuf.getvalue()
        self.__wbuf = BytesIO()

        # Write headers
        self.__headers.update(self.__custom_headers)

        # HTTP request
        self.__resp = r = self.__pool.urlopen('POST', self.path, data, self.__headers, preload_content=False)
        
        #Get reply
        self.code, self.message, self.headers = r.status, r.reason, r.headers