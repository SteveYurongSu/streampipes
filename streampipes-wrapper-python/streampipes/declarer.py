#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


class Singleton(type):
    _instances = {}

    def __call__(self, *args, **kwargs):
        if self not in self._instances:
            self._instances[self] = super(Singleton, self).__call__(*args, **kwargs)
        return self._instances[self]


class DeclarerSingleton(metaclass=Singleton):

    """ EventProcessorManager holds running processor instances """
    _processors = {}

    def __init__(self):
        self.sepa_declarers = {}
        self.supported_protocols = {}
        self.supported_formats = {}
        self.route = '/'
        self.host = None
        self.port = None

    @classmethod
    def add(cls, processors=None):
        """ holds dict with <app_id, processor class>"""
        cls._processors = processors

    @classmethod
    def get_processor(cls, key):
        return cls._processors[key]

    @classmethod
    def get(cls):
        return cls._processors

    def get_declarers(self):
        return self.sepa_declarers

    def get_base_uri(self):
        return f'http://{self.host}:{self.port}{self.route}'
