# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and

import unittest
from airflow.contrib.kubernetes.kubernetesjob import KubernetesJobBuilder
from airflow.contrib.kubernetes.pod_request import SimpleJobRequestFactory
from airflow import configuration




secrets = []
labels = []


class KubernetesJobRequestTest(unittest.TestCase):
    job_to_load = None
    job_req_factory = SimpleJobRequestFactory()
    def setUp(self):
        configuration.load_test_config()
        self.job_to_load = KubernetesJobBuilder(
            image='foo.image',
            cmds=['try', 'this', 'first']
        )


    def base_test(self):
        a = self.job_req_factory.create(self.job_to_load)
        print (a)
        self.assertEqual(a, 'foo')
