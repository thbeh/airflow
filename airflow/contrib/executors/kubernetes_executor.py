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
# limitations under the License.

import calendar
import logging
import time
import multiprocessing
from airflow.contrib.kubernetes.kubernetes_pod_builder import KubernetesPodBuilder
from airflow.contrib.kubernetes.kubernetes_helper import KubernetesHelper
from queue import Queue
from datetime import datetime
from kubernetes import watch
from airflow import settings
from airflow.contrib.kubernetes.kubernetes_request_factory import SimplePodRequestFactory
from airflow.contrib.kubernetes.pod_launcher import incluster_namespace
from airflow.executors.base_executor import BaseExecutor
from airflow.models import TaskInstance
from airflow.utils.state import State
from airflow import configuration


# TODO this is just for proof of concept. remove before merging.

class KubeConfig:
    core_section = "core"
    kubernetes_section = "kubernetes"

    @staticmethod
    def safe_get(section, option, default):
        try:
            return configuration.get(section, option)
        except AirflowConfigException:
            return default

    @staticmethod
    def safe_getboolean(section, option, default):
        try:
            return configuration.getboolean(section, option)
        except AirflowConfigException:
            return default

    def __init__(self):
        self.kube_image = configuration.get('core', 'k8s_image')
        self.git_repo = configuration.get('core', 'k8s_git_repo')
        self.git_branch = configuration.get('core', 'k8s_git_branch')


class KubernetesJobWatcher(multiprocessing.Process, object):
    def __init__(self, namespace, watcher_queue):
        self.logger = logging.getLogger(__name__)
        multiprocessing.Process.__init__(self)
        self.namespace = namespace
        self.watcher_queue = watcher_queue

    def run(self):
        from airflow.models import KubeResourceVersion
        from airflow import settings

        kube_client = get_kube_client()
        resource_version = KubeResourceVersion.get_current_resource_version(session=settings.Session())
        while True:
            try:
                resource_version = self._run(kube_client, resource_version)
            except Exception:
                self.logger.exception("Unknown error in KubernetesJobWatcher. Failing")
                raise
            else:
                self.logger.warn("Watch died gracefully, starting back up with: "
                                 "last resource_version: {}".format(resource_version))

    def _run(self, kube_client, resource_version):
        self.logger.info("Event: and now my watch begins starting at resource_version: {}".format(resource_version))
        watcher = watch.Watch()

    def _run(self):
        self.logger.info("Event: and now my watch begins")
        self.logger.info("Event: proof of image change")
        self.logger.info("Event: running {} with {}".format(str(self._watch_function),
                                                            self.namespace))
        for event in self._watch.stream(self._watch_function, self.namespace):
            task = event['object']
            self.logger.info("Event: {} had an event of type {}".format(task.metadata.name,
                                                                        event['type']))
            self.process_status(task.metadata.name, task.status.phase, task.metadata.labels)

        return last_resource_version

    def process_status(self, pod_id, status, labels, resource_version):
        if status == 'Pending':
            self.logger.info("Event: {} Pending".format(pod_id))
        elif status == 'Failed':
            self.logger.info("Event: {} Failed".format(pod_id))
            self.watcher_queue.put((pod_id, State.FAILED, labels, resource_version))
        elif status == 'Succeeded':
            self.logger.info("Event: {} Succeeded".format(pod_id))
            self.watcher_queue.put((pod_id, None, labels, resource_version))
        elif status == 'Running':
            self.logger.info("Event: {} is Running".format(pod_id))
        else:
            self.logger.warn("Event: Invalid state: {} on pod: {} with labels: {} "
                             "with resource_version: {}".format(status, pod_id, labels, resource_version))


class AirflowKubernetesScheduler(object):
    def __init__(self, task_queue, result_queue):
        self.logger = logging.getLogger(__name__)
        self.logger.info("creating kubernetes executor")
        self.kube_config = KubeConfig()
        self.task_queue = task_queue
        self.namespace = incluster_namespace()
        self.logger.info("k8s: using namespace {}".format(self.namespace))
        self.result_queue = result_queue
        self.watcher_queue = multiprocessing.Queue()
        self.helper = KubernetesHelper()
        w = KubernetesJobWatcher(self.helper.pod_api.list_namespaced_pod, self.namespace, self.watcher_queue)
        w.start()

    def run_next(self, next_job):
        """

        The run_next command will check the task_queue for any un-run jobs.
        It will then create a unique job-id, launch that job in the cluster,
        and store relevent info in the current_jobs map so we can track the job's
        status

        :return:

        """
        self.logger.info('k8s: job is {}'.format(str(next_job)))
        key, command = next_job
        dag_id, task_id, execution_date = key
        self.logger.info("running for command {}".format(command))
        pod_id = self._create_job_id_from_key(key=key)
        pod = KubernetesPodBuilder(
            image=self.kube_config.kube_image,
            cmds=["bash", "-cx", "--"],
            args=[cmd_args],
            kub_req_factory=SimplePodRequestFactory(),
            namespace=self.namespace
        )
        pod.set_image_pull_policy("IfNotPresent")
        pod.add_env_variables({"AIRFLOW__CORE__EXECUTOR": "LocalExecutor"})
        pod.add_name(pod_id)
        pod.add_labels({
            "dag_id": dag_id,
            "task_id": task_id,
            "execution_date": self._datetime_to_label_safe_datestring(execution_date)
        })
        pod.launch()

        self.logger.info("k8s: Job created!")

    def delete_job(self, key):
        job_id = self._create_job_id_from_key(key)
        self.helper.delete_pod(job_id, namespace=self.namespace)

    def sync(self):
        """

        The sync function checks the status of all currently running kubernetes jobs.
        If a job is completed, it's status is placed in the result queue to
        be sent back to the scheduler.

        :return:

        """
        self._health_check_kube_watcher()
        while not self.watcher_queue.empty():
            self.end_task()

    def end_task(self):
        job_id, state, labels = self.watcher_queue.get()
        logging.info("Attempting to finish job; job_id: {}; state: {}; labels: {}".format(job_id, state, labels))
        key = self._labels_to_key(labels)
        if key:
            self.logger.info("finishing job {}".format(key))
            self.result_queue.put((key, state, pod_id, resource_version))

    @staticmethod
    def _strip_unsafe_kubernetes_special_chars(string):
        """
        Kubernetes only supports lowercase alphanumeric characters and "-" and "." in the pod name
        However, there are special rules about how "-" and "." can be used so let's only keep alphanumeric chars
        see here for detail: https://kubernetes.io/docs/concepts/overview/working-with-objects/names/
        :param string:
        :return:
        """
        return ''.join(ch.lower() for ind, ch in enumerate(string) if ch.isalnum())

    @staticmethod
    def _make_safe_pod_id(safe_dag_id, safe_task_id, safe_uuid):
        """
        Kubernetes pod names must be <= 253 chars and must pass the following regex for validation
        "^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$"
        :param safe_dag_id: a dag_id with only alphanumeric characters
        :param safe_task_id: a task_id with only alphanumeric characters
        :param random_uuid: a uuid
        :return:
        """
        MAX_POD_ID_LEN = 253

        safe_key = safe_dag_id + safe_task_id

        safe_pod_id = safe_key[:MAX_POD_ID_LEN-len(safe_uuid)-1] + "-" + safe_uuid

        return safe_pod_id

    @staticmethod
    def _create_pod_id(dag_id, task_id):
        safe_dag_id = AirflowKubernetesScheduler._strip_unsafe_kubernetes_special_chars(dag_id)
        safe_task_id = AirflowKubernetesScheduler._strip_unsafe_kubernetes_special_chars(task_id)
        safe_uuid = AirflowKubernetesScheduler._strip_unsafe_kubernetes_special_chars(uuid4().hex)
        return AirflowKubernetesScheduler._make_safe_pod_id(safe_dag_id, safe_task_id, safe_uuid)

    @staticmethod
    def _label_safe_datestring_to_datetime(string):
        """
        Kubernetes doesn't like ":" in labels, since ISO datetime format uses ":" but not "_" let's replace ":" with "_"
        :param string: string
        :return: datetime.datetime object
        """
        return parser.parse(string.replace("_", ":"))

    @staticmethod
    def _datetime_to_label_safe_datestring(datetime_obj):
        """
        Kubernetes doesn't like ":" in labels, since ISO datetime format uses ":" but not "_" let's replace ":" with "_"
        :param datetime_obj: datetime.datetime object
        :return: ISO-like string representing the datetime
        """
        return datetime_obj.isoformat().replace(":", "_")

    def _labels_to_key(self, labels):
        try:
            return labels["dag_id"], labels["task_id"], self._label_safe_datestring_to_datetime(labels["execution_date"])
        except Exception as e:
            self.logger.warn("Error while converting labels to key; labels: {}; exception: {}".format(
                labels, e
            ))
            return None


class KubernetesExecutor(BaseExecutor):
    def start(self):
        from airflow.models import KubeResourceVersion
        self.logger.info('k8s: starting kubernetes executor')
        self._session = settings.Session()
        self.task_queue = Queue()
        self.result_queue = Queue()
        self.kub_client = AirflowKubernetesScheduler(self.task_queue, self.result_queue)

    def sync(self):
        self.kub_client.sync()

        last_resource_version = None
        while not self.result_queue.empty():
            results = self.result_queue.get()
            key, state, pod_id, resource_version = results
            last_resource_version = resource_version
            self.logger.info("Changing state of {}".format(results))
            self._change_state(key, state, pod_id)

        self.KubeResourceVersion.checkpoint_resource_version(last_resource_version, session=self._session)

        if not self.task_queue.empty():
            (key, command) = self.task_queue.get()
            self.kub_client.run_next((key, command))

    def terminate(self):
        pass

    def change_state(self, key, state):
        self.logger.info("k8s: setting state of {} to {}".format(key, state))
        if state != State.RUNNING:
            # self.kub_client.delete_job(key)
            self.logger.info("current running {}".format(self.running))
            self.running.pop(key)
        self.event_buffer[key] = state
        (dag_id, task_id, ex_time) = key
        item = self._session.query(TaskInstance).filter_by(
            dag_id=dag_id,
            task_id=task_id,
            execution_date=ex_time
        ).one()

        if item.state == State.RUNNING or item.state == State.QUEUED:
            item.state = state
            self._session.add(item)
            self._session.commit()

    def end(self):
        self.logger.info('ending kube executor')
        self.task_queue.join()

    def execute_async(self, key, command, queue=None):
        self.logger.info("k8s: adding task {} with command {}".format(key, command))
        self.task_queue.put((key, command))
