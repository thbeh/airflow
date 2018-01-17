# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import copy
import os
import six

from airflow.contrib.kubernetes.pod_generator import PodGenerator
from airflow.contrib.kubernetes.pod import Pod, Resources
from airflow.contrib.kubernetes.secret import Secret


'''
This class is a necessary building block to the kubernetes executor, which will be PR'd
shortly
'''


class WorkerGenerator(PodGenerator):
    def __init__(self, kube_config):
        PodGenerator.__init__(self, kube_config)
        self.volumes, self.volume_mounts = self._init_volumes_and_mounts()
        self.init_containers = self._init_init_containers()

    def _init_volumes_and_mounts(self):
        dags_volume_name = "airflow-dags"
        dags_path = os.path.join(self.kube_config.dags_folder,
                                 self.kube_config.git_subpath)
        volumes = [{
            'name': dags_volume_name
        }]
        volume_mounts = [{
            'name': dags_volume_name,
            'mountPath': dags_path,
            'readOnly': True
        }]

        # Mount the airflow.cfg file via a configmap the user has specified
        if self.kube_config.airflow_configmap:
            config_volume_name = "airflow-config"
            config_path = '{}/airflow.cfg'.format(self.kube_config.airflow_home)
            volumes.append({
                'name': config_volume_name,
                'configMap': {
                    'name': self.kube_config.airflow_configmap
                }
            })
            volume_mounts.append({
                'name': config_volume_name,
                'mountPath': config_path,
                'subPath': 'airflow.cfg',
                'readOnly': True
            })

        # A PV with the DAGs should be mounted
        if self.kube_config.dags_volume_claim:
            volumes[0]['persistentVolumeClaim'] = {
                "claimName": self.kube_config.dags_volume_claim}
            if self.kube_config.dags_volume_subpath:
                volume_mounts[0]["subPath"] = self.kube_config.dags_volume_subpath
        else:
            # Create a Shared Volume for the Git-Sync module to populate
            volumes[0]["emptyDir"] = {}
        return volumes, volume_mounts

    def _init_labels(self, dag_id, task_id, execution_date):
        return {
                   "airflow-slave": "",
                   "dag_id": dag_id,
                   "task_id": task_id,
                   "execution_date": execution_date
               },

    def _get_environment(self):
        env = super(self, WorkerGenerator).env_vars
        """Defines any necessary environment variables for the pod executor"""
        env['AIRFLOW__CORE__DAGS_FOLDER'] = '/tmp/dags'
        env['AIRFLOW__CORE__EXECUTOR'] = 'LocalExecutor'

        if self.kube_config.airflow_configmap:
            env['AIRFLOW__CORE__AIRFLOW_HOME'] = self.kube_config.airflow_home
        return env

    def _init_init_containers(self, volume_mounts):
        """When using git to retrieve the DAGs, use the GitSync Init Container"""
        # If we're using volume claims to mount the dags, no init container is needed
        if self.kube_config.dags_volume_claim:
            return []

        # Otherwise, define a git-sync init container
        init_environment = [{
            'name': 'GIT_SYNC_REPO',
            'value': self.kube_config.git_repo
        }, {
            'name': 'GIT_SYNC_BRANCH',
            'value': self.kube_config.git_branch
        }, {
            'name': 'GIT_SYNC_ROOT',
            'value': '/tmp'
        }, {
            'name': 'GIT_SYNC_DEST',
            'value': 'dags'
        }, {
            'name': 'GIT_SYNC_ONE_TIME',
            'value': 'true'
        }]
        if self.kube_config.git_user:
            init_environment.append({
                'name': 'GIT_SYNC_USERNAME',
                'value': self.kube_config.git_user
            })
        if self.kube_config.git_password:
            init_environment.append({
                'name': 'GIT_SYNC_PASSWORD',
                'value': self.kube_config.git_password
            })

        volume_mounts[0]['readOnly'] = False
        return [{
            'name': self.kube_config.git_sync_init_container_name,
            'image': self.kube_config.git_sync_container,
            'securityContext': {'runAsUser': 0},
            'env': init_environment,
            'volumeMounts': volume_mounts
        }]

    def make_worker_pod(self,
                        namespace,
                        pod_id,
                        dag_id,
                        task_id,
                        execution_date,
                        airflow_command,
                        kube_executor_config):
        cmds = ["bash", "-cx", "--"]
        labels = self._init_labels(dag_id, task_id, execution_date)
        PodGenerator.make_pod(self,
                              namespace=namespace,
                              pod_id=pod_id,
                              cmds=cmds,
                              arguments=airflow_command,
                              labels=labels,
                              kube_executor_config=kube_executor_config)

