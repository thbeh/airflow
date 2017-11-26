# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest
import time
from uuid import uuid4
from airflow.utils.state import State
from tests.contrib.executors.integration.airflow_controller import (
    run_command, RunCommandError,
    run_dag, get_dag_run_state, dag_final_state, DagRunState,
    kill_scheduler, taint_minikube_cluster, untaint_minikube_cluster,
    get_num_pending_containers, get_all_containers, get_task_run_state
)


try:
    run_command("kubectl get pods")
except RunCommandError:
    SKIP_KUBE = True
else:
    SKIP_KUBE = False


class KubernetesExecutorTest(unittest.TestCase):

    @unittest.skipIf(SKIP_KUBE, 'Kubernetes integration tests are unsupported by this configuration')
    def test_kubernetes_executor_dag_runs_successfully(self):
        dag_id, run_id = "example_python_operator", uuid4().hex
        run_dag(dag_id, run_id)
        state = dag_final_state(dag_id, run_id, timeout=120)
        self.assertEquals(state, DagRunState.SUCCESS)

    @unittest.skipIf(SKIP_KUBE, 'Kubernetes integration tests are unsupported by this configuration')
    def test_start_dag_then_kill_scheduler_then_ensure_dag_succeeds(self):
        dag_id, run_id = "example_python_operator", uuid4().hex
        print("running DAG")
        run_dag(dag_id, run_id)

        self.assertEquals(get_dag_run_state(dag_id, run_id), DagRunState.RUNNING)

        i = 0
        while get_num_pending_containers() > 0:
            self.assertLess(i, 100, "there was an infinite loop caused by this test")
            print("loop")
            time.sleep(1)
            i += 1

        kill_scheduler()

        self.assertEquals(dag_final_state(dag_id, run_id, timeout=180), DagRunState.SUCCESS)

    @unittest.skipIf(SKIP_KUBE, 'Kubernetes integration tests are unsupported by this configuration')
    def test_kubernetes_executor_config_works(self):
        dag_id, run_id = "example_kubernetes_executor", uuid4().hex
        run_dag(dag_id, run_id)

        self.assertEquals(get_dag_run_state(dag_id, run_id), DagRunState.RUNNING)
        self.assertEquals(dag_final_state(dag_id, run_id, timeout=180), DagRunState.SUCCESS)



    def untaint(self):
        untaint_minikube_cluster()

    @unittest.skipIf(SKIP_KUBE, 'Kubernetes integration tests are unsupported by this configuration')
    def test_throttle_of_pending_tasks(self):
        dag_id, run_id = "example_python_operator", uuid4().hex
        print("tainting cluster")

        run_dag(dag_id, run_id)
        time.sleep(5)
        while get_task_run_state(dag_id,run_id, task_id='print_the_context') != State.RUNNING:
            time.sleep(1)
        taint_minikube_cluster()
        while get_task_run_state(dag_id,run_id, task_id='print_the_context') != State.SUCCESS:
            time.sleep(1)
        time.sleep(15)
        num_pending_containers = get_num_pending_containers()
        print("num pending: {}".format(num_pending_containers))
        dag_id2, run_id2 = "example_kubernetes_executor", uuid4().hex
        run_dag(dag_id2, run_id2)
        time.sleep(10)
        num_pending_containers2 = get_num_pending_containers()
        self.assertEqual(num_pending_containers, num_pending_containers2)
        untaint_minikube_cluster()
        self.assertEquals(dag_final_state(dag_id, run_id, timeout=180), DagRunState.SUCCESS)
        self.assertEquals(dag_final_state(dag_id2, run_id2, timeout=180), DagRunState.SUCCESS)


if __name__ == "__main__":
    pass
