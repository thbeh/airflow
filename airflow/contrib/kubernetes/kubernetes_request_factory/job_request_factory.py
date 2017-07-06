from .kubernetes_request_factory import *


class SimpleJobRequestFactory(KubernetesRequestFactory):
    """
        Request generator for a simple pod.
    """


    _yaml = """apiVersion: batch/v1
kind: Job
metadata:
  name: name
spec:
  template:
    metadata:
      name: name
    spec:
      containers:
      - name: base
        image: airflow-slave:latest
        imagePullPolicy: Never
        command: ["/usr/local/airflow/entrypoint.sh", "/bin/bash sleep 25"]
        volumeMounts:
          - name: shared-data
            mountPath: "/usr/local/airflow/dags"
      restartPolicy: Never
    """

    def create(self, pod):
        req = yaml.load(self._yaml)
        extract_name(pod, req)
        extract_labels(pod, req)
        extract_image(pod, req)
        extract_cmds(pod, req)
        if len(pod.node_selectors) > 0:
            extract_node_selector(pod, req)
        extract_secrets(pod, req)
        attach_volume_mounts(req)
        return req



