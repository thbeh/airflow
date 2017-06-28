from airflow import AirflowException
import logging
print("starting dag integration")
logging.info("starting dag integ")

def _integrate_plugins():
    pass

def GetDagImporter():
    global DAG_IMPORTER

    _integrate_plugins()
    dag_importer_path = DAG_IMPORTER.split('.')

    if dag_importer_path[0] in globals():
        dag_importer_plugin = globals()[dag_importer_path[0]].__dict__[dag_importer_path[1]]()
        return dag_importer_plugin
    else:
        raise AirflowException("dag importer {0} not supported.".format(DAG_IMPORTER))

dag_import_spec = {}


def import_dags():
    # self._import_cinder()
    _import_hostpath()


def _import_hostpath():
    global dag_import_spec
    spec = {'name': 'shared-data', 'hostPath': {}}
    spec['hostPath']['path'] = '/tmp/dags'
    dag_import_spec = spec


def _import_cinder():
    '''
    kind: StorageClass
    apiVersion: storage.k8s.io/v1
    metadata:
        name: gold
    provisioner: kubernetes.io/cinder
    parameters:
        type: fast
    availability: nova
    :return: 
    '''
    global dag_import_spec
    spec = {}

    spec['kind'] = 'StorageClass'
    spec['apiVersion'] = 'storage.k8s.io/v1'
    spec['metatdata']['name'] = 'gold'
    spec['provisioner'] = 'kubernetes.io/cinder'
    spec['parameters']['type'] = 'fast'
    spec['availability'] = 'nova'

import_dags()
