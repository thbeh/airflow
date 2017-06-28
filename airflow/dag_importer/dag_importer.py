
dag_import_spec = {}
def import_dags(self):
    # self._import_cinder()
    self._import_hostpath()

def _import_hostpath(self):
    global dag_import_spec
    spec = {'name': 'shared-data', 'hostPath': {}}
    spec['hostPath']['path'] = '/tmp/dags'
    dag_import_spec = spec

def _import_cinder(self):
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
    dag_import_spec = spec


