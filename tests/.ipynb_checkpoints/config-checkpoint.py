# Setup and tear down the test
import subprocess
import json
import time
import logging
import pytest
from itertools import chain

# The container that will we can use for the transformer
default_container = "sslhep/servicex_func_adl_xaod_transformer:v0.4"
#default_container = "sslhep/servicex_code_gen_funcadl_xaod:support_subscripts"

def copy_file_to_container(container_name, file_uri, file_name):
    logging.info(f'Making sure the file {file_name} is local in the xrootd container.')
    cmd = f'cd /data/xrd; if [ ! -f {file_name} ]; then wget -O {file_name}-temp {file_uri}; mv {file_name}-temp {file_name}; fi'
    r = subprocess.run(['docker', 'exec', container_name, '/bin/bash', '-c', cmd])
    if r.returncode != 0:
        raise BaseException(f'Unable to docker into the xrootd container "{container_name}".')


def is_chart_running(name: str):
    'Is a chart of name `name` running?'
    result = subprocess.run(['helm', 'list', '--filter', name, '-q'], stdout=subprocess.PIPE)
    if result.returncode != 0:
        return False
    if result.stdout.decode('utf-8').strip() != name:
        return False
    return True


def stop_helm_chart(name: str):
    'Delete a chart if it is running'
    if not is_chart_running(name):
        return
    logging.info(f'Deleteing running chart {name}.')

    # It is running, lets do the delete now.
    subprocess.run(['helm', 'delete', name])

    # It often fails on windows - so we check the listing again.
    if is_chart_running(name):
        raise BaseException(f"Unable to delete the chart {name}!")

    logging.info(f'Waiting until all pods from chart {name} are off kubectl.')
    while True:
        s = get_pod_status(name)
        if len(s) == 0:
            logging.info(f'All pods from chart {name} are now deleted.')
            return
        time.sleep(1)


def get_pod_status(name: str):
    'Get the pod status for everything that starts with name'
    result = subprocess.run(['kubectl', 'get', 'pod', '-o', 'json'], stdout=subprocess.PIPE)
    data = json.loads(result.stdout)
    return [{'name': p['metadata']['name'], 'status': all([s['ready'] for s in p['status']['containerStatuses']])} for p in data['items'] if p['metadata']['name'].startswith(name)]


def start_helm_chart(chart_name: str, restart_if_running: bool = False, config_files=['../servicex-desktop-local.yaml']):
    '''
    Start the testing chart.

    Returns:
        chart-name      Name of the started chart.
        IP-Address      Where to contact anything running in the new chart
    '''
    ip_address = 'localhost'
    if is_chart_running(chart_name) and not restart_if_running:
        logging.info(f'Chart with name {chart_name} already running. We will use it for testing.')
        return (chart_name, ip_address)

    # Ok, make sure helm is clear of anything left over.
    stop_helm_chart(chart_name)
    logging.info(f'Starting chart {chart_name}.')

    # Start the chart now that the system is clean.
    cmd = ['helm', 'install', chart_name] + list(chain.from_iterable([['-f', f] for f in config_files])) + ['../ServiceX/servicex']
    result = subprocess.run(cmd, stdout=subprocess.PIPE)
    if result.returncode != 0:
        stop_helm_chart(chart_name)
        raise BaseException("Unable to start test helm chart")

    # Now, wait until it is up and running. The initial sleep is because if we don't, the containers
    # may not have status associated with them!
    logging.info(f'Waiting until all pods for chart {chart_name} are ready.')
    time.sleep(30)
    while True:
        time.sleep(10)
        status = get_pod_status(chart_name)
        is_ready = all(s['status'] for s in status)
        if is_ready:
            logging.info(f'All pods from chart {chart_name} are ready.')
            return (chart_name, ip_address)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    print(start_helm_chart('servicex-integrated-testing'))

class forward_port:
    'Run a process while we are up that forwards a port'
    def __init__ (self, pod_name:str, port_number:int):
        self._port = port_number
        self._pod = pod_name
    
    def __enter__(self):
        'Create the sub process that does the port forward.'
        self._proc = subprocess.Popen(["kubectl", "port-forward", self._pod, f"{self._port}:{self._port}"])
        pass

    def __exit__(self, p1, p2, p3):
        self._proc.kill()
        pass

def find_pod(helm_release_name:str, pod_name:str):
    'Find the pod name in the release and return the full name'
    pods = get_pod_status(helm_release_name)
    named_pods = [p['name'] for p in pods if p['name'].startswith(f"{helm_release_name}-{pod_name}")]
    assert len(named_pods) == 1
    return named_pods[0]

@pytest.yield_fixture(scope='session')
def running_backend():
    'Configure a backend that is up and running. Will not restart if it is running. Using the file server rather than the network for testing.'
    c_name = 'servicex-integrated-testing'

    (_, ip_address) = start_helm_chart(c_name, restart_if_running=False)
    with forward_port(find_pod(c_name, "servicex-app"), 5000):
        with forward_port(find_pod(c_name, "minio"), 9000):
            with forward_port(find_pod(c_name, 'rabbitmq'), 15672):
                yield f"http://{ip_address}:5000/servicex"


@pytest.yield_fixture(scope='session')
def restarted_backend():
    'Configure a backend that gets restarted if it is currently running.'
    c_name = 'servicex-integrated-testing'

    (_, ip_address) = start_helm_chart(c_name, restart_if_running=True)
    with forward_port(find_pod(c_name, "servicex-app"), 5000):
        with forward_port(find_pod(c_name, "minio"), 9000):
            yield f'http://{ip_address}:5000/servicex'

