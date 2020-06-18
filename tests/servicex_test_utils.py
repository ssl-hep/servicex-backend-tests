# Some utils to help out with writing tests.
import requests
from time import sleep
from minio import Minio
import tempfile
import numpy as np

def wait_for_request_done(backend_address: str, request_id: str) -> None:
    'Wait until a request has finished processing and files are ready to go'
    status_endpoint = f'{backend_address}/transformation/{request_id}/status'

    # Wait for the transform to complete.
    done = False
    info = None
    while not done:
        sleep(5)
        status = requests.get(status_endpoint)
        assert status.status_code == 200
        info = status.json()
        if ('files-remaining' in info) and (info['files-remaining'] is not None):
            done = int(info['files-remaining']) == 0
        else:
            print (f'missing "files-remaining" in response: {info}.')
    print(f'Finihsed processing. Final message: {info}')

def get_servicex_request_data(backend_address: str, request_id: str, as_data_type = 'pandas'):
    '''
    Get the data back in a table. Assumes request is done and there is only one result file.

    as_data_type can be either pandas or awkward, which determines the return type.
    '''
    # Now get the data
    # TODO: This should not be hardwired right now!
    # Really, it should come back in the request status!
    minio_endpoint = "localhost:9000"
    minio_client = Minio(minio_endpoint,
                    access_key='miniouser',
                    secret_key='leftfoot1',
                    secure=False)
    objects = list(minio_client.list_objects(request_id))
    assert len(objects) >= 1
    print(f'Found {len(objects)} objects to read back from minio')

    sample_files = list([file.object_name for file in objects])
    assert len(sample_files) > 0
    import uproot
    import uproot_methods
    import pandas
    with tempfile.TemporaryDirectory() as tmpdirname:
        def file_to_table(r_id, sample_file_name, output_name):
            minio_client.fget_object(r_id,
                                    sample_file_name,
                                    output_name)
            f_in = uproot.open(output_name)
            try:
                r = f_in[f_in.keys()[0]]
                assert r is not None
                if as_data_type == 'pandas':
                    return r.pandas.df()
                else:
                    return r.arrays()
                    
            finally:
                f_in._context.source.close()

        all_data = (file_to_table(request_id, s_file, f'{tmpdirname}/sample_{i}.root') for i, s_file in enumerate(sample_files))
        if len(sample_files) == 1:
            return list(all_data)[0]

        if as_data_type == 'pandas':
            return pandas.concat(all_data)
        else:
            frames = list(all_data)
            col_names = frames[0].keys()
            return {c: np.concatenate([ar[c] for ar in frames]) for c in col_names}
