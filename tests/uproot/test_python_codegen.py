from func_adl_servicex import ServiceXSourceUpROOT
from servicex import ServiceXDataset
from servicex import ignore_cache
from servicex.servicex_python_function import ServiceXPythonFunction


def run_query(input_filenames=None, tree_name=None):
    import functools, logging, numpy as np, awkward as ak, uproot, vector
    return (lambda selection: ak.zip(selection, depth_limit=(
        None if len(selection) == 1 else 1)) if not isinstance(selection,
                                                               ak.Array) else selection)(
        (lambda e: {'lep_pt': e['lep_pt']})((lambda input_files, tree_name_to_use: (
        logging.getLogger(__name__).info('Using treename=' + repr(tree_name_to_use)),
        uproot.lazy({input_file: tree_name_to_use for input_file in input_files}))[1])(
            (lambda source: [source] if isinstance(source, str) else source)(
                input_filenames if input_filenames is not None else ['bogus.root']),
            tree_name if tree_name is not None else 'mini')))

def test_python_codegen(endpoint_uproot, uproot_single_file):
    with ignore_cache():
        sx = ServiceXDataset([uproot_single_file],
                             backend_name=endpoint_uproot,
                             codegen="python",
                             status_callback_factory=None)

        selection = ServiceXPythonFunction(sx)
        encoded_selection = selection._encode_function(run_query)
        r = sx.get_data_pandas_df(encoded_selection)
        print(r)

