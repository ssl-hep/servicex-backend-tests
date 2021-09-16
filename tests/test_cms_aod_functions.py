from func_adl_servicex import ServiceXSourceCMSRun1AOD


def test_func_adl_simple_jet_pts(endpoint_cms):
    '''Test simple query to CMS backend
    '''
    query = (ServiceXSourceCMSRun1AOD("cernopendata://1507",
                                      backend=endpoint_cms)
        .SelectMany('lambda e: e.TrackMuons("globalMuons")')
        .Select('lambda m: m.pt()')
        .AsPandasDF("JetPt")
        .value())

    retrieved_data = query.JetPt
    retrieved_data = retrieved_data.to_numpy()
    print(retrieved_data.shape)
    assert len(retrieved_data) == 398291


def test_metadata(endpoint_cms):
    '''Test simple query to CMS with metadata
    '''
    query = (ServiceXSourceCMSRun1AOD("cernopendata://1507",
                                      backend=endpoint_cms)
        .SelectMany('lambda e: e.TrackMuons("globalMuons")')
        .MetaData({
            'metadata_type': 'add_method_type_info',
            'type_string': 'reco::Track',
            'method_name': 'pt',
            'return_type': 'double',
            'is_pointer': 'False',
        })
        .Select('lambda m: m.pt()')
        .AsPandasDF("JetPt")
        .value())

    retrieved_data = query.JetPt
    retrieved_data = retrieved_data.to_numpy()
    print(retrieved_data.shape)
    assert len(retrieved_data) == 398291
