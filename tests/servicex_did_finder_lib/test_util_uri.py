import pytest
from servicex_did_finder_lib.util_uri import parse_did_uri


def test_plain_uri():
    r = parse_did_uri('forkit')

    assert r.did == "forkit"
    assert r.get_mode == "all"
    assert r.file_count == -1


def test_uri_with_mode_avail():
    r = parse_did_uri('forkit?get=available')

    assert r.did == "forkit"
    assert r.get_mode == "available"
    assert r.file_count == -1


def test_uri_with_mode_all():
    r = parse_did_uri('forkit?get=all')

    assert r.did == "forkit"
    assert r.get_mode == "all"
    assert r.file_count == -1


def test_uri_with_mode_bad():
    with pytest.raises(ValueError) as e:
        parse_did_uri('forkit?get=all_available')

    assert "all_available" in str(e.value)


def test_uri_with_file_count():
    r = parse_did_uri('forkit?files=10')

    assert r.did == "forkit"
    assert r.get_mode == "all"
    assert r.file_count == 10


def test_uri_with_file_count_neg():
    r = parse_did_uri('forkit?files=-1')

    assert r.did == "forkit"
    assert r.get_mode == "all"
    assert r.file_count == -1


def test_uri_with_file_and_get():
    r = parse_did_uri('forkit?files=10&get=available')

    assert r.did == "forkit"
    assert r.get_mode == "available"
    assert r.file_count == 10


def test_uri_with_other_params():
    r = parse_did_uri('forkit?get=available&stuff=hi&files=10')

    assert r.did == "forkit?stuff=hi"
    assert r.get_mode == "available"
    assert r.file_count == 10


def test_uri_rucio():
    'Make sure a uri with a scope in it works'
    r = parse_did_uri('mc16_13TeV:mc16_13TeV.361106.PowhegPythia8EvtGen_'
                      'AZNLOCTEQ6L1_Zee.deriv.DAOD_PHYS.e3601_e5984_s3126'
                      '_s3136_r10724_r10726_p4164?files=20')

    assert r.did == ("mc16_13TeV:mc16_13TeV.361106.PowhegPythia8EvtGen_"
                     "AZNLOCTEQ6L1_Zee.deriv.DAOD_PHYS.e3601_e5984_s3126_"
                     "s3136_r10724_r10726_p4164")
    assert r.get_mode == "all"
    assert r.file_count == 20
