from featurestore import FeatureStore


def test_observe_returns_not_ok_until_all_features_seen():
    fs = FeatureStore(buffer_size=10)
    _, ok = fs.observe("device-a", "pump", "temperature", 50.0, ["pressure", "temperature"])
    assert not ok  # still waiting on pressure

    vector, ok = fs.observe("device-a", "pump", "pressure", 12.0, ["pressure", "temperature"])
    assert ok
    assert vector == [12.0, 50.0]  # ordered per feature_order


def test_observe_updates_last_known_value_across_calls():
    fs = FeatureStore(buffer_size=10)
    fs.observe("device-a", "pump", "pressure", 12.0, ["pressure", "temperature"])
    fs.observe("device-a", "pump", "temperature", 50.0, ["pressure", "temperature"])
    vector, ok = fs.observe("device-a", "pump", "temperature", 55.0, ["pressure", "temperature"])
    assert ok
    assert vector == [12.0, 55.0]


def test_observe_with_no_feature_order_never_returns_ok():
    fs = FeatureStore(buffer_size=10)
    _, ok = fs.observe("device-a", "unknown-type", "temperature", 50.0, [])
    assert not ok


def test_training_buffer_trims_to_buffer_size():
    fs = FeatureStore(buffer_size=3)
    for i in range(5):
        fs.observe("device-a", "pump", "pressure", float(i), ["pressure"])

    snapshot = fs.training_snapshot("pump")
    assert len(snapshot) == 3
    assert snapshot == [[2.0], [3.0], [4.0]]  # oldest trimmed, most recent kept


def test_machine_types_with_data_tracks_every_type_seen():
    fs = FeatureStore(buffer_size=10)
    fs.observe("device-a", "pump", "pressure", 1.0, ["pressure"])
    fs.observe("device-b", "compressor", "pressure", 1.0, ["pressure"])

    types = set(fs.machine_types_with_data())
    assert types == {"pump", "compressor"}
