from app.model.data_asset_status import (
    DataAssetStatus,
    STATUS_NO_DATA,
    STATUS_NOT_APPLICABLE,
    STATUS_OK,
    STATUS_STALE,
)


def test_data_asset_status_model_declares_unique_asset_key():
    assert DataAssetStatus._get_collection_name() == "data_asset_status"
    assert "latest_data_date" in DataAssetStatus._fields
    assert "coverage_rate" in DataAssetStatus._fields
    assert {STATUS_OK, STATUS_STALE, STATUS_NO_DATA, STATUS_NOT_APPLICABLE} == {
        "OK",
        "STALE",
        "NO_DATA",
        "NOT_APPLICABLE",
    }
