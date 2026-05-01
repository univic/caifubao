def test_stock_signal_daily_model_shape():
    from app.model.signal import StockSignalDaily

    assert StockSignalDaily._get_collection_name() == "stock_signal_daily"
    assert StockSignalDaily._meta.get("indexes") == [
        {"fields": ["stock_code", "date", "signal_name"], "unique": True},
        ("date", "signal_name"),
        ("stock_code", "-date"),
        ("signal_name", "-date"),
        ("direction", "-date"),
    ]

    assert StockSignalDaily._fields["stock_code"].required is True
    assert StockSignalDaily._fields["signal_name"].required is True
    assert StockSignalDaily._fields["direction"].required is True
    assert StockSignalDaily._fields["signal_type"].required is True
    assert StockSignalDaily._fields["category"].default == "stock"
    assert StockSignalDaily._fields["signal_version"].default == "v1"
    assert type(StockSignalDaily._fields["price_snapshot"]).__name__ == "DictField"
    assert type(StockSignalDaily._fields["factor_snapshot"]).__name__ == "DictField"
    assert type(StockSignalDaily._fields["source_freshness"]).__name__ == "DictField"
