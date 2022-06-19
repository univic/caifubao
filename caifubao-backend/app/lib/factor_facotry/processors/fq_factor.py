from app.utilities import freshness_meta_helper


def calc_fq_factor(processor_obj):
    most_recent_factor_date = freshness_meta_helper.read_freshness_meta(processor_obj.stock_obj, 'fq_factor')

    processor_obj.quote_df["fq_factor"] = (processor_obj.quote_df["close"] / processor_obj.quote_df["previous_close"]).cumprod()
    processor_obj.quote_df["close_hfq"] = (processor_obj.quote_df["fq_factor"] * processor_obj.quote_df.iloc[0]['previous_close']).round(
        decimals=4)
    processor_obj.quote_df["open_hfq"] = (processor_obj.quote_df["open"] * (processor_obj.quote_df["close_hfq"] / processor_obj.quote_df["close"])).round(
        decimals=4)
    processor_obj.quote_df["high_hfq"] = (processor_obj.quote_df["high"] * (processor_obj.quote_df["close_hfq"] / processor_obj.quote_df["close"])).round(
        decimals=4)
    processor_obj.quote_df["low_hfq"] = (processor_obj.quote_df["low"] * (processor_obj.quote_df["close_hfq"] / processor_obj.quote_df["close"])).round(
        decimals=4)
    pass