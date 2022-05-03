from mongoengine import StringField, EmbeddedDocumentListField, DateTimeField, ReferenceField, ListField, \
    EmbeddedDocument, BooleanField, FloatField, IntField, EmbeddedDocumentField
from app.lib.database import db
from app.model.meta_data import MetaData


class StockExchange(db.Document):
    name = StringField()
    code = StringField()
    region = StringField()
    trade_calendar = ListField(DateTimeField())
    market_list = ListField(ReferenceField('FinanceMarket'))


class MarketOverview(EmbeddedDocument):
    date = DateTimeField()
    total_market_value = FloatField()
    average_pe = FloatField()
    transaction_volume = FloatField()
    transaction_amount = FloatField()
    report_date = DateTimeField()
    listing_number = IntField()
    turnover_rate = FloatField()
    circulating_market_value = FloatField()
    circulating_turnover_rate = FloatField()


class FinanceMarket(db.Document):
    name = StringField()
    code = StringField()
    exchange = ReferenceField(StockExchange)
    stock_list = ListField(ReferenceField('Stock'))
    trade_calendar = ListField(DateTimeField())
    overview = EmbeddedDocumentListField(MarketOverview)


class DailyQuote(EmbeddedDocument):
    date = DateTimeField()
    open = FloatField()
    close = FloatField()
    high = FloatField()
    low = FloatField()
    volume = IntField()
    trade_amount = FloatField()
    amplitude = FloatField()
    change_rate = FloatField()
    change_amount = FloatField()
    turnover_rate = FloatField()
    reinstatement_factor = FloatField()


class DataFreshnessMeta(EmbeddedDocument):
    daily_quote = DateTimeField()


class BasicStock(db.Document):
    """
    type:
    """
    meta = {'allow_inheritance': True}
    code = StringField(unique=True, required=True)
    name = StringField(required=True)
    exchange = ReferenceField(StockExchange)
    market = ReferenceField(FinanceMarket)
    daily_quote = EmbeddedDocumentListField('DailyQuote')
    data_freshness_meta = EmbeddedDocumentField(DataFreshnessMeta)


class IndividualStock(BasicStock):
    total_equity = IntField()
    outstanding_share = IntField()


class StockIndex(BasicStock):
    pass


class ValuationIndicator(EmbeddedDocument):
    date = DateTimeField()
    pe = FloatField()
    pe_ttm = FloatField()
    pb = FloatField()
    ps = FloatField()
    ps_ttm = FloatField()
    dv_ratio = FloatField()   # 股息率
    dv_ttm = FloatField()     # 股息率TTM
    total_mv = FloatField()   # 总市值


class FinancialStatus(EmbeddedDocument):
    date = DateTimeField()
    diluted_earnings_per_share = FloatField()  # 摊薄每股收益(元)
    weighted_earnings_per_share = FloatField()  # 加权每股收益(元)
    earnings_per_share_adjusted = FloatField()  # 每股收益_调整后(元)
    earnings_per_share_after_deducting_non_recurring_profits_and_losses = FloatField()  # 扣除非经常性损益后的每股收益(元)
    net_assets_per_share_before_adjustment = FloatField()  # 每股净资产_调整前(元)
    net_assets_per_share_adjusted = FloatField()  # 每股净资产_调整后(元)
    operating_cash_flow_per_share = FloatField()  # 每股经营性现金流(元)
    capital_reserve_per_share = FloatField()  # 每股资本公积金(元)
    undistributed_profit_per_share = FloatField()  # 每股未分配利润(元)
    adjusted_net_assets_per_share = FloatField()  # 调整后的每股净资产(元)
    profit_rate_of_total_assets = FloatField()  # 总资产利润率(%)
    profit_margin_of_main_business = FloatField()  # 主营业务利润率(%)
    net_profit_margin_of_total_assets = FloatField()  # 总资产净利润率(%)
    cost_profit_margin = FloatField()  # 成本费用利润率(%)
    operating_profit_margin = FloatField()  # 营业利润率(%)
    cost_rate_of_main_business = FloatField()  # 主营业务成本率(%)
    net_profit_margin_on_sales = FloatField()  # 销售净利率(%)
    return_on_equity = FloatField()  # 股本报酬率(%)
    return_on_net_assets = FloatField()  # 净资产报酬率(%)
    return_on_assets = FloatField()  # 资产报酬率(%)
    gross_profit_margin_of_sales = FloatField()  # 销售毛利率(%)
    proportion_of_three_expenses = FloatField()  # 三项费用比重
    proportion_of_non_main_business = FloatField()  # 非主营比重
    proportion_of_main_profits = FloatField()  # 主营利润比重
    dividend_payout_rate = FloatField()  # 股息发放率(%)
    return_on_investment = FloatField()  # 投资收益率(%)
    profit_from_main_business = FloatField()  # 主营业务利润(元)
    return_on_net_assets_2 = FloatField()  # 净资产收益率(%)
    weighted_return_on_net_assets = FloatField()  # 加权净资产收益率(%)
    net_profit_after_deducting_non_recurring_profit_and_loss = FloatField()  # 扣除非经常性损益后的净利润(元)
    growth_rate_of_main_business_income = FloatField()  # 主营业务收入增长率(%)
    net_profit_growth_rate = FloatField()  # 净利润增长率(%)
    growth_rate_of_net_assets = FloatField()  # 净资产增长率(%)
    growth_rate_of_total_assets = FloatField()  # 总资产增长率(%)
    turnover_rate_of_accounts_receivable = FloatField()  # 应收账款周转率(次)
    accounts_receivable_turnover_days = FloatField()  # 应收账款周转天数(天)
    inventory_turnover_days = FloatField()  # 存货周转天数(天)
    inventory_turnover_rate = FloatField()  # 存货周转率(次)
    turnover_rate_of_fixed_assets = FloatField()  # 固定资产周转率(次)
    total_asset_turnover_rate = FloatField()  # 总资产周转率(次)
    total_asset_turnover_days = FloatField()  # 总资产周转天数(天)
    turnover_rate_of_current_assets = FloatField()  # 流动资产周转率(次)
    turnover_days_of_current_assets = FloatField()  # 流动资产周转天数(天)
    turnover_rate_of_shareholders_equity = FloatField()  # 股东权益周转率(次)
    current_ratio = FloatField()  # 流动比率
    quick_ratio = FloatField()  # 速动比率
    cash_ratio = FloatField()  # 现金比率(%)
    interest_coverage_ratio_ = FloatField()  # 利息支付倍数
    long_term_debt_to_working_capital_ratio = FloatField()  # 长期债务与营运资金比率(%)
    shareholders_equity_ratio = FloatField()  # 股东权益比率(%)
    long_term_debt_ratio = FloatField()  # 长期负债比率(%)
    ratio_of_shareholders_equity_to_fixed_assets = FloatField()  # 股东权益与固定资产比率(%)
    debt_to_owners_equity_ratio = FloatField()  # 负债与所有者权益比率(%)
    ratio_of_long_term_assets_to_long_term_funds = FloatField()  # 长期资产与长期资金比率(%)
    capitalization_ratio = FloatField()  # 资本化比率(%)
    net_value_ratio_of_fixed_assets = FloatField()  # 固定资产净值率(%)
    capital_immobilization_ratio = FloatField()  # 资本固定化比率(%)
    equity_ratio = FloatField()  # 产权比率(%)
    liquidation_value_ratio = FloatField()  # 清算价值比率(%)
    proportion_of_fixed_assets = FloatField()  # 固定资产比重(%)
    asset_liability_ratio = FloatField()  # 资产负债率(%)
    total_assets = FloatField()  # 总资产(元)
    ratio_of_net_operating_cash_flow_to_sales_revenue = FloatField()  # 经营现金净流量对销售收入比率(%)
    return_on_operating_cash_flow_of_assets = FloatField()  # 资产的经营现金流量回报率(%)
    ratio_of_net_operating_cash_flow_to_net_profit = FloatField()  # 经营现金净流量与净利润的比率(%)
    ratio_of_net_operating_cash_flow_to_liabilities = FloatField()  # 经营现金净流量对负债比率(%)
    cash_flow_ratio = FloatField()  # 现金流量比率(%)
    short_term_stock_investment = FloatField()  # 短期股票投资(元)
    short_term_bond_investment = FloatField()  # 短期债券投资(元)
    short_term_other_operating_investment = FloatField()  # 短期其它经营性投资(元)
    long_term_stock_investment = FloatField()  # 长期股票投资(元)
    long_term_bond_investment = FloatField()  # 长期债券投资(元)
    long_term_other_operating_investment = FloatField()  # 长期其它经营性投资(元)
    accounts_receivable_within_1_year = FloatField()  # 1年以内应收帐款(元)
    accounts_receivable_within_1_2_years = FloatField()  # 1-2年以内应收帐款(元)
    accounts_receivable_within_2_3_years = FloatField()  # 2-3年以内应收帐款(元)
    accounts_receivable_within_3_years = FloatField()  # 3年以内应收帐款(元)
    prepayment_within_1_year = FloatField()  # 1年以内预付货款(元)
    prepayment_within_1_2_years = FloatField()  # 1-2年以内预付货款(元)
    prepayment_within_2_3_years = FloatField()  # 2-3年以内预付货款(元)
    prepayment_within_3_years = FloatField()  # 3年以内预付货款(元)
    other_receivables_within_1_year = FloatField()  # 1年以内其它应收款(元)
    other_receivables_within_1_2_years = FloatField()  # 1-2年以内其它应收款(元)
    other_receivables_within_2_3_years = FloatField()  # 2-3年以内其它应收款(元)
    other_receivables_within_3_years = FloatField()  # 3年以内其它应收款(元)
