# Factor Lab: 多重检验与探索性显著性

已有的 `factor_lab_runner evaluate` 能输出逐日 RankIC、Newey–West t、
扣费分层、换手和时间切分。这些指标不能解决「试了很多因子，挑最好看的一条」
造成的假阳性。此增量给 **整个因子 × horizon 检验族** 增加 Bonferroni 校正，
不改变原有经济闸门。

## 如何运行

```bash
cd datahub
PYTHONPATH=. python -m app.jobs.factor_lab_runner evaluate \
  --panel /data/lab.parquet --all --horizons 5,20,60 \
  --alpha 0.05 --output /data/sweep_corrected.json

# 如果此前/其他批次共预先定义了 51 次尝试，不能只按本次运行的组合数：
PYTHONPATH=. python -m app.jobs.factor_lab_runner evaluate \
  --panel /data/lab.parquet --factor momentum_20 --horizons 20 \
  --alpha 0.05 --hypotheses-count 51 \
  --output /data/momentum_h20_corrected.json
```

JSON 中 `multiple_testing` 记录校正方式、被评估的组合数、
`family_size`、`family_source` 和显著组合数。每个
`factors.<name>.horizons.<h>.significance` 包含：
`p_two_sided_nw_normal`、`p_bonferroni`、`reject_null` 和
`status`。状态有 `significant`、`not_significant`、
`insufficient_dates`（不足 120 个有效 IC 日期）以及
`unavailable_nw_t`。即使无法计算统计量，仍算作该检验族中的一次尝试。

计算使用已存在的 Newey–West t（horizon−1 滞后），**双侧标准正态近似**：
`p = erfc(|t_nw| / sqrt(2))`，
`p_adjusted = min(1, family_size * p)`。报告为「全样本、探索性检验」，
不是独立的样本外验证；重叠持有期、新兴市场厚尾、横截面依赖等仍可能令
这种近似不准确。Bonferroni 可在相关假设下控制家族一类错误，但并不能
矫正看过结果后重新定义整个检验族或对训练集重复调参的问题。

**必须明确检验族**：默认仅包括**本次命令实际计算的**因子-horizon 对。
如果此前已经试过其他组合，需通过 `--hypotheses-count` 声明至少覆盖
那些尝试；系统无法自动复原历史。一个因子重复筛选多个股票池、流动性下限、
时间窗口或模型参数，也应计入完整试验台账，必要时改为预注册 + 真正 OOS。

**显著不等于赚钱**：`significance.reject_null` 与现存
`gates.passed` 分开；前者不是 IC 符号、收益或组合净成本门槛。
下一步仍需市场状态分层、规模/行业中性化、容量约束、滚动 OOS 和实盘前瞻
观察，绝不因为此标志自动生成交易信号。
