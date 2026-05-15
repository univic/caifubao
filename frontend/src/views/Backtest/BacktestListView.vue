<template>
  <div class="list-page">
    <header class="page-hero">
      <div class="hero-content">
        <p class="eyebrow">Strategy Research</p>
        <h1 class="page-title">策略研究</h1>
        <p class="subtitle">基于评分引擎的选股策略验证。评分回测模拟 Top-N 组合收益，评分实验提供分桶校准与模型对比。</p>
      </div>
      <div class="hero-actions">
        <el-button type="primary" @click="$router.push('/backtest/new')">新建回测</el-button>
      </div>
    </header>

    <!-- Quick Access Cards -->
    <div class="cards-grid">
      <div class="strategy-card" @click="$router.push('/backtest/new')">
        <div class="card-icon" style="background: rgba(94, 106, 210, 0.12);">
          <el-icon :size="24" color="#7170ff"><TrendCharts /></el-icon>
        </div>
        <div class="card-content">
          <h3>评分回测</h3>
          <p>基于已验证评分预测的 Top-N 选股组合模拟，验证评分引擎的实盘选股能力。</p>
          <span class="card-action">创建回测 →</span>
        </div>
      </div>

      <div class="strategy-card" @click="$router.push('/score-experiments')">
        <div class="card-icon" style="background: rgba(16, 185, 129, 0.1);">
          <el-icon :size="24" color="#10b981"><DataAnalysis /></el-icon>
        </div>
        <div class="card-content">
          <h3>评分实验</h3>
          <p>创建校准实验，分析分桶命中率、组件贡献度、模型版本对比，定位评分因子有效性。</p>
          <span class="card-action">查看实验 →</span>
        </div>
      </div>
    </div>

    <!-- How-to Guide -->
    <div class="guide-card">
      <div class="guide-content">
        <h3>如何使用评分策略回测</h3>
        <ol>
          <li>
            <strong>选择评分周期：</strong>Score5（短期5日）、Score20（波段20日）、Score60（中期60日）。不同周期适合不同的选股频率。
          </li>
          <li>
            <strong>设置持仓数量：</strong>每日买入评分最高的前 N 只股票（1-50 只），均匀分配资金。
          </li>
          <li>
            <strong>指定回测区间：</strong>区间内必须有已验证（VERIFIED）的评分预测数据。系统将使用已验证的实际收益进行组合模拟。
          </li>
          <li>
            <strong>解读结果：</strong>查看累计收益率、年化收益、夏普比率、最大回撤和净值曲线，评估评分引擎的选股效果。
          </li>
        </ol>
        <div class="guide-note">
          <strong>注意：</strong>目前回测结果不会持久化存储，刷新页面后需要重新运行。后续版本将支持保存和回顾历史回测。
        </div>
      </div>
    </div>

    <div class="scoring-guide">
      <h3>评分策略 vs 传统技术信号</h3>
      <div class="comparison-grid">
        <div class="comparison-item">
          <div class="comp-title">评分策略回测</div>
          <p>基于多因子评分（信号强度、趋势、动量、突破、相对强弱、估值），系统性评估每只股票的上涨潜力，按评分排序选股。</p>
          <ul>
            <li>已验证预测的真实收益作为回测数据源</li>
            <li>每日 Top-N 等权组合</li>
            <li>持有至目标日期（T+周期）后退出</li>
          </ul>
        </div>
        <div class="comparison-item">
          <div class="comp-title">评分实验校准</div>
          <p>通过分桶分析、组件权重调整和模型版本对比，持续优化评分引擎的因子配置和预测准确率。</p>
          <ul>
            <li>按评分分桶统计命中率</li>
            <li>多模型版本同期对比</li>
            <li>因子贡献度归因分析</li>
          </ul>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { TrendCharts, DataAnalysis } from '@element-plus/icons-vue'
</script>

<style scoped>
.list-page {
  max-width: 900px;
  margin: 0 auto;
}

.page-hero {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  margin-bottom: 32px;
}

.hero-actions {
  padding-top: 8px;
}

.eyebrow {
  font-size: 12px;
  font-weight: 510;
  color: #8a8f98;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  margin: 0 0 8px 0;
}

.page-title {
  font-size: 32px;
  font-weight: 590;
  color: #f7f8f8;
  margin: 0 0 8px 0;
  letter-spacing: -0.02em;
}

.subtitle {
  font-size: 15px;
  color: #8a8f98;
  margin: 0;
  line-height: 1.6;
  max-width: 560px;
}

/* Quick Access Cards */
.cards-grid {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 16px;
  margin-bottom: 24px;
}

.strategy-card {
  display: flex;
  gap: 16px;
  background: rgba(255, 255, 255, 0.02);
  border: 1px solid rgba(255, 255, 255, 0.08);
  border-radius: 14px;
  padding: 22px 24px;
  cursor: pointer;
  transition: all 0.2s;
}

.strategy-card:hover {
  background: rgba(255, 255, 255, 0.04);
  border-color: rgba(255, 255, 255, 0.12);
  transform: translateY(-1px);
}

.card-icon {
  width: 48px;
  height: 48px;
  border-radius: 12px;
  display: flex;
  align-items: center;
  justify-content: center;
  flex-shrink: 0;
}

.card-content {
  flex: 1;
  min-width: 0;
}

.card-content h3 {
  font-size: 16px;
  font-weight: 590;
  color: #f7f8f8;
  margin: 0 0 6px 0;
}

.card-content p {
  font-size: 13px;
  color: #8a8f98;
  line-height: 1.5;
  margin: 0 0 8px 0;
}

.card-action {
  font-size: 13px;
  font-weight: 510;
  color: #7170ff;
}

/* Guide cards */
.guide-card {
  background: rgba(255, 255, 255, 0.02);
  border: 1px solid rgba(255, 255, 255, 0.08);
  border-radius: 12px;
  padding: 28px;
  margin-bottom: 24px;
}

.guide-content h3 {
  font-size: 18px;
  font-weight: 590;
  color: #f7f8f8;
  margin: 0 0 16px 0;
}

.guide-content ol {
  padding-left: 20px;
  margin: 0 0 16px 0;
}

.guide-content li {
  font-size: 14px;
  color: #d0d6e0;
  line-height: 1.8;
  margin-bottom: 6px;
}

.guide-content li strong {
  color: #f7f8f8;
  font-weight: 510;
}

.guide-note {
  background: rgba(94, 106, 210, 0.08);
  border: 1px solid rgba(94, 106, 210, 0.15);
  border-radius: 6px;
  padding: 12px 16px;
  font-size: 13px;
  color: #a5b4fc;
  line-height: 1.6;
}

.guide-note strong {
  color: #c7d2fe;
}

.scoring-guide {
  background: rgba(255, 255, 255, 0.02);
  border: 1px solid rgba(255, 255, 255, 0.08);
  border-radius: 12px;
  padding: 28px;
}

.scoring-guide h3 {
  font-size: 18px;
  font-weight: 590;
  color: #f7f8f8;
  margin: 0 0 16px 0;
}

.comparison-grid {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 24px;
}

.comparison-item {
  background: rgba(255, 255, 255, 0.02);
  border: 1px solid rgba(255, 255, 255, 0.05);
  border-radius: 8px;
  padding: 20px;
}

.comp-title {
  font-size: 15px;
  font-weight: 590;
  color: #f7f8f8;
  margin-bottom: 8px;
}

.comparison-item p {
  font-size: 13px;
  color: #8a8f98;
  line-height: 1.6;
  margin: 0 0 12px 0;
}

.comparison-item ul {
  padding-left: 18px;
  margin: 0;
}

.comparison-item li {
  font-size: 13px;
  color: #d0d6e0;
  line-height: 1.7;
}

@media (max-width: 768px) {
  .cards-grid {
    grid-template-columns: 1fr;
  }
  .comparison-grid {
    grid-template-columns: 1fr;
  }
  .page-title { font-size: 24px; }
}
</style>
