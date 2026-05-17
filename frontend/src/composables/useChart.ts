/**
 * ECharts lifecycle composable — reduces boilerplate for chart init/dispose/resize.
 * Reusable across Dashboard, History, QuoteDetail, and other chart-heavy views.
 */
import { onBeforeUnmount, onMounted, ref } from 'vue'
import * as echarts from 'echarts'

export interface UseChartOptions {
  /** ECharts init options (theme, renderer, etc.) */
  initOpts?: Parameters<typeof echarts.init>[1]
  /** Whether to auto-resize on window resize (default true) */
  resize?: boolean
}

export function useChart(options: UseChartOptions = {}) {
  const { resize = true } = options
  const chartRef = ref<HTMLElement | null>(null)
  let instance: echarts.ECharts | null = null

  /** Create or return existing ECharts instance bound to chartRef. */
  function getInstance(): echarts.ECharts | null {
    if (instance) return instance
    if (!chartRef.value) return null
    instance = echarts.init(chartRef.value, options.initOpts)
    return instance
  }

  /** Set chart options, creating the instance if needed. */
  function setOption(
    option: echarts.EChartsOption,
    notMerge?: boolean
  ): echarts.ECharts | null {
    const inst = getInstance()
    if (!inst) return null
    inst.setOption(option, notMerge ?? true)
    return inst
  }

  /** Resize the chart instance. */
  function doResize() {
    instance?.resize()
  }

  /** Dispose the chart instance and clear ref. */
  function dispose() {
    instance?.dispose()
    instance = null
  }

  let _resizeHandler: (() => void) | null = null

  onMounted(() => {
    if (resize) {
      _resizeHandler = () => doResize()
      window.addEventListener('resize', _resizeHandler)
    }
  })

  onBeforeUnmount(() => {
    if (_resizeHandler) window.removeEventListener('resize', _resizeHandler)
    dispose()
  })

  return { chartRef, getInstance, setOption, doResize, dispose }
}

/**
 * Create a sparkline chart on a given element.
 * Returns the ECharts instance (caller is responsible for disposal).
 */
export function createSparkline(
  el: HTMLElement,
  data: number[],
  isUp: boolean
): echarts.ECharts {
  const instance = echarts.init(el)
  instance.setOption({
    grid: { left: 0, right: 0, top: 10, bottom: 10 },
    xAxis: { type: 'category', show: false },
    yAxis: { type: 'value', show: false, min: 'dataMin', max: 'dataMax' },
    series: [
      {
        data,
        type: 'line',
        smooth: true,
        symbol: 'none',
        lineStyle: {
          width: 2,
          color: isUp ? '#ef4444' : '#22c55e'
        },
        areaStyle: {
          color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
            { offset: 0, color: isUp ? 'rgba(239, 68, 68, 0.2)' : 'rgba(34, 197, 94, 0.2)' },
            { offset: 1, color: 'transparent' }
          ])
        }
      }
    ]
  })
  return instance
}

/**
 * Create a donut/pie chart. Returns instance (caller manages disposal).
 */
export function createDonutChart(
  el: HTMLElement,
  data: { value: number; name: string; color: string }[],
  opts?: { radius?: [string, string]; showLegend?: boolean }
): echarts.ECharts {
  const { radius = ['50%', '80%'], showLegend = true } = opts ?? {}
  const instance = echarts.init(el)
  instance.setOption({
    tooltip: { trigger: 'item', formatter: '{b}: {c} ({d}%)' },
    legend: showLegend
      ? {
          orient: 'vertical',
          right: 10,
          top: 'center',
          textStyle: { color: '#8a8f98' }
        }
      : undefined,
    series: [
      {
        name: '',
        type: 'pie',
        radius,
        avoidLabelOverlap: false,
        itemStyle: { borderRadius: 4, borderColor: '#191a1b', borderWidth: 2 },
        label: { show: false, position: 'center' },
        emphasis: {
          label: { show: true, fontSize: 16, fontWeight: 'bold', color: '#f7f8f8' }
        },
        labelLine: { show: false },
        data
      }
    ]
  })
  return instance
}
