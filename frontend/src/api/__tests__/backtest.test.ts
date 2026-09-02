import { describe, expect, expectTypeOf, it } from 'vitest'

import {
  isScoreDrivenStrategy,
  type ComparePayload,
  type RunBacktestPayload,
  type RunMultiBacktestPayload,
  type ScanPayload,
} from '../backtest'

describe('backtest request contract', () => {
  it.each([
    'SCORE_THRESHOLD',
    'SCORE_MOMENTUM',
    'MULTI_HORIZON_CONSENSUS',
    'TOP_N_ROTATION',
  ])('classifies %s as score-driven', (strategy) => {
    expect(isScoreDrivenStrategy(strategy)).toBe(true)
  })

  it.each(['MA_CROSS', 'BUY_HOLD', ''])('does not classify %s as score-driven', (strategy) => {
    expect(isScoreDrivenStrategy(strategy)).toBe(false)
  })

  it('requires a model version in score-driven request types', () => {
    expectTypeOf<Extract<RunBacktestPayload, { strategy: 'SCORE_THRESHOLD' | 'SCORE_MOMENTUM' }>>()
      .toMatchTypeOf<{ model_version: string }>()
    expectTypeOf<RunMultiBacktestPayload>().toMatchTypeOf<{ model_version: string }>()
    expectTypeOf<ComparePayload>().toMatchTypeOf<{ model_version: string }>()
    expectTypeOf<Extract<ScanPayload, { strategy: 'SCORE_THRESHOLD' | 'SCORE_MOMENTUM' }>>()
      .toMatchTypeOf<{ model_version: string }>()
  })
})
