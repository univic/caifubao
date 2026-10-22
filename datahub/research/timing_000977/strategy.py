# -*- coding: utf-8 -*-
"""Final strategy family for 000977 and the candidate grid used in validation.

Design (all inputs are causal, decided on the close):

``exposure = override(csi300 drawdown) * [ calm * ((1-w) + w * reversal) ]``
then capped by a volatility target, with a 5% no-trade band in execution.

* ``calm``  — 60d realised vol below the ``q`` quantile of its own trailing
  two years.  High-vol regimes carry most of this stock's crash risk.
* ``reversal`` — long after a ``entry`` 120-day decline, until the decline has
  recovered to ``exit`` (the rebound premium; the stock's momentum IC is
  negative at 20–250 day horizons).
* ``override`` — risk-off when CSI300 sits more than ``thr`` below its 250-day
  high, i.e. during a market-wide bear.
* ``vol_target`` — hold no more than ``target / realised_vol`` so risk stays
  roughly constant through calm and turbulent periods.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

import signals as S

TRADING_DAYS = 244.0


@dataclass(frozen=True)
class Config:
    calm_q: float | None = 0.70
    rev_entry: float | None = 0.25
    blend: float = 0.40
    override_thr: float | None = 0.20
    override_scale: float = 0.50
    vol_target: float | None = None
    band: float = 0.05
    calm_n: int = 60
    calm_lookback: int = 500
    rev_n: int = 120
    rev_exit: float = 0.05
    vt_n: int = 20

    @property
    def label(self) -> str:
        parts = []
        parts.append(
            f"calm{self.calm_n}_{self.calm_lookback}_q{int((self.calm_q or 0) * 100)}"
            if self.calm_q
            else "nocalm"
        )
        if self.rev_entry:
            parts.append(
                f"rev{self.rev_n}_{int(self.rev_entry * 100)}_{int(self.rev_exit * 100)}w{int(self.blend * 100)}"
            )
        if self.override_thr:
            parts.append(
                f"csiDD{int(self.override_thr * 100)}x{int(self.override_scale * 100)}"
            )
        if self.vol_target:
            parts.append(f"vt{int(self.vol_target * 100)}")
        return "|".join(parts)


class ExposureBuilder:
    """Caches the causal legs so a candidate scan is cheap."""

    def __init__(self, df: pd.DataFrame, index_frame: pd.DataFrame):
        self.df = df
        close = df["close"]
        self.csi = index_frame["close"].reindex(df.index).ffill()
        self.csi_dd250 = self.csi / self.csi.rolling(250).max() - 1.0
        self._calm: dict[tuple[int, int, float], pd.Series] = {}
        self._rev: dict[tuple[int, float, float], pd.Series] = {}
        self._vt: dict[tuple[float, int], pd.Series] = {}
        self._ov: dict[tuple[float, float], pd.Series] = {}
        self.close = close
        self._ret = {n: close.pct_change(n) for n in (120,)}

    def calm(self, n: int, lookback: int, q: float) -> pd.Series:
        key = (n, lookback, round(q, 4))
        if key not in self._calm:
            self._calm[key] = S.vol_regime(self.df, n, lookback, q)
        return self._calm[key]

    def rev(self, n: int, entry: float, exit_: float) -> pd.Series:
        key = (n, round(entry, 4), round(exit_, 4))
        if key not in self._rev:
            self._rev[key] = S.ret_reversal(self.df, n, entry, exit_)
        return self._rev[key]

    def vt(self, target: float, n: int) -> pd.Series:
        key = (round(target, 4), n)
        if key not in self._vt:
            self._vt[key] = S.vol_target(self.df, target, n)
        return self._vt[key]

    def override(self, thr: float, scale: float) -> pd.Series:
        key = (round(thr, 4), round(scale, 4))
        if key not in self._ov:
            vals = np.where(self.csi_dd250.to_numpy() < -thr, scale, 1.0)
            self._ov[key] = pd.Series(vals, index=self.df.index)
        return self._ov[key]

    def build(self, cfg: Config) -> pd.Series:
        n = len(self.df)
        if cfg.calm_q is None:
            base = np.ones(n)
        else:
            base = self.calm(cfg.calm_n, cfg.calm_lookback, cfg.calm_q).to_numpy()
        rev = np.zeros(n)
        if cfg.rev_entry is not None:
            rev = self.rev(cfg.rev_n, cfg.rev_entry, cfg.rev_exit).to_numpy()
        if cfg.calm_q is not None:
            expo = base * ((1.0 - cfg.blend) + cfg.blend * rev)
        else:
            expo = (1.0 - cfg.blend) + cfg.blend * rev
        if cfg.override_thr is not None:
            expo = expo * self.override(cfg.override_thr, cfg.override_scale).to_numpy()
        if cfg.vol_target is not None:
            expo = np.minimum(expo, self.vt(cfg.vol_target, cfg.vt_n).to_numpy())
        return pd.Series(np.clip(expo, 0.0, 1.0), index=self.df.index)


def candidates() -> list[Config]:
    """A priori grid; the walk-forward picks one config per block from this list."""
    out: list[Config] = []
    for q in (0.60, 0.70, 0.80):
        for w in (0.0, 0.25, 0.40, 0.50):
            for rev in (None, 0.20, 0.25, 0.30):
                if w > 0 and rev is None:
                    continue
                for thr, scale in ((None, 1.0), (0.15, 0.5), (0.20, 0.5), (0.20, 0.3)):
                    for vt in (None, 0.45, 0.55):
                        out.append(
                            Config(
                                calm_q=q,
                                rev_entry=rev,
                                blend=w,
                                override_thr=thr,
                                override_scale=scale,
                                vol_target=vt,
                            )
                        )
    # a couple of pure-reversal / pure-vol-target controls
    out.append(
        Config(
            calm_q=None,
            rev_entry=0.25,
            blend=1.0,
            override_thr=None,
            override_scale=1.0,
        )
    )
    out.append(
        Config(
            calm_q=None,
            rev_entry=0.20,
            blend=1.0,
            override_thr=None,
            override_scale=1.0,
        )
    )
    out.append(
        Config(
            calm_q=0.70,
            rev_entry=None,
            blend=0.0,
            override_thr=None,
            override_scale=1.0,
        )
    )
    dedup: dict[str, Config] = {}
    for cfg in out:
        dedup[cfg.label] = cfg
    return list(dedup.values())


FINAL = Config(
    calm_q=0.70, rev_entry=0.25, blend=0.50, override_thr=0.15, override_scale=0.50
)


def headline() -> dict[str, Config]:
    """Configs quoted in the report (balanced first)."""
    return {
        "balanced": Config(
            calm_q=0.70,
            rev_entry=0.25,
            blend=0.60,
            override_thr=0.20,
            override_scale=0.50,
        ),
        "defensive": Config(
            calm_q=0.70,
            rev_entry=0.25,
            blend=1.0,
            override_thr=None,
            override_scale=1.0,
        ),
        "aggressive": Config(
            calm_q=0.70,
            rev_entry=0.25,
            blend=0.40,
            override_thr=None,
            override_scale=1.0,
        ),
        "calm_only": Config(
            calm_q=0.70,
            rev_entry=None,
            blend=0.0,
            override_thr=None,
            override_scale=1.0,
        ),
    }


def signal_snapshot(builder: ExposureBuilder, cfg: Config) -> pd.DataFrame:
    expo = builder.build(cfg)
    return pd.DataFrame({"exposure": expo, "close": builder.close})
