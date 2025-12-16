# Betting Strategy & Rules

**Version:** 1.0
**Last Updated:** 2025-12-16
**Bankroll:** $2,000 (initial)

---

## Table of Contents

1. [Overview](#1-overview)
2. [Expected Value Calculation](#2-expected-value-calculation)
3. [Bet Selection Filters](#3-bet-selection-filters)
4. [Position Sizing (Kelly Criterion)](#4-position-sizing-kelly-criterion)
5. [Risk Management](#5-risk-management)
6. [Per-Race Exposure Management](#6-per-race-exposure-management)
7. [Sensitivity Analysis](#7-sensitivity-analysis)
8. [Split Testing Framework](#8-split-testing-framework)
9. [Execution Guidelines](#9-execution-guidelines)
10. [Performance Monitoring](#10-performance-monitoring)

---

## 1. Overview

### 1.1 Strategy Philosophy

The betting strategy is designed to:

1. **Identify +EV opportunities** where model probability exceeds implied market probability
2. **Size bets appropriately** using fractional Kelly to balance growth and variance
3. **Manage risk** through per-race caps, daily limits, and conservative thresholds
4. **Track performance** to detect edge decay and calibration drift

### 1.2 Key Parameters

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| Kelly Fraction | 0.25 (quarter Kelly) | Reduce variance for small bankroll |
| Min EV Threshold | 8% | Conservative buffer for odds gap |
| Min Probability | 8% | Avoid extreme longshots |
| Min Overlay | 20% | Meaningful edge required |
| Max Odds | 15-1 | Avoid lottery tickets |
| Max Per-Race | 2% of bankroll | Limit single-race exposure |
| Daily Loss Limit | 10% of bankroll | Stop-loss protection |
| Min Bet Size | $2 | Platform minimum |

---

## 2. Expected Value Calculation

### 2.1 EV Formula

**For Parimutuel (TwinSpires):**

```
EV = (p_model × decimal_odds) - 1
```

Where:
- `p_model` = Model's calibrated probability
- `decimal_odds` = Market odds + 1 (e.g., 5-1 = 6.0)

**Example:**
```
p_model = 0.20 (20%)
odds = 5-1 (decimal = 6.0)
EV = (0.20 × 6.0) - 1 = 0.20 = +20%
```

### 2.2 Overlay Calculation

```
overlay = (p_model - p_market) / p_market
```

Where:
- `p_market` = 1 / decimal_odds (implied probability)

**Example:**
```
p_model = 0.20
odds = 5-1 → p_market = 1/6 = 0.167
overlay = (0.20 - 0.167) / 0.167 = 0.20 = 20%
```

### 2.3 Implementation

```python
class EVCalculator:
    def calculate_ev(self, p_model: float, decimal_odds: float) -> float:
        """
        Calculate expected value.

        Args:
            p_model: Model probability (0-1)
            decimal_odds: Decimal odds (e.g., 6.0 for 5-1)

        Returns:
            EV as decimal (0.08 = 8%)
        """
        return (p_model * decimal_odds) - 1

    def calculate_overlay(self, p_model: float, decimal_odds: float) -> float:
        """
        Calculate overlay percentage.
        """
        p_market = 1 / decimal_odds
        return (p_model - p_market) / p_market

    def calculate_implied_probability(self, decimal_odds: float) -> float:
        """
        Convert odds to implied probability.
        """
        return 1 / decimal_odds

    def odds_to_decimal(self, fractional: str) -> float:
        """
        Convert fractional odds (e.g., '5-1') to decimal.
        """
        parts = fractional.split('-')
        return (int(parts[0]) / int(parts[1])) + 1
```

### 2.4 Takeout Adjustment (Parimutuel)

For parimutuel pools, the takeout (~17% for win pools) is already reflected in the odds. No additional adjustment needed when using tote odds.

---

## 3. Bet Selection Filters

### 3.1 Filter Criteria

All filters must pass for a bet to qualify:

| Filter | Threshold | Purpose |
|--------|-----------|---------|
| EV | ≥ 8% | Compensate for odds gap uncertainty |
| Probability | ≥ 8% | Avoid extreme longshots |
| Overlay | ≥ 20% | Ensure meaningful edge |
| Odds | ≤ 15-1 | Avoid high-variance lottery bets |

### 3.2 Filter Implementation

```python
class BetFilter:
    def __init__(self, config: dict):
        self.min_ev = config['betting']['min_ev_threshold']           # 0.08
        self.min_prob = config['betting']['min_probability']          # 0.08
        self.min_overlay = config['betting']['min_overlay']           # 1.20
        self.max_odds = config['betting']['max_odds']                 # 15.0

    def qualifies(self, p_model: float, decimal_odds: float) -> bool:
        """
        Check if bet meets all filter criteria.
        """
        ev = (p_model * decimal_odds) - 1
        p_market = 1 / decimal_odds
        overlay = p_model / p_market

        checks = [
            ev >= self.min_ev,
            p_model >= self.min_prob,
            overlay >= self.min_overlay,
            decimal_odds <= (self.max_odds + 1)
        ]

        return all(checks)

    def get_rejection_reason(self, p_model: float, decimal_odds: float) -> str:
        """
        Return reason bet was rejected.
        """
        ev = (p_model * decimal_odds) - 1
        p_market = 1 / decimal_odds
        overlay = p_model / p_market

        if ev < self.min_ev:
            return f"EV too low: {ev:.1%} < {self.min_ev:.1%}"
        if p_model < self.min_prob:
            return f"Probability too low: {p_model:.1%} < {self.min_prob:.1%}"
        if overlay < self.min_overlay:
            return f"Overlay too low: {overlay:.1%} < {self.min_overlay:.1%}"
        if decimal_odds > (self.max_odds + 1):
            return f"Odds too high: {decimal_odds-1:.0f}-1 > {self.max_odds:.0f}-1"
        return "Qualified"
```

### 3.3 Filter Rationale

**8% EV Threshold:**
- Backtest may overestimate live edge by 30-50%
- 8% backtest EV → ~4-5.5% expected live EV
- Still positive after accounting for odds slippage

**8% Probability Threshold:**
- Longshots have high variance
- Model less reliable for rare outcomes
- At $2K bankroll, can't afford long losing streaks

**20% Overlay Threshold:**
- Ensures substantial edge, not marginal
- Market may be right on close calls
- Provides buffer for model uncertainty

**15-1 Max Odds:**
- Extreme longshots require many bets to realize edge
- High variance incompatible with small bankroll
- Focus on more certain opportunities

---

## 4. Position Sizing (Kelly Criterion)

### 4.1 Kelly Formula

**Full Kelly:**
```
f* = (p × b - q) / b
```

Where:
- `f*` = Optimal fraction of bankroll
- `p` = Probability of winning
- `q` = Probability of losing (1 - p)
- `b` = Net odds (decimal_odds - 1)

**Equivalent formula:**
```
f* = (p × decimal_odds - 1) / (decimal_odds - 1)
```

### 4.2 Fractional Kelly

Use **0.25× Kelly** (quarter Kelly):

```
f_bet = 0.25 × f*
```

**Rationale:**
- Small bankroll ($2K) requires variance reduction
- Model probability estimates have uncertainty
- Historical odds gap adds additional uncertainty
- Quarter Kelly reduces max drawdown significantly

### 4.3 Kelly Implementation

```python
class PositionSizer:
    def __init__(self, kelly_fraction: float = 0.25):
        self.kelly_fraction = kelly_fraction

    def calculate_kelly_fraction(self, p_model: float, decimal_odds: float) -> float:
        """
        Calculate full Kelly fraction.

        Returns fraction of bankroll (0-1).
        """
        b = decimal_odds - 1  # Net odds
        q = 1 - p_model

        kelly = (p_model * b - q) / b

        # Kelly should never be negative for +EV bets
        return max(0, kelly)

    def calculate_bet_size(
        self,
        p_model: float,
        decimal_odds: float,
        bankroll: float
    ) -> float:
        """
        Calculate recommended bet size.
        """
        full_kelly = self.calculate_kelly_fraction(p_model, decimal_odds)
        fractional_kelly = full_kelly * self.kelly_fraction
        return bankroll * fractional_kelly
```

### 4.4 Kelly Examples

| Probability | Odds | Full Kelly | 0.25× Kelly | Bet ($2K) |
|-------------|------|------------|-------------|-----------|
| 20% | 5-1 | 4.0% | 1.0% | $20 |
| 25% | 4-1 | 6.25% | 1.56% | $31 |
| 15% | 8-1 | 3.75% | 0.94% | $19 |
| 30% | 3-1 | 7.5% | 1.875% | $38 |
| 10% | 12-1 | 2.3% | 0.58% | $12 |

---

## 5. Risk Management

### 5.1 Bankroll Constraints

```python
class BankrollManager:
    def __init__(self, config: dict):
        self.initial_bankroll = config['bankroll']['initial']  # 2000.0
        self.max_per_race_pct = config['betting']['max_per_race_pct']  # 0.02
        self.daily_loss_limit_pct = config['betting']['daily_loss_limit_pct']  # 0.10
        self.min_bet = config['betting']['min_bet_amount']  # 2.0

    def apply_constraints(
        self,
        kelly_bet: float,
        bankroll: float,
        race_exposure_used: float,
        daily_loss: float
    ) -> float:
        """
        Apply all constraints to Kelly bet size.
        """
        # Max per-race constraint
        max_race_bet = bankroll * self.max_per_race_pct
        race_remaining = max_race_bet - race_exposure_used

        # Daily loss limit constraint
        daily_limit = bankroll * self.daily_loss_limit_pct
        daily_remaining = daily_limit - daily_loss

        # Apply all constraints
        constrained_bet = min(
            kelly_bet,
            max_race_bet,
            race_remaining,
            daily_remaining
        )

        # Enforce minimum bet
        if constrained_bet < self.min_bet:
            return 0.0  # Don't bet if below minimum

        # Round to whole dollar
        return round(constrained_bet)
```

### 5.2 Constraint Summary

| Constraint | Value | At $2K Bankroll |
|------------|-------|-----------------|
| Max per race | 2% | $40 |
| Daily loss limit | 10% | $200 |
| Minimum bet | $2 | $2 |

### 5.3 Daily Loss Tracking

```python
class DailyLossTracker:
    def __init__(self, daily_limit: float):
        self.daily_limit = daily_limit
        self.daily_loss = 0.0
        self.current_date = None

    def update(self, bet_amount: float, payout: float, race_date: date):
        """
        Update daily loss after bet result.
        """
        if self.current_date != race_date:
            self.daily_loss = 0.0
            self.current_date = race_date

        net = payout - bet_amount
        if net < 0:
            self.daily_loss += abs(net)

    def can_bet(self, proposed_bet: float) -> bool:
        """
        Check if proposed bet is within daily limit.
        """
        return (self.daily_loss + proposed_bet) <= self.daily_limit

    def remaining_limit(self) -> float:
        """
        Return remaining daily betting capacity.
        """
        return max(0, self.daily_limit - self.daily_loss)
```

---

## 6. Per-Race Exposure Management

### 6.1 Multiple Bets Per Race

**Key Insight:** Multiple +EV bets in the same race reduce variance (more likely to cash something) rather than creating correlated risk.

**Rules:**
- Maximum total exposure per race: 2% of bankroll ($40)
- No limit on number of horses per race
- Kelly sizing determines allocation within race cap
- If multiple horses qualify, scale proportionally

### 6.2 Scaling Implementation

```python
def scale_race_bets(
    recommendations: List[BetRecommendation],
    max_race_exposure: float
) -> List[BetRecommendation]:
    """
    Scale bets if total exceeds race cap.
    """
    total_recommended = sum(r.stake for r in recommendations)

    if total_recommended <= max_race_exposure:
        return recommendations  # No scaling needed

    # Calculate scale factor
    scale_factor = max_race_exposure / total_recommended

    # Apply scaling
    scaled = []
    for rec in recommendations:
        new_stake = round(rec.stake * scale_factor)
        if new_stake >= 2:  # Minimum bet
            rec.stake = new_stake
            scaled.append(rec)

    return scaled
```

### 6.3 Scaling Example

```
Race has 3 qualifying bets:
  Horse A: Kelly suggests $25
  Horse B: Kelly suggests $18
  Horse C: Kelly suggests $12
  Total: $55 (exceeds $40 cap)

Scale factor: 40/55 = 0.727

After scaling:
  Horse A: $25 × 0.727 = $18
  Horse B: $18 × 0.727 = $13
  Horse C: $12 × 0.727 = $9
  Total: $40 ✓
```

---

## 7. Sensitivity Analysis

### 7.1 Historical Odds Gap Problem

Backtests use final odds, but you bet against earlier odds:

- Final odds incorporate late information
- Adverse selection: your bet moves the line
- Typical edge degradation: 30-40%

### 7.2 Degradation Scenarios

| Scenario | Degradation | Interpretation |
|----------|-------------|----------------|
| Best Case | 20% | Strong signal persists |
| Expected | 35% | Typical smart money erosion |
| Worst Case | 55% | Highly efficient markets |

### 7.3 Sensitivity Table

Given backtest ROI, expected live ROI:

| Backtest ROI | Best (20%) | Expected (35%) | Worst (55%) |
|--------------|------------|----------------|-------------|
| +10% | +8.0% | +6.5% | +4.5% |
| +8% | +6.4% | +5.2% | +3.6% |
| +5% | +4.0% | +3.25% | +2.25% |
| +3% | +2.4% | +1.95% | +1.35% |
| +2% | +1.6% | +1.3% | +0.9% |

### 7.4 Minimum Backtest ROI

To achieve +2% live ROI (covering typical rake slippage):

- Best case scenario: Need +2.5% backtest
- Expected scenario: Need +3.1% backtest
- Worst case scenario: Need +4.4% backtest

**Recommendation:** Target +5% backtest ROI minimum before going live.

### 7.5 Morning Line Benchmarking

Run parallel backtest using morning line odds instead of final odds:

```python
def morning_line_backtest(races: List[Race]) -> BacktestResult:
    """
    Backtest using morning line odds only.
    Provides reality check on edge persistence.
    """
    for race in races:
        for entry in race.entries:
            # Use morning line instead of final odds
            entry.odds = entry.morning_line_odds

    return run_standard_backtest(races)
```

If morning line backtest shows positive ROI, edge likely persists.

---

## 8. Split Testing Framework

### 8.1 Track Classification

**High-Volume Tracks:**
- Churchill Downs (CD)
- Saratoga (SAR)
- Belmont (BEL)
- Gulfstream Park (GP)
- Santa Anita (SA)
- Del Mar (DMR)

**Regional Tracks:**
- Keeneland (KEE)
- Turfway Park (TP)
- Charles Town (CT)
- Penn National (PEN)
- Laurel Park (LRL)
- Tampa Bay Downs (TAM)

### 8.2 Hypothesis

Regional tracks may offer higher EV due to:
- Less sophisticated betting pools
- Less public information/analysis
- Higher variance (smaller fields)

But also risks:
- Lower liquidity (bets move odds more)
- More inconsistent data quality
- Smaller sample sizes

### 8.3 Test Design

```python
SPLIT_TEST_CONFIG = {
    'enabled': True,
    'allocation': {
        'high_volume': 0.50,
        'regional': 0.50
    },
    'min_bets_for_significance': 500
}
```

### 8.4 Tracked Metrics

| Metric | High-Volume | Regional |
|--------|-------------|----------|
| ROI | X.X% | X.X% |
| Sharpe Ratio | X.XX | X.XX |
| Win Rate | X.X% | X.X% |
| Avg EV | X.X% | X.X% |
| Odds Slippage | X.X% | X.X% |
| Sample Size | XXX | XXX |

### 8.5 Decision Criteria

After 500+ bets per segment:

| Outcome | Criteria | Action |
|---------|----------|--------|
| Regional wins | >2% ROI diff, p<0.05 | Shift to 70/30 regional |
| High-volume wins | >2% ROI diff, p<0.05 | Shift to 70/30 high-volume |
| No difference | p>0.05 | Maintain 50/50 |

---

## 9. Execution Guidelines

### 9.1 Daily Workflow

**Morning (6:00 AM - First Post):**
1. System generates day's race cards
2. Initial predictions computed
3. Review qualified bets on dashboard

**Pre-Race (T-10 minutes):**
1. Refresh odds for specific race
2. Recalculate EV and stakes
3. Final recommendation displayed
4. Manually place bet if qualified

**Post-Race:**
1. Log actual bet placed
2. Record final odds
3. Update P&L when results available

**End of Day:**
1. Review daily summary
2. Check calibration metrics
3. Log any execution notes

### 9.2 Manual Execution Checklist

Before placing each bet:

- [ ] Verify horse number matches recommendation
- [ ] Confirm odds still meet threshold
- [ ] Check stake amount is correct
- [ ] Verify sufficient platform balance
- [ ] Record time of bet placement

### 9.3 Bet Logging

Log every bet with:

| Field | Description |
|-------|-------------|
| recommendation_id | System recommendation ID |
| executed | Yes/No |
| actual_stake | Amount bet |
| actual_odds | Odds at execution |
| platform | TwinSpires/DraftKings |
| execution_time | Timestamp |
| skip_reason | If skipped, why |

### 9.4 Discipline Tracking

Track adherence to recommendations:

```python
def calculate_discipline_score(bet_log: List[BetLog]) -> dict:
    """
    Measure how well user followed recommendations.
    """
    total_recs = len(bet_log)
    executed = sum(1 for b in bet_log if b.executed)

    stake_deviations = [
        abs(b.actual_stake - b.recommended_stake) / b.recommended_stake
        for b in bet_log if b.executed
    ]

    return {
        'execution_rate': executed / total_recs,
        'avg_stake_deviation': np.mean(stake_deviations),
        'total_recommendations': total_recs,
        'total_executed': executed
    }
```

---

## 10. Performance Monitoring

### 10.1 Key Metrics

| Metric | Target | Frequency |
|--------|--------|-----------|
| ROI | > 1% live | Weekly |
| Sharpe Ratio | > 0.5 | Monthly |
| Max Drawdown | < 30% | Daily |
| Calibration (ECE) | < 0.03 | Weekly |
| Win Rate | ~12-15% | Weekly |

### 10.2 Alert Thresholds

| Condition | Alert Level | Action |
|-----------|-------------|--------|
| Daily loss > 8% | Warning | Review bets |
| Daily loss > 10% | Critical | Stop betting |
| ROI < 0% for 30 days | Warning | Review model |
| Calibration drift > 2σ | Warning | Recalibrate |
| Drawdown > 25% | Warning | Reduce stakes |
| Drawdown > 30% | Critical | Pause betting |

### 10.3 Performance Dashboard

```
═══════════════════════════════════════════════════════════════
PERFORMANCE SUMMARY - Last 30 Days
═══════════════════════════════════════════════════════════════
Bankroll:     $2,142 (+$142 / +7.1%)
Total Bets:   127
Win Rate:     14.2% (18 winners)
ROI:          +3.4%
Sharpe:       0.58
Max DD:       12.3% ($245)

Split Test:
  High-Volume: 68 bets, +2.8% ROI
  Regional:    59 bets, +4.1% ROI

Calibration:  ECE = 0.024 ✓
═══════════════════════════════════════════════════════════════
```

### 10.4 Weekly Review Checklist

- [ ] Review ROI vs target
- [ ] Check calibration metrics
- [ ] Analyze losing streaks
- [ ] Review odds slippage
- [ ] Compare split test segments
- [ ] Update threshold parameters if needed
- [ ] Document any model concerns

---

## Appendix A: Probability-to-Odds Conversion

| Probability | American | Fractional | Decimal |
|-------------|----------|------------|---------|
| 50% | -100 | 1-1 | 2.00 |
| 33% | +200 | 2-1 | 3.00 |
| 25% | +300 | 3-1 | 4.00 |
| 20% | +400 | 4-1 | 5.00 |
| 17% | +500 | 5-1 | 6.00 |
| 14% | +600 | 6-1 | 7.00 |
| 11% | +800 | 8-1 | 9.00 |
| 9% | +1000 | 10-1 | 11.00 |
| 6% | +1500 | 15-1 | 16.00 |

---

## Appendix B: Sample Bet Ticket

```
═══════════════════════════════════════════════════════════════
BET TICKET #20251216-CD-5-001
═══════════════════════════════════════════════════════════════
RACE:    Churchill Downs - Race 5 - 3:15 PM ET
DATE:    December 16, 2025

HORSE:   #4 FAST MOVER
BET:     $18 WIN

MODEL:   Probability: 22.3%
         EV: +12.1%
         Overlay: 33.5%

ODDS:    Current: 5-1 (min acceptable: 4-1)

BANKROLL: $2,000
         This bet: 0.9%
         Race total: $18
         Daily used: $42

STATUS:  ✓ QUALIFIED
═══════════════════════════════════════════════════════════════
```

---

*Document maintained by: Strategy Team*
*Review cycle: Monthly or after significant drawdown*
