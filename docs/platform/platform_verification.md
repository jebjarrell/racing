# Platform Verification Checklist

**Version:** 1.0
**Last Updated:** 2025-12-16
**Status:** Pre-Implementation

---

## Table of Contents

1. [Overview](#1-overview)
2. [Legal Verification](#2-legal-verification)
3. [Platform Comparison](#3-platform-comparison)
4. [TwinSpires Verification](#4-twinspires-verification)
5. [DraftKings Verification](#5-draftkings-verification)
6. [Manual Workflow Testing](#6-manual-workflow-testing)
7. [Decision Matrix](#7-decision-matrix)
8. [Final Recommendation](#8-final-recommendation)

---

## 1. Overview

### 1.1 Purpose

Before going live with the betting system, verify:

1. **Legal compliance** for Kentucky residents
2. **Platform functionality** for manual betting workflow
3. **Odds formats** and data availability
4. **Account requirements** and limits

### 1.2 Platforms Under Consideration

| Platform | Type | Primary Use |
|----------|------|-------------|
| TwinSpires | Parimutuel | Traditional tote betting |
| DraftKings | Fixed-odds | Fixed-odds horse racing |

---

## 2. Legal Verification

### 2.1 Kentucky Residency

**Current Status:** Kentucky

**Verification Tasks:**

- [ ] **TwinSpires Legal:** Confirm TwinSpires is legal for KY residents
  - TwinSpires is headquartered in Kentucky (Lexington)
  - Owned by Churchill Downs Inc.
  - Expected: Legal ✓

- [ ] **DraftKings Racing Legal:** Confirm DraftKings Racing is legal for KY residents
  - DraftKings offers horse racing in select states
  - Need to verify KY is included
  - Expected: Verify ⚠️

- [ ] **Age Verification:** Confirm 21+ requirement met
  - Standard age requirement for horse racing betting

- [ ] **Tax Implications:** Document tax reporting requirements
  - W-2G forms for winnings > $600 at 300:1 odds
  - Keep records of all wagers

### 2.2 Legal Resources

| Resource | URL |
|----------|-----|
| Kentucky Horse Racing Commission | khrc.ky.gov |
| TwinSpires Terms | twinspires.com/terms |
| DraftKings Racing Terms | draftkings.com/horse-racing |

---

## 3. Platform Comparison

### 3.1 Feature Comparison Matrix

| Feature | TwinSpires | DraftKings |
|---------|------------|------------|
| **Odds Type** | Parimutuel | Fixed-odds |
| **Odds Timing** | Final at race start | Locked at bet time |
| **Win Pool Takeout** | ~17% | Built into odds |
| **Track Coverage** | All US tracks | Select US tracks |
| **Bet Minimum** | $2 | $1 |
| **Bet Maximum** | Pool-dependent | Varies by race |
| **Account Funding** | Bank, Card, PayPal | Bank, Card |
| **Withdrawal Speed** | 2-5 business days | 2-5 business days |
| **Mobile App** | Yes | Yes |
| **API Access** | Limited/None | Limited/None |
| **Live Streaming** | Yes | Limited |

### 3.2 Odds Format Comparison

**TwinSpires (Parimutuel):**
- Odds fluctuate until race starts
- Final odds determined by pool
- Takeout already reflected in odds
- Display: American format (5-1)

**DraftKings (Fixed-Odds):**
- Odds locked when bet placed
- Price set by bookmaker
- Margin built into odds
- Display: American format (+500)

### 3.3 EV Calculation Differences

**TwinSpires:**
```
EV = (p_model × decimal_odds) - 1
# Takeout already in odds, no adjustment needed
```

**DraftKings:**
```
EV = (p_model × decimal_odds) - 1
# Odds are fixed at bet time, actual odds obtained
```

---

## 4. TwinSpires Verification

### 4.1 Account Setup

- [ ] Create TwinSpires account
- [ ] Complete identity verification
- [ ] Add funding method
- [ ] Verify withdrawal process
- [ ] Test small deposit ($20)

### 4.2 Platform Testing

- [ ] Navigate to race selection
- [ ] View race card with entries
- [ ] Check odds display format
- [ ] Verify morning line availability
- [ ] Check will-pays display

### 4.3 Betting Workflow

- [ ] Place test win bet ($2)
- [ ] Record time from decision to confirmation
- [ ] Note any friction points
- [ ] Verify bet in bet history
- [ ] Document confirmation format

### 4.4 Data Availability

| Data Point | Available? | Format |
|------------|------------|--------|
| Morning line odds | ⬜ | |
| Current tote odds | ⬜ | |
| Odds refresh rate | ⬜ | |
| Pool totals | ⬜ | |
| Scratches | ⬜ | |
| Changes | ⬜ | |

### 4.5 Limitations Identified

| Limitation | Impact | Mitigation |
|------------|--------|------------|
| | | |

---

## 5. DraftKings Verification

### 5.1 Account Setup

- [ ] Create DraftKings account
- [ ] Verify horse racing access for KY
- [ ] Complete identity verification
- [ ] Add funding method
- [ ] Test small deposit ($20)

### 5.2 Platform Testing

- [ ] Navigate to horse racing section
- [ ] View available tracks
- [ ] Check odds display format
- [ ] Verify odds lock timing
- [ ] Check track coverage vs TwinSpires

### 5.3 Betting Workflow

- [ ] Place test win bet ($2)
- [ ] Record time from decision to confirmation
- [ ] Note any friction points
- [ ] Verify bet in bet history
- [ ] Document confirmation format

### 5.4 Data Availability

| Data Point | Available? | Format |
|------------|------------|--------|
| Fixed odds | ⬜ | |
| Odds movement history | ⬜ | |
| Track coverage | ⬜ | |
| Scratches | ⬜ | |
| Race cards | ⬜ | |

### 5.5 Limitations Identified

| Limitation | Impact | Mitigation |
|------------|--------|------------|
| | | |

---

## 6. Manual Workflow Testing

### 6.1 End-to-End Test Scenario

**Scenario:** System recommends $10 WIN bet on Horse #4 at 5-1 odds

**Steps:**
1. Dashboard shows recommendation (T-10 min)
2. Open betting platform
3. Navigate to correct race
4. Verify horse and odds
5. Enter bet amount
6. Confirm bet
7. Record execution details
8. Log in system

### 6.2 Timing Test

| Step | TwinSpires Time | DraftKings Time |
|------|-----------------|-----------------|
| Open app/site | | |
| Navigate to race | | |
| Enter bet | | |
| Confirm bet | | |
| **Total** | | |

**Target:** < 60 seconds from decision to confirmation

### 6.3 Error Scenarios

Test handling of:

- [ ] Scratch after recommendation
- [ ] Odds movement beyond threshold
- [ ] Platform downtime
- [ ] Insufficient funds
- [ ] Bet rejection

### 6.4 Workflow Documentation

Document step-by-step process for each platform:

**TwinSpires Workflow:**
1.
2.
3.

**DraftKings Workflow:**
1.
2.
3.

---

## 7. Decision Matrix

### 7.1 Scoring Criteria

Score each platform 1-5 on:

| Criterion | Weight | TwinSpires | DraftKings |
|-----------|--------|------------|------------|
| Legal availability | 20% | | |
| Track coverage | 20% | | |
| Odds favorability | 15% | | |
| Ease of use | 15% | | |
| Betting limits | 10% | | |
| Mobile experience | 10% | | |
| Withdrawal speed | 5% | | |
| Customer support | 5% | | |
| **Weighted Total** | 100% | | |

### 7.2 Pros and Cons Summary

**TwinSpires:**

Pros:
-

Cons:
-

**DraftKings:**

Pros:
-

Cons:
-

---

## 8. Final Recommendation

### 8.1 Primary Platform Selection

**Recommended Platform:** _______________

**Rationale:**
-
-
-

### 8.2 Secondary Platform

**Backup Platform:** _______________

**Use Cases:**
- When primary unavailable
- For track coverage gaps
- For odds comparison

### 8.3 Implementation Notes

| Item | Detail |
|------|--------|
| Primary platform | |
| Account funded | |
| Workflow tested | |
| Ready for paper trading | |
| Ready for live betting | |

---

## Appendix A: Verification Checklist Summary

### Pre-Live Checklist

- [ ] Legal verification complete
- [ ] Primary platform account active
- [ ] Funding method verified
- [ ] Test bets placed successfully
- [ ] Workflow documented
- [ ] Timing meets requirements (<60 sec)
- [ ] Error handling tested
- [ ] Backup platform ready

### Sign-Off

| Role | Name | Date | Signature |
|------|------|------|-----------|
| User | | | |
| Verified By | | | |

---

## Appendix B: Contact Information

| Platform | Support | Hours |
|----------|---------|-------|
| TwinSpires | 1-877-SPIRES | 24/7 |
| DraftKings | In-app chat | 24/7 |
| Kentucky HRC | 859-246-2040 | Business hours |

---

*Document maintained by: Operations Team*
*Review: Before go-live and annually*
