# Polymarket Mean Reversion Bot
# Polymarket Mean Reversion v1

This repository contains:

- Smartflow ingestion pipeline (trades, markets, flow snapshots, wallet labels)
- Mean Reversion v1 trading bot (executor, dashboard, analytics views)

The goal is to keep this repo clean and minimal - only the files needed to run
the pipeline + MR v1 strategy.

---

## 📦 Installation (local)

```bash
cd polymarket-mean-reversion
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt