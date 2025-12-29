#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
from datetime import datetime, timezone
import sys

ROOT = Path("/root/polymarket-mean-reversion")
FILE = ROOT / "mr" / "mean_reversion_executor.py"

OLD = """            # 3) Normal behavior: open mr_positions row
            cur.execute(\"\"\"
                INSERT INTO mr_positions (strategy, market_id, outcome, side, entry_price, entry_ts, size, avg_price_18h, dislocation)
                VALUES (%s,%s,%s,'long',%s,%s,%s,%s,%s)
            \"\"\", (STRATEGY, mid, outcome, entry_px, now_ts, size, avg, dislo))
"""

NEW = """            # 3) Normal behavior: open mr_positions row
            # IMPORTANT:
            # In live mode we rely on the intent -> order -> fills pipeline.
            # Creating mr_positions here causes ghost positions (DB shows open, but no Polymarket order).
            ALLOW_DIRECT = (os.getenv("MR_ALLOW_DIRECT_POSITIONS", "0") or "").strip().lower() in (
                "1", "true", "yes", "y", "on"
            )

            if MR_INTENTS_ENABLE and not ALLOW_DIRECT:
                counters["intent_only_skip_pos"] += 1
                continue

            cur.execute(\"\"\"
                INSERT INTO mr_positions (strategy, market_id, outcome, side, entry_price, entry_ts, size, avg_price_18h, dislocation)
                VALUES (%s,%s,%s,'long',%s,%s,%s,%s,%s)
            \"\"\", (STRATEGY, mid, outcome, entry_px, now_ts, size, avg, dislo))
"""

def main() -> int:
    if not FILE.exists():
        print(f"ERROR: file not found: {FILE}", file=sys.stderr)
        return 2

    txt = FILE.read_text(encoding="utf-8")

    if NEW in txt:
        print("Already patched (NEW block found). No changes.")
        return 0

    if OLD not in txt:
        print("ERROR: did not find the expected OLD block. Refusing to patch.", file=sys.stderr)
        return 3

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    bak = FILE.with_suffix(FILE.suffix + f".bak.{ts}")
    bak.write_text(txt, encoding="utf-8")
    print(f"Backup written: {bak}")

    patched = txt.replace(OLD, NEW, 1)
    FILE.write_text(patched, encoding="utf-8")
    print("Patched successfully.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
