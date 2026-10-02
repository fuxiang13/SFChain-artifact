#!/usr/bin/env python3
"""RQ3 analysis: liveness under injected faults (delay / disconnection / refusal)"""
import sys, os, re, statistics, calendar
from pathlib import Path
from datetime import datetime, timedelta
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

BASE = str(Path(__file__).resolve().parents[3])
LOGS = os.path.join(BASE, "logs")
SEAL = re.compile(r"(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\.\d+).*block created successfully, txType=(\w+), height=(\d+)")
CONS = re.compile(r"(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\.\d+).*consensus completed, height=(\d+), chain=(\w+)")
STEP0 = re.compile(r"(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\.\d+).*block generation - step 0\] starting to generate (\w+) chain block")
RESUME = re.compile(r"(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\.\d+).*RESUME releasing (\d+) queued")

def to_ms(dt): return calendar.timegm(dt.timetuple()) * 1000 + dt.microsecond // 1000

def parse(tag):
    f = os.path.join(LOGS, f"management_{tag}.log")
    if not os.path.exists(f): return None
    seals, cons, pend0, starts = {}, {}, {}, {}
    for line in open(f, encoding="utf-8", errors="replace"):
        m = STEP0.match(line)
        if m:
            ch = m.group(2)
            if ch not in pend0: pend0[ch] = datetime.strptime(m.group(1), "%Y/%m/%d %H:%M:%S.%f")
            continue
        m = SEAL.match(line)
        if m:
            ch, h = m.group(2), int(m.group(3))
            seals.setdefault((ch, h), []).append(datetime.strptime(m.group(1), "%Y/%m/%d %H:%M:%S.%f"))
            starts.setdefault((ch, h), pend0.pop(ch))
            continue
        m = CONS.match(line)
        if m:
            cons.setdefault((m.group(3), int(m.group(2))), []).append(datetime.strptime(m.group(1), "%Y/%m/%d %H:%M:%S.%f"))
    off = timedelta(hours=8)
    lats = []
    last_cons_ms = 0
    for k, sd in seals.items():
        cd = cons.get(k)
        if not cd: continue
        cm = to_ms(cd[0] - off)
        if k in starts:
            sm = to_ms(starts[k] - off)
        else:
            sm = to_ms(sd[0] - off)
        lats.append(cm - sm)
        if cm > last_cons_ms: last_cons_ms = cm
    return {"lats": lats, "seals": len(seals), "cons": len(cons), "last_cons_ms": last_cons_ms,
            "first_seal_ms": min((to_ms(v[0]-off) for v in seals.values()), default=0)}

def resume_info(tag):
    """Read the dated resume event from this round's proxy log."""
    f = os.path.join(LOGS, f"proxy9091_{tag}.log")
    if not os.path.exists(f): return None, None
    resumed_at, queued = None, None
    rx = re.compile(r"(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\.\d+).*RESUME releasing (\d+) queued")
    for line in open(f, encoding="utf-8", errors="replace"):
        m = rx.search(line)
        if m:
            dt = datetime.strptime(m.group(1), "%Y/%m/%d %H:%M:%S.%f")
            resumed_at = to_ms(dt - timedelta(hours=8))
            queued = int(m.group(2))
    return resumed_at, queued


SC = ["base", "d100", "d500", "d1000", "dual500", "bs100", "bs400", "bs500", "disc", "refuse"]
print(f"{'scenario':>8} | {'rounds':>6} | {'blocks':>7} | {'p50':>6} {'avg':>6} {'max':>6} | note")
print("-" * 78)
results = {}
for sc in SC:
    per = []
    for r in (1, 2, 3):
        d = parse(f"rq3_{sc}_r{r}")
        if d: per.append((r, d))
    if not per: continue
    all_lats = [x for _, d in per for x in d["lats"]]
    seals = sum(d["seals"] for _, d in per); cons = sum(d["cons"] for _, d in per)
    note = ""
    if sc == "refuse":
        note = f"seals={seals} cons={cons} (refusal blocks finalization)"
        print(f"{sc:>8} | {len(per):>6} | {cons//len(per):>7} | {'-':>6} {'-':>6} {'-':>6} | {note}")
        results[sc] = {"seals": seals//len(per), "cons": cons//len(per)}
        continue
    if sc == "disc":
        drains, qd = [], []
        for r, d in per:
            ra, q = resume_info(f"rq3_{sc}_r{r}")
            if ra and d["last_cons_ms"] > ra:
                drains.append((d["last_cons_ms"] - ra) / 1000); qd.append(q)
        note = f"queued_at_resume={qd}, drain_s={['%.1f'%x for x in drains]}"
        print(f"{sc:>8} | {len(per):>6} | {cons//len(per):>7} | {'-':>6} {'-':>6} {'-':>6} | {note}")
        results[sc] = {"drains": drains, "queued": qd, "blocks": cons//len(per)}
        continue
    round_lats = [sorted(d["lats"]) for _, d in per]
    if len(per) != 3 or any(not x for x in round_lats):
        raise SystemExit(f"Incomplete three-round scenario: {sc}")
    p50 = statistics.median(x[len(x)//2] for x in round_lats)
    avg = statistics.median(statistics.fmean(x) for x in round_lats)
    mx = max(all_lats)
    print(f"{sc:>8} | {len(per):>6} | {cons//len(per):>7} | {p50:>6.0f} {avg:>6.0f} {mx:>6.0f} |")
    results[sc] = {"p50": p50, "avg": avg, "max": mx, "blocks": cons//len(per)}
print()
if "base" in results and "d500" in results:
    print(f"delay pass-through (d500 - base): p50 +{results['d500']['p50']-results['base']['p50']:.0f}ms (injected 500ms)")
