from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from pandas.errors import EmptyDataError

SITE = Path("site")


def load_csv(path: str) -> pd.DataFrame:
    p = Path(path)
    if not p.exists() or p.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(p)
    except EmptyDataError:
        return pd.DataFrame()


def metric(metrics: dict, key: str, pct: bool = False) -> str:
    v = metrics.get(key)
    if v is None or v == "":
        return "n/a"
    if isinstance(v, (int, float)):
        return f"{v:.2f}%" if pct else f"{v:.2f}" if not float(v).is_integer() else str(int(v))
    return str(v)


def rows_html(df: pd.DataFrame, cols: list[str], pct_cols: set[str] | None = None, limit: int | None = None) -> str:
    if df.empty:
        return f"<tr><td colspan='{len(cols)}'>No data</td></tr>"
    pct_cols = pct_cols or set()
    work = df.head(limit) if limit else df
    lines = []
    for _, r in work.iterrows():
        cells = []
        for c in cols:
            v = r.get(c, "")
            if c in pct_cols and pd.notna(v):
                cells.append(f"<td>{float(v):.2f}%</td>")
            elif isinstance(v, float):
                cells.append(f"<td>{v:.4f}</td>")
            else:
                cells.append(f"<td>{v}</td>")
        lines.append("<tr>" + "".join(cells) + "</tr>")
    return "\n".join(lines)


def main() -> None:
    SITE.mkdir(parents=True, exist_ok=True)

    metrics = {}
    mpath = Path("site/metrics.json")
    if mpath.exists():
        metrics = json.loads(mpath.read_text(encoding="utf-8"))

    holdings = load_csv("data/holdings_history.csv")
    if holdings.empty:
        raise FileNotFoundError("data/holdings_history.csv is missing or empty")

    theme_map = load_csv("config/theme_map.csv")
    ticker_map = load_csv("config/ticker_map.csv")
    latest_date = holdings["snapshot_date"].max()
    latest = holdings[holdings["snapshot_date"] == latest_date].copy()
    latest = latest.merge(ticker_map[["instrument", "ticker"]], on="instrument", how="left") if not ticker_map.empty else latest
    latest = latest.merge(theme_map, on="instrument", how="left") if not theme_map.empty else latest
    latest["theme"] = latest.get("theme", pd.Series(index=latest.index)).fillna("Unmapped")
    latest["subtheme"] = latest.get("subtheme", pd.Series(index=latest.index)).fillna("Unmapped")
    latest = latest.sort_values("weight", ascending=False)

    changes = load_csv("data/analytics/monthly_changes.csv")
    active = load_csv("data/analytics/active_trade_estimates.csv")
    contrib = load_csv("data/analytics/contribution_history.csv")
    score = load_csv("data/analytics/decision_scorecard.csv")
    turn = load_csv("data/analytics/turnover_history.csv")
    est_ret = load_csv("data/analytics/estimated_portfolio_returns.csv")

    warnings = []
    dqj = Path("site/data_quality.json")
    if dqj.exists():
        warnings = json.loads(dqj.read_text(encoding="utf-8")).get("warnings", [])

    latest_change = pd.DataFrame()
    if not changes.empty:
        cdate = changes["snapshot_date"].max()
        latest_change = changes[changes["snapshot_date"] == cdate].sort_values("weight_change", ascending=False)

    latest_active = pd.DataFrame()
    if not active.empty:
        adate = active["snapshot_date"].max()
        latest_active = active[active["snapshot_date"] == adate].copy()
        latest_active = latest_active.sort_values("active_weight_change", ascending=False)

    latest_contrib = pd.DataFrame()
    if not contrib.empty:
        end_dt = contrib["current_snapshot_date"].max()
        latest_contrib = contrib[contrib["current_snapshot_date"] == end_dt].copy()

    if "estimated_contribution" in latest_contrib.columns:
        tops = latest_contrib.sort_values("estimated_contribution", ascending=False).head(8)
        dets = latest_contrib.sort_values("estimated_contribution", ascending=True).head(8)
    else:
        tops = pd.DataFrame(columns=["instrument", "estimated_contribution"])
        dets = pd.DataFrame(columns=["instrument", "estimated_contribution"])

    summary_turnover = "n/a"
    if not turn.empty:
        summary_turnover = f"{float(turn.iloc[-1]['turnover_estimate']):.2f}"

    est_perf = "n/a"
    if not est_ret.empty and pd.notna(est_ret.iloc[-1].get("estimated_portfolio_return")):
        est_perf = f"{float(est_ret.iloc[-1]['estimated_portfolio_return']) * 100:.2f}%"

    warn_html = "".join(f"<li>{w}</li>" for w in warnings) if warnings else "<li>No warnings.</li>"

    html = f"""<!doctype html>
<html>
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width,initial-scale=1\" />
  <title>EASYGE Active Tracker</title>
  <style>
    :root {{ --bg:#f2f5f8; --card:#fff; --ink:#1a2433; --muted:#5b6b80; --accent:#005f73; --line:#d5dee8; --good:#117a65; --bad:#b23a48; }}
    * {{ box-sizing:border-box; }}
    body {{ margin:0; font-family: "Avenir Next", "Segoe UI", sans-serif; background:linear-gradient(180deg,#eaf2f8 0,#f6f8fb 45%,#eef3f9 100%); color:var(--ink); }}
    .wrap {{ max-width:1200px; margin:0 auto; padding:24px; }}
    h1,h2 {{ margin:0 0 10px; }}
    p.sub {{ margin:0 0 20px; color:var(--muted); }}
    .cards {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(160px,1fr)); gap:12px; margin-bottom:18px; }}
    .card {{ background:var(--card); border:1px solid var(--line); border-radius:12px; padding:12px; box-shadow:0 2px 10px rgba(0,35,65,.05); }}
    .label {{ color:var(--muted); font-size:12px; text-transform:uppercase; letter-spacing:.5px; }}
    .val {{ font-size:24px; font-weight:700; margin-top:4px; }}
    .grid {{ display:grid; grid-template-columns:2fr 1fr; gap:14px; }}
    .panel {{ background:var(--card); border:1px solid var(--line); border-radius:12px; padding:14px; margin-bottom:14px; }}
    table {{ width:100%; border-collapse:collapse; font-size:13px; }}
    th,td {{ padding:8px; border-bottom:1px solid var(--line); text-align:left; vertical-align:top; }}
    th {{ font-size:12px; color:var(--muted); text-transform:uppercase; letter-spacing:.5px; }}
    .search {{ width:100%; padding:10px; border:1px solid var(--line); border-radius:8px; margin-bottom:10px; }}
    .twocol {{ display:grid; grid-template-columns:1fr 1fr; gap:10px; }}
    .good {{ color:var(--good); }}
    .bad {{ color:var(--bad); }}
    img {{ max-width:100%; border-radius:10px; border:1px solid var(--line); margin:8px 0; }}
    @media (max-width: 980px) {{ .grid {{ grid-template-columns:1fr; }} .twocol {{ grid-template-columns:1fr; }} }}
  </style>
</head>
<body>
  <div class=\"wrap\">
    <h1>EASYGE Active Holdings Tracker</h1>
    <p class=\"sub\">Latest snapshot: {metrics.get('latest_snapshot_date', latest_date)}</p>

    <div class=\"cards\">
      <div class=\"card\"><div class=\"label\">Holdings</div><div class=\"val\">{metric(metrics,'num_holdings')}</div></div>
      <div class=\"card\"><div class=\"label\">Cash</div><div class=\"val\">{metric(metrics,'cash_weight',True)}</div></div>
      <div class=\"card\"><div class=\"label\">Top 1</div><div class=\"val\">{metric(metrics,'top_1_weight',True)}</div></div>
      <div class=\"card\"><div class=\"label\">Top 5</div><div class=\"val\">{metric(metrics,'top_5_weight',True)}</div></div>
      <div class=\"card\"><div class=\"label\">Top 10</div><div class=\"val\">{metric(metrics,'top_10_weight',True)}</div></div>
      <div class=\"card\"><div class=\"label\">Turnover</div><div class=\"val\">{summary_turnover}</div></div>
      <div class=\"card\"><div class=\"label\">Estimated Interval Return</div><div class=\"val\">{est_perf}</div></div>
      <div class=\"card\"><div class=\"label\">Largest Holding</div><div class=\"val\" style=\"font-size:16px;\">{metrics.get('largest_holding','n/a')}</div></div>
    </div>

    <div class=\"grid\">
      <div>
        <div class=\"panel\">
          <h2>Latest Holdings</h2>
          <input id=\"holdSearch\" class=\"search\" placeholder=\"Search holdings table\" />
          <table id=\"holdingsTable\">
            <thead><tr><th>Instrument</th><th>Ticker</th><th>Currency</th><th>Weight</th><th>Theme</th><th>Subtheme</th></tr></thead>
            <tbody>{rows_html(latest, ['instrument','ticker','currency','weight','theme','subtheme'], {'weight'})}</tbody>
          </table>
        </div>

        <div class=\"panel\">
          <h2>Monthly Changes</h2>
          <table>
            <thead><tr><th>Instrument</th><th>Prev</th><th>Current</th><th>Change</th><th>Type</th></tr></thead>
            <tbody>{rows_html(latest_change, ['instrument','previous_weight','current_weight','weight_change','change_type'], {'previous_weight','current_weight'})}</tbody>
          </table>
        </div>

        <div class=\"panel\">
          <h2>Active Trade Estimates</h2>
          <table>
            <thead><tr><th>Instrument</th><th>Raw change (pp)</th><th>Expected drift</th><th>Active change (pp)</th><th>Signal</th></tr></thead>
            <tbody>{rows_html(latest_active, ['instrument','raw_weight_change','expected_weight_after_price_move','active_weight_change','trade_signal'], limit=30)}</tbody>
          </table>
        </div>
      </div>

      <div>
        <div class=\"panel\">
          <h2>Performance Attribution</h2>
          <div class=\"twocol\">
            <div>
              <h3>Top Contributors</h3>
              <table><thead><tr><th>Name</th><th>Contribution</th></tr></thead><tbody>{rows_html(tops, ['instrument','estimated_contribution'])}</tbody></table>
            </div>
            <div>
              <h3>Top Detractors</h3>
              <table><thead><tr><th>Name</th><th>Contribution</th></tr></thead><tbody>{rows_html(dets, ['instrument','estimated_contribution'])}</tbody></table>
            </div>
          </div>
        </div>

        <div class=\"panel\">
          <h2>Decision Scorecard</h2>
          <table>
            <thead><tr><th>Decision</th><th>Count</th><th>Avg 1m</th><th>Hit 1m</th><th>Avg 3m</th><th>Hit 3m</th></tr></thead>
            <tbody>{rows_html(score, ['decision_type','count','average_forward_1m_return','hit_rate_1m','average_forward_3m_return','hit_rate_3m'])}</tbody>
          </table>
        </div>

        <div class=\"panel\">
          <h2>Data Quality Warnings</h2>
          <ul>{warn_html}</ul>
        </div>

        <div class=\"panel\">
          <h2>Charts</h2>
          <img src=\"charts/latest_top20.png\" alt=\"Top 20\" />
          <img src=\"charts/concentration.png\" alt=\"Concentration\" />
          <img src=\"charts/theme_exposure.png\" alt=\"Theme exposure\" />
        </div>
      </div>
    </div>
  </div>

  <script>
    const input = document.getElementById('holdSearch');
    const table = document.getElementById('holdingsTable');
    input.addEventListener('input', () => {{
      const q = input.value.toLowerCase();
      for (const row of table.tBodies[0].rows) {{
        row.style.display = row.innerText.toLowerCase().includes(q) ? '' : 'none';
      }}
    }});
  </script>
</body>
</html>
"""

    (SITE / "index.html").write_text(html, encoding="utf-8")
    latest.to_csv(SITE / "holdings_history.csv", index=False)
    print("Dashboard built.")


if __name__ == "__main__":
    main()
