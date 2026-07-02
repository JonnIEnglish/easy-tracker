import pandas as pd

from scripts.fetch_nav import last_known_nav_zac, parse_nav_observation


def test_parse_nav_observation_from_representative_snippet() -> None:
    html = """
    <section class="instrument-details">
      <div class="metric">
        <span class="label">Price / NAV (ZAC)</span>
        <strong>1,245.67</strong>
      </div>
      <div class="meta">As at: 08 May 2026</div>
    </section>
    """
    observation = parse_nav_observation(
        html=html,
        fund_code="EASYGE",
        source_url="https://etfs.easyequities.co.za/easyetf-instrument-page/easyge",
        captured_at_utc="2026-05-09T10:00:00Z",
    )
    assert observation is not None
    assert observation["fund_code"] == "EASYGE"
    assert observation["nav_zac"] == 1245.67
    assert observation["nav_date"] == "2026-05-08"


def test_parse_nav_observation_normalizes_zar_label_to_zac() -> None:
    html = """
    <section class="instrument-details">
      <div class="metric">
        <span class="label">Price / NAV (ZAR)</span>
        <strong>12.4567</strong>
      </div>
      <div class="meta">As at: 08 May 2026</div>
    </section>
    """
    observation = parse_nav_observation(
        html=html,
        fund_code="EASYGE",
        source_url="https://etfs.easyequities.co.za/easyetf-instrument-page/easyge",
        captured_at_utc="2026-05-09T10:00:00Z",
    )
    assert observation is not None
    assert observation["nav_zac"] == 1245.67


def test_last_known_nav_zac_picks_latest_capture() -> None:
    history = pd.DataFrame(
        [
            {"fund_code": "EASYGE", "nav_zac": 1000.0, "captured_at_utc": "2026-05-08T10:00:00Z"},
            {"fund_code": "EASYGE", "nav_zac": 1010.0, "captured_at_utc": "2026-05-09T10:00:00Z"},
            {"fund_code": "EASYAI", "nav_zac": 5000.0, "captured_at_utc": "2026-05-09T10:00:00Z"},
        ]
    )
    assert last_known_nav_zac(history, "EASYGE") == 1010.0
    assert last_known_nav_zac(history, "EASYBF") is None
