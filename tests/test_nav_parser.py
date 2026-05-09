from scripts.fetch_nav import parse_nav_observation


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
