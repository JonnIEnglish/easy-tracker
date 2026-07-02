from scripts.utils import reconcile_zac_scale


def test_reconcile_zac_scale_leaves_consistent_values_untouched() -> None:
    assert reconcile_zac_scale(11851.30, 11500.0) == 11851.30


def test_reconcile_zac_scale_corrects_rand_reported_as_cents() -> None:
    # Value looks 100x too small versus its reference (rand mistaken for cents).
    assert reconcile_zac_scale(118.51, 11500.0) == 11851.0


def test_reconcile_zac_scale_corrects_cents_reported_as_rand() -> None:
    # Value looks 100x too large versus its reference (cents mistaken for rand).
    assert reconcile_zac_scale(1185130.0, 11500.0) == 11851.30


def test_reconcile_zac_scale_ignores_missing_or_non_positive_reference() -> None:
    assert reconcile_zac_scale(118.51, None) == 118.51
    assert reconcile_zac_scale(118.51, 0.0) == 118.51
    assert reconcile_zac_scale(0.0, 11500.0) == 0.0
