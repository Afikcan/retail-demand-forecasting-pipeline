import pandas as pd
import pytest

from conftest import load_module

evaluate = load_module("model/06_evaluate.py", "evaluate_module")


def test_wmape_perfect_forecast_is_zero():
    pdf = pd.DataFrame({"actual": [10, 20, 30], "forecast": [10, 20, 30]})
    assert evaluate.wmape(pdf, "forecast") == 0


def test_wmape_known_value():
    pdf = pd.DataFrame({"actual": [100, 200], "forecast": [90, 180]})
    # |100-90| + |200-180| = 30; sum(actual) = 300 -> 30/300 = 0.1
    assert evaluate.wmape(pdf, "forecast") == pytest.approx(0.1)


def test_wmape_penalizes_larger_errors_more():
    pdf_small_error = pd.DataFrame({"actual": [100], "forecast": [95]})
    pdf_large_error = pd.DataFrame({"actual": [100], "forecast": [50]})
    assert evaluate.wmape(pdf_small_error, "forecast") < evaluate.wmape(
        pdf_large_error, "forecast"
    )
