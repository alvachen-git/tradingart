import unittest
from unittest.mock import patch

import pandas as pd

import data_engine


def _rows_from_changes(prefix: str, start_date: str, end_date: str, changes):
    rows = []
    for idx, change in enumerate(changes, start=1):
        code = f"{prefix}260{idx}"
        rows.append({"ts_code": code, "trade_date": start_date, "iv": 20.0})
        rows.append({"ts_code": code, "trade_date": end_date, "iv": 20.0 + float(change)})
    return rows


def _table_data_rows(output: str):
    return [
        line
        for line in str(output).splitlines()
        if line.startswith("| ") and not line.startswith("| 排名") and not line.startswith("|---")
    ]


def _fake_read_sql_factory(commodity_rows=None, etf_rows=None):
    commodity_rows = list(commodity_rows or [])
    etf_rows = list(etf_rows or [])

    def _date_frame(rows):
        dates = sorted({str(row["trade_date"]).replace("-", "")[:8] for row in rows}, reverse=True)
        return pd.DataFrame({"trade_date": dates})

    def _fake_read_sql(sql, engine, params=None):
        sql_text = str(sql)
        params = params or {}
        if "FROM commodity_iv_history" in sql_text:
            if "SELECT DISTINCT" in sql_text:
                return _date_frame(commodity_rows)
            return pd.DataFrame(commodity_rows)
        if "FROM etf_iv_history" in sql_text:
            if "SELECT DISTINCT" in sql_text:
                return _date_frame(etf_rows)
            return pd.DataFrame(etf_rows)
        raise AssertionError(f"Unexpected SQL: {sql_text}; params={params}")

    return _fake_read_sql


class IVChangeScannerTest(unittest.TestCase):
    def _invoke(self, **kwargs):
        return data_engine.scan_iv_change_ranking.invoke(kwargs)

    def test_default_returns_top_five_and_excludes_summary_rows(self):
        rows = _rows_from_changes("SN", "20260611", "20260612", [9, 8, 7, 6, 5, 4])
        rows.extend(
            [
                {"ts_code": "SN", "trade_date": "20260611", "iv": 10.0},
                {"ts_code": "SN", "trade_date": "20260612", "iv": 99.0},
            ]
        )

        with patch.object(data_engine, "engine", object()), patch.object(
            data_engine.pd, "read_sql", side_effect=_fake_read_sql_factory(commodity_rows=rows)
        ):
            output = self._invoke(start_date="20260611", end_date="20260612", asset_scope="commodity", symbol="SN")

        data_rows = _table_data_rows(output)
        self.assertEqual(len(data_rows), 5)
        self.assertIn("SN2601", output)
        self.assertIn("SN2605", output)
        self.assertNotIn("SN2606", output)
        self.assertNotIn("| SN |", output)

    def test_increase_direction_sorts_by_point_change_desc(self):
        rows = _rows_from_changes("SN", "20260611", "20260612", [1, 5, 3])
        with patch.object(data_engine, "engine", object()), patch.object(
            data_engine.pd, "read_sql", side_effect=_fake_read_sql_factory(commodity_rows=rows)
        ):
            output = self._invoke(
                start_date="20260611",
                end_date="20260612",
                asset_scope="commodity",
                symbol="SN",
                direction="increase",
            )

        self.assertLess(output.index("SN2602"), output.index("SN2603"))
        self.assertLess(output.index("SN2603"), output.index("SN2601"))

    def test_decrease_direction_sorts_by_point_change_asc(self):
        rows = _rows_from_changes("SN", "20260611", "20260612", [-1, -5, -3])
        with patch.object(data_engine, "engine", object()), patch.object(
            data_engine.pd, "read_sql", side_effect=_fake_read_sql_factory(commodity_rows=rows)
        ):
            output = self._invoke(
                start_date="20260611",
                end_date="20260612",
                asset_scope="commodity",
                symbol="SN",
                direction="decrease",
            )

        self.assertLess(output.index("SN2602"), output.index("SN2603"))
        self.assertLess(output.index("SN2603"), output.index("SN2601"))

    def test_limit_is_clamped_to_ten(self):
        rows = _rows_from_changes("SN", "20260611", "20260612", list(range(20, 8, -1)))
        with patch.object(data_engine, "engine", object()), patch.object(
            data_engine.pd, "read_sql", side_effect=_fake_read_sql_factory(commodity_rows=rows)
        ):
            output = self._invoke(
                start_date="20260611",
                end_date="20260612",
                asset_scope="commodity",
                symbol="SN",
                limit=50,
            )

        self.assertEqual(len(_table_data_rows(output)), 10)

    def test_chinese_symbol_filters_to_matching_commodity(self):
        rows = _rows_from_changes("SN", "20260611", "20260612", [3, 2])
        rows.extend(_rows_from_changes("CU", "20260611", "20260612", [10]))
        with patch.object(data_engine, "engine", object()), patch.object(
            data_engine.pd, "read_sql", side_effect=_fake_read_sql_factory(commodity_rows=rows)
        ):
            output = self._invoke(start_date="20260611", end_date="20260612", asset_scope="commodity", symbol="沪锡")

        self.assertIn("SN2601", output)
        self.assertNotIn("CU2601", output)

    def test_etf_source_participates_in_scan(self):
        etf_rows = [
            {"etf_code": "510300.SH", "trade_date": "20260611", "iv": 18.0},
            {"etf_code": "510300.SH", "trade_date": "20260612", "iv": 20.0},
            {"etf_code": "159915.SZ", "trade_date": "20260611", "iv": 22.0},
            {"etf_code": "159915.SZ", "trade_date": "20260612", "iv": 23.0},
        ]
        with patch.object(data_engine, "engine", object()), patch.object(
            data_engine.pd, "read_sql", side_effect=_fake_read_sql_factory(etf_rows=etf_rows)
        ):
            output = self._invoke(start_date="20260611", end_date="20260612", asset_scope="etf")

        self.assertIn("ETF", output)
        self.assertIn("510300.SH", output)
        self.assertIn("159915.SZ", output)

    def test_missing_comparable_dates_returns_clear_message(self):
        rows = [{"ts_code": "SN2601", "trade_date": "20260612", "iv": 20.0}]
        with patch.object(data_engine, "engine", object()), patch.object(
            data_engine.pd, "read_sql", side_effect=_fake_read_sql_factory(commodity_rows=rows)
        ):
            output = self._invoke(start_date="20260611", end_date="20260612", asset_scope="commodity", symbol="SN")

        self.assertIn("未找到可比 IV 数据", output)
        self.assertIn("最近可用日期", output)


if __name__ == "__main__":
    unittest.main()
