import base64
import io
import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch
import pandas as pd

_IMPORT_ERROR = None
try:
    from starlette.requests import Request
    from starlette.testclient import TestClient
    import mobile_api
except Exception as exc:  # pragma: no cover
    Request = None
    TestClient = None
    mobile_api = None
    _IMPORT_ERROR = exc


def _request_with_headers(headers: dict[str, str] | None = None) -> Request:
    raw_headers = []
    for key, value in (headers or {}).items():
        raw_headers.append((key.lower().encode("latin-1"), str(value).encode("latin-1")))
    return Request({"type": "http", "headers": raw_headers})


def _wav_bytes(sample_rate: int = 16000, channels: int = 1, bits_per_sample: int = 16, data_bytes: bytes = b"\0" * 3200) -> bytes:
    byte_rate = sample_rate * channels * bits_per_sample // 8
    block_align = channels * bits_per_sample // 8
    return (
        b"RIFF"
        + (36 + len(data_bytes)).to_bytes(4, "little")
        + b"WAVEfmt "
        + (16).to_bytes(4, "little")
        + (1).to_bytes(2, "little")
        + channels.to_bytes(2, "little")
        + sample_rate.to_bytes(4, "little")
        + byte_rate.to_bytes(4, "little")
        + block_align.to_bytes(2, "little")
        + bits_per_sample.to_bytes(2, "little")
        + b"data"
        + len(data_bytes).to_bytes(4, "little")
        + data_bytes
    )


@unittest.skipIf(mobile_api is None, f"mobile_api import failed: {_IMPORT_ERROR}")
class TestMobileApiDevice(unittest.TestCase):
    def test_device_ping_echoes_headers_and_user(self):
        req = _request_with_headers(
            {
                "X-Device-Id": "stackchan-01",
                "X-Device-Model": "StackChan",
                "X-Device-Version": "v1",
            }
        )

        out = mobile_api.device_ping(request=req, username="tester")

        self.assertTrue(out["ok"])
        self.assertEqual(out["user_id"], "tester")
        self.assertEqual(out["device_id"], "stackchan-01")
        self.assertEqual(out["device_headers_echo"]["device_model"], "StackChan")

    def test_device_config_defaults_to_manual_without_auto_poll(self):
        req = _request_with_headers({"X-Device-Id": "stackchan-01"})
        with patch.object(mobile_api, "_DEVICE_AUTO_POLL_ENABLED_DEFAULT", False), patch.object(
            mobile_api, "_DEVICE_VOICE_ENABLED_DEFAULT", True
        ):
            out = mobile_api.device_config(request=req, username="tester")

        self.assertEqual(out["briefing_mode"], "manual")
        self.assertFalse(out["auto_poll_enabled"])
        self.assertIsNone(out["auto_poll_seconds"])
        self.assertTrue(out["voice_enabled"])
        self.assertEqual(out["voice_mode"], "tap_to_wake")
        self.assertEqual(out["record_max_seconds"], 8)
        self.assertEqual(out["audio_format"], "wav_pcm_16k_mono")
        self.assertGreaterEqual(out["voice_task_max_wait_seconds"], 300)
        self.assertTrue(out["voice_latency_observation_enabled"])
        self.assertEqual(out["voice_realtime_endpoint"], "/api/device/voice/realtime")

    def test_device_voice_realtime_accepts_chunked_pcm_and_returns_events(self):
        pcm_frame = b"\1\0" * 1600

        with patch.dict(os.environ, {"DEVICE_REALTIME_ASR_ENABLED": "0"}), patch.object(
            mobile_api, "_resolve_websocket_user", return_value="tester"
        ), patch.object(
            mobile_api, "_device_transcribe_wav", return_value="你好"
        ), patch.object(
            mobile_api, "_device_synthesize_speech_wav", return_value=_wav_bytes(data_bytes=b"\0" * 16000)
        ):
            with TestClient(mobile_api.app) as client:
                with client.websocket_connect(
                    "/api/device/voice/realtime",
                    headers={"X-Device-Id": "stackchan-voice", "X-Device-Model": "StackChan"},
                ) as ws:
                    hello = ws.receive_json()
                    self.assertEqual(hello["type"], "hello")
                    self.assertTrue(hello["capabilities"]["binary_audio_frames"])
                    self.assertFalse(hello["capabilities"]["tts_audio_delta"])

                    ws.send_json({"type": "start", "conversation_id": "rt-1"})
                    self.assertEqual(ws.receive_json(), {"type": "status", "state": "listening"})

                    ws.send_json(
                        {
                            "type": "audio",
                            "seq": 1,
                            "pcm_b64": base64.b64encode(pcm_frame).decode("ascii"),
                        }
                    )
                    ack = ws.receive_json()
                    self.assertEqual(ack["type"], "audio_ack")
                    self.assertEqual(ack["seq"], 1)
                    self.assertEqual(ack["bytes"], len(pcm_frame))

                    ws.send_json({"type": "stop"})
                    self.assertEqual(ws.receive_json()["state"], "transcribing")
                    self.assertEqual(ws.receive_json()["state"], "thinking")
                    result = ws.receive_json()
                    self.assertEqual(result["type"], "result")
                    self.assertEqual(result["route_type"], "instant_reply")
                    self.assertEqual(result["transcript"], "你好")
                    final_text = ws.receive_json()
                    self.assertEqual(final_text, {"type": "final_transcript", "text": "你好"})
                    answer_delta = ws.receive_json()
                    self.assertEqual(answer_delta["type"], "answer_delta")
                    self.assertTrue(answer_delta["is_final"])
                    self.assertIn("你好", answer_delta["text"])
                    self.assertEqual(ws.receive_json()["state"], "speaking")
                    audio = ws.receive_json()
                    self.assertEqual(audio["type"], "audio_url")
                    done = ws.receive_json()
                    self.assertEqual(done["type"], "done")
                    self.assertEqual(done["conversation_id"], "rt-1")
                    self.assertEqual(done["followup_window_seconds"], 8)
                    followup = ws.receive_json()
                    self.assertEqual(followup["type"], "status")
                    self.assertEqual(followup["state"], "followup_listening")
                    self.assertEqual(followup["timeout_seconds"], 8)

    def test_device_voice_realtime_accepts_barge_in_and_resets_to_listening(self):
        with patch.dict(os.environ, {"DEVICE_REALTIME_ASR_ENABLED": "0"}), patch.object(
            mobile_api, "_resolve_websocket_user", return_value="tester"
        ):
            with TestClient(mobile_api.app) as client:
                with client.websocket_connect(
                    "/api/device/voice/realtime",
                    headers={"X-Device-Id": "stackchan-voice", "X-Device-Model": "StackChan"},
                ) as ws:
                    hello = ws.receive_json()
                    self.assertTrue(hello["capabilities"]["barge_in"])

                    ws.send_json({"type": "start", "conversation_id": "rt-1"})
                    self.assertEqual(ws.receive_json(), {"type": "status", "state": "listening"})
                    ws.send_json({"type": "speech_start"})
                    self.assertEqual(ws.receive_json(), {"type": "status", "state": "user_speaking"})
                    ws.send_json({"type": "barge_in", "conversation_id": "rt-2"})
                    interrupted = ws.receive_json()
                    self.assertEqual(interrupted["type"], "status")
                    self.assertEqual(interrupted["state"], "listening")
                    self.assertTrue(interrupted["interrupted"])

    def test_device_voice_query_uses_realtime_transcript_override_without_batch_stt(self):
        req = _request_with_headers({"X-Device-Id": "stackchan-voice"})
        with patch.object(mobile_api, "_device_transcribe_wav", side_effect=AssertionError("batch stt should be skipped")), patch.object(
            mobile_api, "_device_synthesize_speech_wav", return_value=_wav_bytes(data_bytes=b"\0" * 16000)
        ):
            out = mobile_api._build_device_voice_query_payload(
                username="tester",
                request=req,
                audio_bytes=_wav_bytes(data_bytes=b"\1\0" * 1600),
                transcript_override="现在几点",
            )

        self.assertEqual(out["route_type"], "instant_reply")
        self.assertEqual(out["transcript"], "现在几点")
        self.assertEqual(out["stt_status"], "ok")

    def test_device_config_clamps_auto_poll_seconds_when_enabled(self):
        req = _request_with_headers({"X-Device-Id": "stackchan-01"})
        with patch.object(mobile_api, "_DEVICE_AUTO_POLL_ENABLED_DEFAULT", True), patch.object(
            mobile_api, "_DEVICE_AUTO_POLL_SECONDS_DEFAULT", 12
        ):
            out = mobile_api.device_config(request=req, username="tester")

        self.assertTrue(out["auto_poll_enabled"])
        self.assertEqual(out["auto_poll_seconds"], 60)
        self.assertEqual(out["briefing_mode"], "hybrid")

    def test_device_briefing_builds_fresh_payload_from_available_sources(self):
        req = _request_with_headers({"X-Device-Id": "stackchan-01"})
        market_rows = [
            {"合约": "白银", "IV变动(日)": 3.2, "涨跌%(日)": -1.8},
            {"合约": "原油", "IV变动(日)": 0.6, "涨跌%(日)": 2.3},
        ]
        chaos = {"score_display": 71, "updated_at": "2026-05-05 09:30:00"}
        iv = {"index_ewma5": 66, "trade_date": "20260505", "regime": "偏热"}

        with patch.object(mobile_api.de, "get_comprehensive_market_data", return_value=pd.DataFrame(market_rows), create=True), patch.object(
            mobile_api.de, "get_latest_geopolitical_risk_snapshot", return_value=chaos
        ), patch.object(
            mobile_api.de, "get_cross_asset_iv_index", return_value=iv
        ):
            out = mobile_api.device_briefing(request=req, username="tester")

        self.assertEqual(out["user_id"], "tester")
        self.assertEqual(out["device_id"], "stackchan-01")
        self.assertEqual(out["market_state"], "risk_off")
        self.assertEqual(out["risk_level"], "high")
        self.assertEqual(out["chaos_index"], 71)
        self.assertEqual(out["iv_temperature"], 66)
        self.assertEqual(out["data_freshness"], "fresh")
        self.assertIn("IV", out["latest_alert"])
        self.assertTrue(out["headline"])
        self.assertTrue(out["speak_text"])

    def test_device_briefing_returns_degraded_fallback_when_sources_fail(self):
        req = _request_with_headers({"X-Device-Id": "stackchan-02"})
        with patch.object(mobile_api.de, "get_comprehensive_market_data", side_effect=RuntimeError("db down")), patch.object(
            mobile_api.de, "get_latest_geopolitical_risk_snapshot", side_effect=RuntimeError("risk down")
        ), patch.object(
            mobile_api.de, "get_cross_asset_iv_index", side_effect=RuntimeError("iv down")
        ):
            out = mobile_api.device_briefing(request=req, username="tester")

        self.assertEqual(out["market_state"], "neutral")
        self.assertEqual(out["risk_level"], "medium")
        self.assertEqual(out["data_freshness"], "degraded")
        self.assertIsNone(out["chaos_index"])
        self.assertIsNone(out["iv_temperature"])
        self.assertIn("降级", out["headline"])
        self.assertIn("降级", out["speak_text"])

    def test_device_contracts_menu_groups_products_and_contracts(self):
        req = _request_with_headers({"X-Device-Id": "stackchan-03"})
        option_payload = {
            "updated_at": "10:30",
            "items": [
                {"name": "PP2609 (聚丙烯)", "product_code": "pp", "cur_price": 7350.0, "pct_1d": 1.2, "iv": 18.5, "iv_rank": 62.0},
                {"name": "CU2606 (铜)", "product_code": "cu", "cur_price": 82300.0, "pct_1d": 0.8, "iv": 20.0, "iv_rank": 71.0},
            ],
        }
        contracts_by_product = {
            "pp": {
                "items": [
                    {"name": "PP2605", "cur_price": 7288.0, "pct_1d": -0.2, "iv": 16.1, "iv_rank": 42.0},
                    {"name": "PP2609", "cur_price": 7350.0, "pct_1d": 1.2, "iv": 18.5, "iv_rank": 62.0},
                ]
            },
            "cu": {
                "items": [
                    {"name": "CU2606", "cur_price": 82300.0, "pct_1d": 0.8, "iv": 20.0, "iv_rank": 71.0},
                ]
            },
        }

        with patch.object(mobile_api, "market_options", return_value=option_payload), patch.object(
            mobile_api, "market_contracts", side_effect=lambda product, username: contracts_by_product[product]
        ) as mock_contracts:
            out = mobile_api.device_contracts_menu(request=req, max_products=10, max_contracts=3, username="tester")

        mock_contracts.assert_not_called()
        self.assertEqual(out["device_id"], "stackchan-03")
        self.assertEqual(out["data_freshness"], "fresh")
        self.assertEqual(len(out["products"]), 2)
        by_code = {item["product_code"]: item for item in out["products"]}
        self.assertIn("pp", by_code)
        self.assertEqual(by_code["pp"]["product_name"], "聚丙烯")
        self.assertEqual(by_code["pp"]["contracts"][0]["contract"], "PP2609")
        self.assertEqual(len(by_code["pp"]["contracts"]), 1)
        self.assertEqual(by_code["cu"]["contracts"][0]["iv_rank"], 71.0)

    def test_device_contracts_menu_expands_one_product_on_demand(self):
        req = _request_with_headers({"X-Device-Id": "stackchan-03"})
        contracts_payload = {
            "items": [
                {"name": "PP2605", "cur_price": 7288.0, "pct_1d": -0.2, "iv": 16.1, "iv_rank": 42.0},
                {"name": "PP2609", "cur_price": 7350.0, "pct_1d": 1.2, "iv": 18.5, "iv_rank": 62.0},
            ]
        }

        with patch.object(mobile_api, "market_contracts", return_value=contracts_payload) as mock_contracts:
            out = mobile_api.device_contracts_menu(
                request=req,
                max_products=1,
                max_contracts=3,
                product="pp",
                username="tester",
            )

        mock_contracts.assert_called_once_with(product="pp", username="tester")
        self.assertEqual(out["data_freshness"], "fresh")
        self.assertEqual(len(out["products"]), 1)
        self.assertEqual(out["products"][0]["product_code"], "pp")
        self.assertEqual(out["products"][0]["contracts"][0]["contract"], "PP2609")
        self.assertEqual(len(out["products"][0]["contracts"]), 2)

    def test_device_contracts_menu_returns_etf_products_and_single_etf_contract(self):
        req = _request_with_headers({"X-Device-Id": "stackchan-etf"})

        out = mobile_api.device_contracts_menu(
            request=req,
            max_products=10,
            max_contracts=3,
            category="etf",
            username="tester",
        )

        self.assertEqual(out["category"], "etf")
        self.assertGreaterEqual(len(out["products"]), 3)
        by_code = {item["product_code"]: item for item in out["products"]}
        self.assertIn("510300.SH", by_code)
        self.assertEqual(by_code["510300.SH"]["contracts"], [])

        with patch.object(
            mobile_api,
            "_query_device_etf_snapshot",
            return_value={"latest_price": 4.123, "price_pct": 0.8, "iv": 18.6, "iv_rank": 55.2, "updated_at": "20260506"},
        ):
            expanded = mobile_api.device_contracts_menu(
                request=req,
                max_products=1,
                max_contracts=3,
                product="510300.SH",
                category="etf",
                username="tester",
            )

        self.assertEqual(expanded["products"][0]["contracts"][0]["contract"], "510300.SH")
        self.assertEqual(expanded["products"][0]["contracts"][0]["iv_rank"], 55.2)

    def test_device_contracts_menu_favorites_filters_to_preferred_products(self):
        req = _request_with_headers({"X-Device-Id": "stackchan-fav"})
        option_payload = {
            "updated_at": "10:30",
            "items": [
                {"name": "PP2609 (聚丙烯)", "product_code": "pp", "cur_price": 7350.0, "pct_1d": 1.2, "iv": 18.5, "iv_rank": 62.0},
                {"name": "CU2606 (铜)", "product_code": "cu", "cur_price": 82300.0, "pct_1d": 0.8, "iv": 20.0, "iv_rank": 71.0},
                {"name": "BU2606 (沥青)", "product_code": "bu", "cur_price": 3300.0, "pct_1d": 0.1, "iv": 15.0, "iv_rank": 20.0},
            ],
        }

        with patch.object(mobile_api, "market_options", return_value=option_payload):
            out = mobile_api.device_contracts_menu(
                request=req,
                max_products=10,
                max_contracts=1,
                category="favorites",
                username="tester",
            )

        codes = [item["product_code"] for item in out["products"]]
        self.assertEqual(codes, ["pp", "cu"])
        self.assertNotIn("bu", codes)

    def test_device_contract_briefing_returns_single_contract_payload(self):
        req = _request_with_headers({"X-Device-Id": "stackchan-04"})
        contracts_payload = {
            "items": [
                {"name": "PP2609", "cur_price": 7350.0, "pct_1d": 1.2, "iv": 18.5, "iv_rank": 62.0},
            ]
        }
        chart_payload = {
            "product": "pp",
            "cn_name": "聚丙烯",
            "main_contract": "PP2609",
            "cur_price": 7368.0,
            "cur_pct": 1.4,
            "cur_iv": 18.9,
            "db_cur_td": "20260505",
        }

        with patch.object(mobile_api, "market_contracts", return_value=contracts_payload), patch.object(
            mobile_api, "market_chart", return_value=chart_payload
        ):
            out = mobile_api.device_contract_briefing(request=req, contract="pp2609", username="tester")

        self.assertEqual(out["contract"], "PP2609")
        self.assertEqual(out["product_code"], "pp")
        self.assertEqual(out["product_name"], "聚丙烯")
        self.assertEqual(out["latest_price"], 7368.0)
        self.assertEqual(out["price_pct"], 1.4)
        self.assertEqual(out["iv"], 18.9)
        self.assertEqual(out["iv_rank"], 62.0)
        self.assertEqual(out["technical_state"], "pending")
        self.assertEqual(out["technical_label"], "待生成")
        self.assertIn("PP2609", out["headline"])
        self.assertTrue(out["speak_text"])

    def test_device_contract_briefing_returns_etf_payload(self):
        req = _request_with_headers({"X-Device-Id": "stackchan-etf"})
        with patch.object(
            mobile_api,
            "_query_device_etf_snapshot",
            return_value={"latest_price": 4.123, "price_pct": 0.8, "iv": 18.6, "iv_rank": 55.2, "updated_at": "20260506"},
        ):
            out = mobile_api.device_contract_briefing(
                request=req,
                contract="510300.SH",
                category="etf",
                username="tester",
            )

        self.assertEqual(out["category"], "etf")
        self.assertEqual(out["contract"], "510300.SH")
        self.assertEqual(out["product_name"], "沪深300ETF")
        self.assertEqual(out["latest_price"], 4.123)
        self.assertEqual(out["iv"], 18.6)
        self.assertEqual(out["iv_rank"], 55.2)
        self.assertEqual(out["technical_label"], "待生成")

    def test_device_contract_briefing_rejects_invalid_contract(self):
        req = _request_with_headers({"X-Device-Id": "stackchan-05"})

        with self.assertRaises(mobile_api.HTTPException) as ctx:
            mobile_api.device_contract_briefing(request=req, contract="bad-code", username="tester")

        self.assertEqual(ctx.exception.status_code, 400)

    def test_device_voice_query_returns_answer_and_audio_url(self):
        req = _request_with_headers({"X-Device-Id": "stackchan-voice"})
        upload = SimpleNamespace(file=io.BytesIO(_wav_bytes()))

        with patch.object(mobile_api, "_device_transcribe_wav", return_value="PP现在怎么看"), patch.object(
            mobile_api, "_device_generate_voice_answer", return_value="PP现在偏中性，IV处在中位。"
        ), patch.object(
            mobile_api, "_device_synthesize_speech_wav", return_value=_wav_bytes(data_bytes=b"\0" * 16000)
        ):
            out = mobile_api.device_voice_query(
                request=req,
                audio=upload,
                contract="PP2609",
                category="futures",
                screen_context="PP2609 detail",
                conversation_id="conv-1",
                username="tester",
            )

        self.assertEqual(out["device_id"], "stackchan-voice")
        self.assertEqual(out["conversation_id"], "conv-1")
        self.assertEqual(out["transcript"], "PP现在怎么看")
        self.assertEqual(out["answer_text"], "PP现在偏中性，IV处在中位。")
        self.assertEqual(out["emotion"], "speaking")
        self.assertEqual(out["action"], "speak")
        self.assertTrue(out["audio_url"].startswith("/api/device/voice/audio/"))
        self.assertIn("timings_ms", out)
        self.assertIn("upload_read_ms", out["timings_ms"])
        self.assertIn("stt_ms", out["timings_ms"])
        self.assertIn("server_total_ms", out["timings_ms"])

    def test_device_voice_query_rejects_invalid_wav(self):
        req = _request_with_headers({"X-Device-Id": "stackchan-voice"})
        upload = SimpleNamespace(file=io.BytesIO(b"not-wav"))

        with self.assertRaises(mobile_api.HTTPException) as ctx:
            mobile_api.device_voice_query(request=req, audio=upload, username="tester")

        self.assertEqual(ctx.exception.status_code, 400)

    def test_device_voice_query_returns_text_when_tts_fails(self):
        req = _request_with_headers({"X-Device-Id": "stackchan-voice"})
        upload = SimpleNamespace(file=io.BytesIO(_wav_bytes()))

        with patch.object(mobile_api, "_device_transcribe_wav", return_value="白银怎么看"), patch.object(
            mobile_api, "_device_generate_voice_answer", return_value="白银短线波动偏大，先观察。"
        ), patch.object(mobile_api, "_device_synthesize_speech_wav", return_value=None):
            out = mobile_api.device_voice_query(request=req, audio=upload, username="tester")

        self.assertEqual(out["answer_text"], "白银短线波动偏大，先观察。")
        self.assertEqual(out["action"], "display")
        self.assertEqual(out["audio_url"], "")
        self.assertEqual(out["data_freshness"], "degraded")

    def test_device_voice_query_speaks_quota_error_when_stt_fails(self):
        req = _request_with_headers({"X-Device-Id": "stackchan-voice"})
        upload = SimpleNamespace(file=io.BytesIO(_wav_bytes(data_bytes=b"\1" * 3200)))

        err = RuntimeError(
            "empty transcript status=403 code=Throttling.AllocationQuota "
            "message=Free allocated quota exceeded"
        )
        with patch.dict(mobile_api.os.environ, {"DEVICE_VOICE_AUDIO_DISK_CACHE_DISABLED": "1"}, clear=False), patch.object(
            mobile_api, "_device_transcribe_wav", side_effect=err
        ), patch.object(
            mobile_api, "_device_synthesize_speech_wav", return_value=_wav_bytes(data_bytes=b"\0" * 16000)
        ) as tts_mock:
            out = mobile_api.device_voice_query(request=req, audio=upload, username="tester")

        tts_mock.assert_called_once()
        self.assertEqual(out["transcript"], "语音识别失败")
        self.assertIn("语音识别额度暂时用完", out["answer_text"])
        self.assertEqual(out["action"], "speak")
        self.assertTrue(out["audio_url"].startswith("/api/device/voice/audio/"))
        self.assertEqual(out["stt_status"], "failed")
        self.assertIn("AllocationQuota", out["stt_error"])
        self.assertGreater(out["audio_duration_ms"], 0)
        self.assertGreater(out["audio_peak"], 0)

    def test_device_voice_query_classifies_quiet_audio_when_stt_empty(self):
        req = _request_with_headers({"X-Device-Id": "stackchan-voice"})
        upload = SimpleNamespace(file=io.BytesIO(_wav_bytes(data_bytes=b"\0" * 20000)))
        err = RuntimeError("empty transcript status=200 code= message= request_id=req-1")

        with patch.dict(mobile_api.os.environ, {"DEVICE_VOICE_AUDIO_DISK_CACHE_DISABLED": "1"}, clear=False), patch.object(
            mobile_api, "_device_transcribe_wav", side_effect=err
        ), patch.object(
            mobile_api, "_device_synthesize_speech_wav", return_value=_wav_bytes(data_bytes=b"\0" * 16000)
        ):
            out = mobile_api.device_voice_query(request=req, audio=upload, username="tester")

        self.assertEqual(out["stt_status"], "failed")
        self.assertEqual(out["stt_failure_reason"], "audio_too_quiet")
        self.assertEqual(out["answer_text"], "没听清楚，请再说一次哦。")
        self.assertEqual(out["stt_user_message"], out["answer_text"])

    def test_device_voice_query_classifies_short_audio_and_keeps_client_stats(self):
        req = _request_with_headers({"X-Device-Id": "stackchan-voice"})
        upload = SimpleNamespace(file=io.BytesIO(_wav_bytes(data_bytes=b"\1" * 1200)))
        err = RuntimeError("empty transcript status=200 code= message= request_id=req-2")

        with patch.dict(mobile_api.os.environ, {"DEVICE_VOICE_AUDIO_DISK_CACHE_DISABLED": "1"}, clear=False), patch.object(
            mobile_api, "_device_transcribe_wav", side_effect=err
        ), patch.object(
            mobile_api, "_device_synthesize_speech_wav", return_value=_wav_bytes(data_bytes=b"\0" * 16000)
        ):
            out = mobile_api.device_voice_query(
                request=req,
                audio=upload,
                client_audio_peak="1800",
                client_audio_rms="120.5",
                username="tester",
            )

        self.assertEqual(out["stt_failure_reason"], "recording_too_short")
        self.assertEqual(out["answer_text"], "没听清楚，请再说一次哦。")
        self.assertEqual(out["client_audio_peak"], 1800.0)
        self.assertEqual(out["client_audio_rms"], 120.5)

    def test_device_voice_market_fact_uses_explicit_etf_context(self):
        req = _request_with_headers({"X-Device-Id": "stackchan-voice"})
        upload = SimpleNamespace(file=io.BytesIO(_wav_bytes()))
        etf_payload = {
            "contract": "159915.SZ",
            "product_code": "159915.SZ",
            "product_name": "创业板ETF",
            "latest_price": 1.987,
            "price_pct": -0.42,
            "iv": 23.4,
            "iv_rank": 61.8,
            "technical_label": "待生成",
        }

        with patch.object(mobile_api, "_device_transcribe_wav", return_value="创业板ETF今天价格多少"), patch.object(
            mobile_api, "_build_device_contract_briefing_payload", return_value=etf_payload
        ) as briefing_mock, patch.object(
            mobile_api, "_device_generate_voice_answer"
        ) as llm_mock, patch.object(
            mobile_api, "_device_synthesize_speech_wav", return_value=None
        ):
            out = mobile_api.device_voice_query(
                request=req,
                audio=upload,
                contract="A2609",
                category="futures",
                username="tester",
            )

        briefing_mock.assert_called_once()
        call_kwargs = briefing_mock.call_args.kwargs
        self.assertEqual(call_kwargs["contract"], "159915.SZ")
        self.assertEqual(call_kwargs["category"], "etf")
        llm_mock.assert_not_called()
        self.assertIn("创业板ETF", out["answer_text"])
        self.assertIn("1.987", out["answer_text"])
        self.assertIn("IV Rank61.8", out["answer_text"])

    def test_device_voice_market_fact_maps_chinext_iv_to_etf_context(self):
        req = _request_with_headers({"X-Device-Id": "stackchan-voice"})
        upload = SimpleNamespace(file=io.BytesIO(_wav_bytes()))
        etf_payload = {
            "contract": "159915.SZ",
            "product_code": "159915.SZ",
            "product_name": "创业板ETF",
            "latest_price": 3.845,
            "price_pct": 1.6,
            "iv": 30.8,
            "iv_rank": 76.6,
            "technical_label": "待生成",
        }

        with patch.object(mobile_api, "_device_transcribe_wav", return_value="创业板的iv有多少"), patch.object(
            mobile_api, "_build_device_contract_briefing_payload", return_value=etf_payload
        ) as briefing_mock, patch.object(
            mobile_api, "_device_generate_voice_answer"
        ) as llm_mock, patch.object(
            mobile_api, "_device_synthesize_speech_wav", return_value=None
        ):
            out = mobile_api.device_voice_query(request=req, audio=upload, username="tester")

        briefing_mock.assert_called_once()
        call_kwargs = briefing_mock.call_args.kwargs
        self.assertEqual(call_kwargs["contract"], "159915.SZ")
        self.assertEqual(call_kwargs["category"], "etf")
        llm_mock.assert_not_called()
        self.assertIn("创业板ETF", out["answer_text"])
        self.assertIn("IV30.8", out["answer_text"])
        self.assertIn("IV Rank76.6", out["answer_text"])

    def test_device_voice_market_fact_refuses_to_invent_missing_data(self):
        req = _request_with_headers({"X-Device-Id": "stackchan-voice"})
        upload = SimpleNamespace(file=io.BytesIO(_wav_bytes()))

        with patch.object(mobile_api, "_device_transcribe_wav", return_value="创业板ETF最新价是多少"), patch.object(
            mobile_api, "_build_device_contract_briefing_payload", return_value={}
        ), patch.object(
            mobile_api, "_device_generate_voice_answer"
        ) as llm_mock, patch.object(
            mobile_api, "_device_synthesize_speech_wav", return_value=None
        ):
            out = mobile_api.device_voice_query(request=req, audio=upload, username="tester")

        llm_mock.assert_not_called()
        self.assertIn("不能直接报行情数字", out["answer_text"])
        self.assertEqual(out["data_freshness"], "degraded")

    def test_device_voice_market_fact_uses_stock_snapshot(self):
        req = _request_with_headers({"X-Device-Id": "stackchan-voice"})
        upload = SimpleNamespace(file=io.BytesIO(_wav_bytes()))
        stock_payload = {
            "category": "stock",
            "contract": "600519.SH",
            "product_code": "600519.SH",
            "product_name": "贵州茅台",
            "latest_price": 1700.12,
            "price_pct": 1.23,
            "iv": None,
            "iv_rank": None,
            "technical_label": "待生成",
        }

        with patch.object(mobile_api, "_device_transcribe_wav", return_value="茅台今天价格多少"), patch.object(
            mobile_api, "_build_device_stock_briefing_payload", return_value=stock_payload
        ) as briefing_mock, patch.object(
            mobile_api, "_device_generate_voice_answer"
        ) as llm_mock, patch.object(
            mobile_api, "_device_synthesize_speech_wav", return_value=None
        ):
            out = mobile_api.device_voice_query(request=req, audio=upload, username="tester")

        briefing_mock.assert_called_once()
        self.assertEqual(briefing_mock.call_args.kwargs["stock_code"], "600519.SH")
        llm_mock.assert_not_called()
        self.assertIn("贵州茅台", out["answer_text"])
        self.assertIn("1700.1", out["answer_text"])
        self.assertNotIn("IV Rank", out["answer_text"])

    def test_device_voice_market_fact_fuzzy_matches_asr_stock_name(self):
        req = _request_with_headers({"X-Device-Id": "stackchan-voice"})
        upload = SimpleNamespace(file=io.BytesIO(_wav_bytes()))
        stock_payload = {
            "category": "stock",
            "contract": "688008.SH",
            "product_code": "688008.SH",
            "product_name": "澜起科技",
            "latest_price": 74.2,
            "price_pct": -0.6,
            "iv": None,
            "iv_rank": None,
            "technical_label": "待生成",
        }

        with patch.object(mobile_api, "_device_transcribe_wav", return_value="蓝起科技今天价格多少"), patch.object(
            mobile_api, "_device_voice_stock_name_candidates", side_effect=AssertionError("fast alias should not load stock candidates")
        ) as candidates_mock, patch.object(
            mobile_api, "_build_device_stock_briefing_payload", return_value=stock_payload
        ) as briefing_mock, patch.object(
            mobile_api, "_device_generate_voice_answer"
        ) as llm_mock, patch.object(
            mobile_api, "_device_synthesize_speech_wav", return_value=None
        ):
            out = mobile_api.device_voice_query(request=req, audio=upload, username="tester")

        briefing_mock.assert_called_once()
        self.assertEqual(briefing_mock.call_args.kwargs["stock_code"], "688008.SH")
        self.assertEqual(briefing_mock.call_args.kwargs["name_hint"], "澜起科技")
        candidates_mock.assert_not_called()
        llm_mock.assert_not_called()
        self.assertIn("澜起科技", out["answer_text"])
        self.assertIn("74.2", out["answer_text"])

    def test_device_voice_stock_fast_alias_bypasses_slow_symbol_resolvers(self):
        with patch.object(
            mobile_api, "_device_voice_stock_name_candidates", side_effect=AssertionError("fast alias should not load candidates")
        ) as candidates_mock:
            code, name = mobile_api._resolve_device_voice_stock_symbol("蓝启科技今天涨跌多少")

        self.assertEqual(code, "688008.SH")
        self.assertEqual(name, "澜起科技")
        candidates_mock.assert_not_called()

    def test_device_voice_market_context_resolves_stock_once(self):
        req = _request_with_headers({"X-Device-Id": "stackchan-voice"})
        stock_payload = {
            "category": "stock",
            "contract": "688008.SH",
            "product_name": "澜起科技",
            "latest_price": 74.2,
        }

        with patch.object(
            mobile_api, "_resolve_device_voice_stock_symbol", return_value=("688008.SH", "澜起科技")
        ) as resolve_mock, patch.object(
            mobile_api, "_build_device_stock_briefing_payload", return_value=stock_payload
        ) as briefing_mock:
            out = mobile_api._load_device_voice_market_context(
                username="tester",
                request=req,
                transcript="澜起科技价格多少",
                contract="A2609",
                category="futures",
            )

        resolve_mock.assert_called_once()
        briefing_mock.assert_called_once()
        self.assertEqual(briefing_mock.call_args.kwargs["stock_code"], "688008.SH")
        self.assertEqual(briefing_mock.call_args.kwargs["name_hint"], "澜起科技")
        self.assertEqual(out["product_name"], "澜起科技")

    def test_device_stock_snapshot_uses_us_stock_prices_fallback(self):
        us_prices = pd.DataFrame(
            [
                {"ts_code": "NVDA", "name": "", "trade_date": "20260507", "close_price": 125.0},
                {"ts_code": "NVDA", "name": "", "trade_date": "20260506", "close_price": 100.0},
            ]
        )
        with patch("pandas.read_sql", side_effect=[pd.DataFrame(), us_prices]) as read_sql_mock:
            out = mobile_api._query_device_stock_snapshot("NVDA.US")

        self.assertEqual(read_sql_mock.call_count, 2)
        self.assertEqual(out["latest_price"], 125.0)
        self.assertEqual(out["price_pct"], 25.0)
        self.assertEqual(out["updated_at"], "20260507")

    def test_device_voice_market_fact_uses_futures_product_snapshot(self):
        req = _request_with_headers({"X-Device-Id": "stackchan-voice"})
        upload = SimpleNamespace(file=io.BytesIO(_wav_bytes()))
        contract_payload = {
            "category": "futures",
            "contract": "A2609",
            "product_code": "a",
            "product_name": "豆一",
            "latest_price": 4910.0,
            "price_pct": -0.8,
            "iv": 16.4,
            "iv_rank": 89.2,
            "technical_label": "待生成",
        }

        with patch.object(mobile_api, "_device_transcribe_wav", return_value="豆一现在价格多少"), patch.object(
            mobile_api, "market_contracts", return_value={"items": [{"name": "A2609 (豆一)"}]}
        ) as menu_mock, patch.object(
            mobile_api, "_build_device_contract_briefing_payload", return_value=contract_payload
        ) as briefing_mock, patch.object(
            mobile_api, "_device_generate_voice_answer"
        ) as llm_mock, patch.object(
            mobile_api, "_device_synthesize_speech_wav", return_value=None
        ):
            out = mobile_api.device_voice_query(request=req, audio=upload, username="tester")

        menu_mock.assert_called_once()
        briefing_mock.assert_called_once()
        self.assertEqual(briefing_mock.call_args.kwargs["contract"], "A2609")
        llm_mock.assert_not_called()
        self.assertIn("豆一", out["answer_text"])
        self.assertIn("4910", out["answer_text"])
        self.assertIn("IV Rank89.2", out["answer_text"])

    def test_device_voice_market_fact_preserves_time_question(self):
        req = _request_with_headers({"X-Device-Id": "stackchan-voice"})
        upload = SimpleNamespace(file=io.BytesIO(_wav_bytes()))
        contract_payload = {
            "category": "futures",
            "contract": "IF2606",
            "product_code": "if",
            "product_name": "沪深300ETF",
            "latest_price": 4.852,
            "price_pct": -0.5,
            "iv": 16.7,
            "iv_rank": 35.7,
            "technical_label": "待生成",
        }

        with patch.dict(mobile_api.os.environ, {"DEVICE_VOICE_AUDIO_DISK_CACHE_DISABLED": "1"}, clear=False), patch.object(
            mobile_api, "_device_transcribe_wav", return_value="现在几点，还有沪深300波动率是多少"
        ), patch.object(
            mobile_api, "_load_device_voice_market_context", return_value=contract_payload
        ), patch.object(
            mobile_api, "_device_synthesize_speech_wav", return_value=_wav_bytes(data_bytes=b"\0" * 16000)
        ):
            out = mobile_api.device_voice_query(request=req, audio=upload, username="tester")

        self.assertEqual(out["route_type"], "market_fact")
        self.assertIn("北京时间", out["answer_text"])
        self.assertIn("沪深300", out["answer_text"])
        self.assertIn("IV16.7", out["answer_text"])

    def test_device_voice_route_classifies_fast_and_deep_questions(self):
        self.assertEqual(mobile_api._classify_device_voice_route("创业板ETF价格多少"), "market_fact")
        self.assertEqual(mobile_api._classify_device_voice_route("创业板ETF IV多少"), "market_fact")
        self.assertEqual(mobile_api._classify_device_voice_route("现在几点了"), "instant_reply")
        self.assertEqual(mobile_api._classify_device_voice_route("黄金现在能做吗"), "deep_analysis")
        self.assertEqual(mobile_api._classify_device_voice_route("你好，沪深300现在的波动率是多少"), "market_fact")
        self.assertEqual(mobile_api._classify_device_voice_route("你好，黄金能不能做"), "deep_analysis")
        self.assertEqual(mobile_api._classify_device_voice_route("结束语音"), "stop_listening")
        self.assertEqual(mobile_api._classify_device_voice_route("你是谁"), "instant_reply")

    def test_device_voice_time_uses_instant_reply_without_market_or_llm(self):
        req = _request_with_headers({"X-Device-Id": "stackchan-voice"})
        upload = SimpleNamespace(file=io.BytesIO(_wav_bytes()))

        with patch.dict(mobile_api.os.environ, {"DEVICE_VOICE_AUDIO_DISK_CACHE_DISABLED": "1"}, clear=False), patch.object(
            mobile_api, "_device_transcribe_wav", return_value="现在几点了"
        ), patch.object(
            mobile_api, "_load_device_voice_market_context"
        ) as context_mock, patch.object(
            mobile_api, "_device_generate_voice_answer"
        ) as llm_mock, patch.object(
            mobile_api, "_device_synthesize_speech_wav", return_value=_wav_bytes(data_bytes=b"\0" * 16000)
        ):
            out = mobile_api.device_voice_query(request=req, audio=upload, username="tester")

        context_mock.assert_not_called()
        llm_mock.assert_not_called()
        self.assertEqual(out["route_type"], "instant_reply")
        self.assertIn("北京时间", out["answer_text"])
        self.assertTrue(out["audio_url"].startswith("/api/device/voice/audio/"))

    def test_device_voice_quick_ai_skips_market_context(self):
        req = _request_with_headers({"X-Device-Id": "stackchan-voice"})
        upload = SimpleNamespace(file=io.BytesIO(_wav_bytes()))

        with patch.object(mobile_api, "_device_transcribe_wav", return_value="你在干嘛"), patch.object(
            mobile_api, "_load_device_voice_market_context"
        ) as context_mock, patch.object(
            mobile_api, "_device_generate_voice_answer", return_value="我在，能帮你看行情、IV 或者做深度分析。"
        ) as llm_mock, patch.object(
            mobile_api, "_device_synthesize_speech_wav", return_value=_wav_bytes(data_bytes=b"\0" * 16000)
        ):
            out = mobile_api.device_voice_query(request=req, audio=upload, username="tester")

        context_mock.assert_not_called()
        llm_mock.assert_called_once()
        self.assertEqual(out["route_type"], "quick_ai")
        self.assertEqual(out["action"], "speak")

    def test_device_voice_hello_uses_instant_reply_without_llm_and_reuses_tts(self):
        req = _request_with_headers({"X-Device-Id": "stackchan-voice"})
        upload1 = SimpleNamespace(file=io.BytesIO(_wav_bytes()))
        upload2 = SimpleNamespace(file=io.BytesIO(_wav_bytes()))
        mobile_api._DEVICE_VOICE_AUDIO_CACHE.clear()
        mobile_api._DEVICE_VOICE_TEXT_AUDIO_CACHE.clear()

        with patch.dict(mobile_api.os.environ, {"DEVICE_VOICE_AUDIO_DISK_CACHE_DISABLED": "1"}, clear=False), patch.object(
            mobile_api, "_device_transcribe_wav", return_value="你好"
        ), patch.object(
            mobile_api, "_load_device_voice_market_context"
        ) as context_mock, patch.object(
            mobile_api, "_device_generate_voice_answer"
        ) as llm_mock, patch.object(
            mobile_api, "_device_synthesize_speech_wav", return_value=_wav_bytes(data_bytes=b"\0" * 16000)
        ) as tts_mock:
            first = mobile_api.device_voice_query(request=req, audio=upload1, username="tester")
            second = mobile_api.device_voice_query(request=req, audio=upload2, username="tester")

        context_mock.assert_not_called()
        llm_mock.assert_not_called()
        hello_text = mobile_api._DEVICE_VOICE_PROMPT_TEXTS["voice_hello"]
        hello_calls = [call for call in tts_mock.call_args_list if call.args and call.args[0] == hello_text]
        self.assertEqual(len(hello_calls), 1)
        self.assertEqual(first["route_type"], "instant_reply")
        self.assertEqual(second["route_type"], "instant_reply")
        self.assertIn("你好", first["answer_text"])
        self.assertEqual(first["audio_url"], second["audio_url"])

    def test_device_voice_audio_cache_ignores_tiny_wav_and_regenerates(self):
        good_audio = _wav_bytes(data_bytes=b"\0" * 16000)
        tiny_audio = _wav_bytes(data_bytes=b"\0" * 100)
        mobile_api._DEVICE_VOICE_AUDIO_CACHE.clear()
        mobile_api._DEVICE_VOICE_TEXT_AUDIO_CACHE.clear()

        with patch.object(mobile_api, "_read_device_voice_disk_audio", return_value=tiny_audio), patch.object(
            mobile_api, "_device_synthesize_speech_wav", return_value=good_audio
        ) as synth_mock, patch.object(
            mobile_api, "_write_device_voice_disk_audio"
        ):
            url = mobile_api._device_voice_audio_url_for_text("短音频缓存回归测试-不会命中旧坏音频")

        self.assertTrue(url.startswith("/api/device/voice/audio/"))
        synth_mock.assert_called_once()
        voice_id = url.rsplit("/", 1)[-1]
        cached = mobile_api._get_device_voice_audio(voice_id)
        self.assertGreater(len(cached or b""), len(tiny_audio))

    def test_device_voice_audio_playable_allows_long_tts_wav(self):
        long_audio = _wav_bytes(data_bytes=b"\0" * (mobile_api._DEVICE_VOICE_MAX_WAV_BYTES + 16000))

        self.assertTrue(mobile_api._device_voice_audio_playable(long_audio))

    def test_device_voice_stop_listening_returns_terminal_action(self):
        req = _request_with_headers({"X-Device-Id": "stackchan-voice"})
        upload = SimpleNamespace(file=io.BytesIO(_wav_bytes()))

        with patch.dict(mobile_api.os.environ, {"DEVICE_VOICE_AUDIO_DISK_CACHE_DISABLED": "1"}, clear=False), patch.object(
            mobile_api, "_device_transcribe_wav", return_value="结束语音"
        ), patch.object(
            mobile_api, "_device_synthesize_speech_wav", return_value=_wav_bytes(data_bytes=b"\0" * 16000)
        ):
            out = mobile_api.device_voice_query(request=req, audio=upload, username="tester")

        self.assertEqual(out["route_type"], "stop_listening")
        self.assertEqual(out["action"], "stop_listening")
        self.assertIn("不听", out["answer_text"])
        self.assertTrue(out["audio_url"].startswith("/api/device/voice/audio/"))

    def test_device_voice_deep_question_returns_thinking_task(self):
        req = _request_with_headers({"X-Device-Id": "stackchan-voice"})
        upload = SimpleNamespace(file=io.BytesIO(_wav_bytes()))
        contract_payload = {
            "category": "futures",
            "contract": "AU2606",
            "product_code": "au",
            "product_name": "黄金",
            "latest_price": 1041.0,
            "price_pct": 1.2,
            "iv": 24.1,
            "iv_rank": 28.3,
            "technical_label": "待生成",
        }

        with patch.object(mobile_api, "_device_transcribe_wav", return_value="黄金现在能做吗"), patch.object(
            mobile_api, "_load_device_voice_market_context", return_value=contract_payload
        ), patch.object(mobile_api.de, "get_user_profile", return_value={"risk_preference": "稳健型"}), patch.object(
            mobile_api, "_build_mobile_context_payload", return_value={"intent_domain": "option"}
        ), patch.object(mobile_api, "_detect_mobile_has_portfolio", return_value=False), patch.object(
            mobile_api, "_device_voice_active_task_id", return_value=""
        ), patch.object(
            mobile_api.TaskManager, "create_task", return_value="task-voice-1"
        ) as create_task_mock, patch.object(mobile_api, "_write_mobile_chat_state"), patch.object(
            mobile_api, "_set_mobile_chat_last_task"
        ), patch.object(mobile_api._redis, "setex"), patch.object(
            mobile_api, "_device_synthesize_speech_wav", return_value=_wav_bytes(data_bytes=b"\0" * 16000)
        ):
            out = mobile_api.device_voice_query(request=req, audio=upload, username="tester")

        create_task_mock.assert_called_once()
        task_kwargs = create_task_mock.call_args.kwargs
        self.assertEqual(task_kwargs["user_id"], "tester")
        self.assertEqual(task_kwargs["context_payload"]["chat_mode"], mobile_api.CHAT_MODE_ANALYSIS)
        self.assertTrue(task_kwargs["context_payload"]["device_voice"])
        self.assertEqual(out["route_type"], "deep_analysis")
        self.assertEqual(out["action"], "thinking")
        self.assertEqual(out["task_id"], "task-voice-1")
        self.assertEqual(out["poll_after_seconds"], mobile_api._DEVICE_VOICE_TASK_POLL_SECONDS)
        self.assertGreaterEqual(out["task_max_wait_seconds"], 300)
        self.assertIn("深度分析", out["answer_text"])
        self.assertTrue(out["audio_url"].startswith("/api/device/voice/audio/"))

    def test_device_voice_deep_question_rejects_second_deep_task_while_active(self):
        req = _request_with_headers({"X-Device-Id": "stackchan-voice"})
        upload = SimpleNamespace(file=io.BytesIO(_wav_bytes()))

        with patch.object(mobile_api, "_device_transcribe_wav", return_value="黄金现在能做吗"), patch.object(
            mobile_api, "_load_device_voice_market_context", return_value={}
        ), patch.object(
            mobile_api, "_device_voice_active_task_id", return_value="task-active"
        ), patch.object(
            mobile_api.TaskManager, "create_task"
        ) as create_task_mock, patch.object(
            mobile_api, "_device_synthesize_speech_wav", return_value=_wav_bytes(data_bytes=b"\0" * 16000)
        ):
            out = mobile_api.device_voice_query(request=req, audio=upload, username="tester")

        create_task_mock.assert_not_called()
        self.assertEqual(out["route_type"], "deep_analysis_busy")
        self.assertEqual(out["action"], "speak")
        self.assertIn("上一个复杂问题", out["answer_text"])

    def test_device_voice_deep_queue_full_speaks_error(self):
        req = _request_with_headers({"X-Device-Id": "stackchan-voice"})
        upload = SimpleNamespace(file=io.BytesIO(_wav_bytes()))

        with patch.object(mobile_api, "_device_transcribe_wav", return_value="黄金现在能做吗"), patch.object(
            mobile_api, "_load_device_voice_market_context", return_value={}
        ), patch.object(mobile_api.de, "get_user_profile", return_value={"risk_preference": "稳健型"}), patch.object(
            mobile_api, "_build_mobile_context_payload", return_value={"intent_domain": "option"}
        ), patch.object(mobile_api, "_detect_mobile_has_portfolio", return_value=False), patch.object(
            mobile_api, "_device_voice_active_task_id", return_value=""
        ), patch.object(
            mobile_api.TaskManager,
            "create_task",
            side_effect=mobile_api.UserTaskQueueFullError(1, 2, 2),
        ), patch.object(
            mobile_api, "_device_synthesize_speech_wav", return_value=_wav_bytes(data_bytes=b"\0" * 16000)
        ):
            out = mobile_api.device_voice_query(request=req, audio=upload, username="tester")

        self.assertEqual(out["route_type"], "deep_analysis")
        self.assertEqual(out["action"], "speak")
        self.assertEqual(out["emotion"], "error")
        self.assertEqual(out["data_freshness"], "degraded")
        self.assertIn("排队", out["answer_text"])

    def test_device_voice_task_success_wraps_chat_status_with_audio(self):
        req = _request_with_headers({"X-Device-Id": "stackchan-voice"})
        status_payload = {
            "status": "success",
            "result": {"response": "黄金短线趋势偏强，但IV不低。仓位建议轻一些，等回调再追踪。"},
        }

        with patch.object(mobile_api, "_read_device_voice_chat_status", return_value=status_payload), patch.object(
            mobile_api, "_device_synthesize_speech_wav", return_value=_wav_bytes(data_bytes=b"\0" * 16000)
        ):
            out = mobile_api.device_voice_task(task_id="task-voice-1", request=req, username="tester")

        self.assertEqual(out["status"], "success")
        self.assertEqual(out["route_type"], "deep_analysis")
        self.assertEqual(out["action"], "speak")
        self.assertIn("黄金短线趋势偏强", out["speak_text"])
        self.assertTrue(out["audio_url"].startswith("/api/device/voice/audio/"))
        self.assertIn("timings_ms", out)
        self.assertIn("status_read_ms", out["timings_ms"])
        self.assertIn("server_total_ms", out["timings_ms"])

    def test_device_voice_task_success_speaks_two_to_three_sentence_summary(self):
        req = _request_with_headers({"X-Device-Id": "stackchan-voice"})
        status_payload = {
            "status": "success",
            "result": {"response": "第一句结论。第二句理由。第三句风控。第四句长报告细节不该播。"},
        }

        with patch.object(mobile_api, "_read_device_voice_chat_status", return_value=status_payload), patch.object(
            mobile_api, "_device_synthesize_speech_wav", return_value=_wav_bytes(data_bytes=b"\0" * 16000)
        ):
            out = mobile_api.device_voice_task(task_id="task-voice-1", request=req, username="tester")

        self.assertEqual(out["speak_text"], "第一句结论。第二句理由。第三句风控。")
        self.assertNotIn("第四句", out["speak_text"])
        self.assertTrue(out["audio_url"].startswith("/api/device/voice/audio/"))

    def test_device_voice_task_processing_keeps_thinking_without_audio(self):
        req = _request_with_headers({"X-Device-Id": "stackchan-voice"})
        status_payload = {"status": "processing", "progress": "分析团队还在看技术面和波动率"}

        with patch.object(mobile_api, "_read_device_voice_chat_status", return_value=status_payload), patch.object(
            mobile_api, "_device_voice_task_elapsed_seconds", return_value=35
        ), patch.object(
            mobile_api, "_device_synthesize_speech_wav"
        ) as tts_mock:
            out = mobile_api.device_voice_task(task_id="task-voice-1", request=req, username="tester")

        self.assertEqual(out["status"], "processing")
        self.assertEqual(out["action"], "thinking")
        self.assertEqual(out["emotion"], "thinking")
        self.assertEqual(out["audio_url"], "")
        self.assertEqual(out["poll_after_seconds"], 5)

    def test_device_voice_task_processing_uses_slow_poll_after_two_minutes(self):
        req = _request_with_headers({"X-Device-Id": "stackchan-voice"})
        status_payload = {"status": "processing", "progress": "分析团队还在看技术面和波动率"}

        with patch.object(mobile_api, "_read_device_voice_chat_status", return_value=status_payload), patch.object(
            mobile_api, "_device_voice_task_elapsed_seconds", return_value=140
        ):
            out = mobile_api.device_voice_task(task_id="task-voice-1", request=req, username="tester")

        self.assertEqual(out["status"], "processing")
        self.assertEqual(out["poll_after_seconds"], 10)

    def test_device_voice_task_keeps_waiting_for_long_analysis_before_timeout(self):
        req = _request_with_headers({"X-Device-Id": "stackchan-voice"})
        status_payload = {"status": "processing", "progress": "分析团队还在看技术面和波动率"}

        with patch.object(mobile_api, "_read_device_voice_chat_status", return_value=status_payload), patch.object(
            mobile_api, "_device_voice_task_elapsed_seconds", return_value=450
        ), patch.object(
            mobile_api, "_read_mobile_chat_state", return_value={"status": "processing"}
        ), patch.object(
            mobile_api.TaskManager, "get_task_status", return_value={"status": "processing"}
        ), patch.object(
            mobile_api, "_device_synthesize_speech_wav"
        ) as tts_mock:
            out = mobile_api.device_voice_task(task_id="task-voice-1", request=req, username="tester")

        tts_mock.assert_not_called()
        self.assertEqual(out["status"], "processing")
        self.assertEqual(out["action"], "thinking")
        self.assertEqual(out["poll_after_seconds"], 10)

    def test_device_voice_task_keeps_processing_when_worker_status_is_pending(self):
        req = _request_with_headers({"X-Device-Id": "stackchan-voice"})
        status_payload = {"status": "processing", "progress": "分析团队还在看技术面和波动率"}

        with patch.object(mobile_api, "_read_device_voice_chat_status", return_value=status_payload), patch.object(
            mobile_api, "_device_voice_task_elapsed_seconds", return_value=45
        ), patch.object(
            mobile_api, "_read_mobile_chat_state", return_value={"status": "processing"}
        ), patch.object(
            mobile_api.TaskManager, "get_task_status", return_value={"status": "pending"}
        ), patch.object(
            mobile_api.TaskManager, "complete_user_task"
        ) as complete_mock, patch.object(
            mobile_api, "_write_mobile_chat_state"
        ) as state_mock, patch.object(
            mobile_api, "_device_synthesize_speech_wav"
        ) as tts_mock:
            out = mobile_api.device_voice_task(task_id="task-voice-1", request=req, username="tester")

        self.assertEqual(out["status"], "processing")
        self.assertEqual(out["action"], "thinking")
        self.assertEqual(out["emotion"], "thinking")
        self.assertEqual(out["task_status_source"], "runtime")
        self.assertEqual(out["state_status"], "processing")
        self.assertEqual(out["worker_status"], "pending")
        self.assertEqual(out["audio_url"], "")
        tts_mock.assert_not_called()
        complete_mock.assert_not_called()
        state_mock.assert_not_called()

    def test_device_voice_task_reports_lost_only_when_state_missing_after_grace(self):
        req = _request_with_headers({"X-Device-Id": "stackchan-voice"})
        status_payload = {"status": "processing", "progress": "分析团队还在看技术面和波动率"}

        with patch.object(mobile_api, "_read_device_voice_chat_status", return_value=status_payload), patch.object(
            mobile_api, "_device_voice_task_elapsed_seconds", return_value=301
        ), patch.object(
            mobile_api, "_read_mobile_chat_state", return_value={}
        ), patch.object(
            mobile_api.TaskManager, "get_task_status", return_value={"status": "pending"}
        ), patch.object(
            mobile_api, "_device_synthesize_speech_wav", return_value=_wav_bytes(data_bytes=b"\0" * 16000)
        ):
            out = mobile_api.device_voice_task(task_id="task-voice-1", request=req, username="tester")

        self.assertEqual(out["status"], "error")
        self.assertEqual(out["action"], "speak")
        self.assertEqual(out["task_status_source"], "lost")
        self.assertIn("状态丢失", out["speak_text"])

    def test_device_voice_task_reports_worker_missing_within_one_minute(self):
        req = _request_with_headers({"X-Device-Id": "stackchan-voice"})
        status_payload = {"status": "processing", "progress": "分析团队还在看技术面和波动率"}

        with patch.object(mobile_api, "_read_device_voice_chat_status", return_value=status_payload), patch.object(
            mobile_api, "_device_voice_task_elapsed_seconds", return_value=65
        ), patch.object(
            mobile_api, "_read_mobile_chat_state", return_value={}
        ), patch.object(
            mobile_api.TaskManager, "get_task_status", return_value={"status": "pending"}
        ), patch.object(
            mobile_api, "_device_synthesize_speech_wav", return_value=_wav_bytes(data_bytes=b"\0" * 16000)
        ):
            out = mobile_api.device_voice_task(task_id="task-voice-1", request=req, username="tester")

        self.assertEqual(out["status"], "error")
        self.assertEqual(out["action"], "speak")
        self.assertEqual(out["task_status_source"], "lost")
        self.assertIn("状态丢失", out["speak_text"])

    def test_device_voice_task_soft_timeout_speaks_after_max_wait(self):
        req = _request_with_headers({"X-Device-Id": "stackchan-voice"})
        status_payload = {"status": "processing", "progress": "分析团队还在看技术面和波动率"}

        with patch.object(mobile_api, "_read_device_voice_chat_status", return_value=status_payload), patch.object(
            mobile_api, "_device_voice_task_elapsed_seconds", return_value=901
        ), patch.object(
            mobile_api, "_device_synthesize_speech_wav", return_value=_wav_bytes(data_bytes=b"\0" * 16000)
        ):
            out = mobile_api.device_voice_task(task_id="task-voice-1", request=req, username="tester")

        self.assertEqual(out["status"], "timeout")
        self.assertEqual(out["action"], "speak")
        self.assertIn("稍后问我刚才结果", out["speak_text"])
        self.assertTrue(out["audio_url"].startswith("/api/device/voice/audio/"))

    def test_extract_dashscope_asr_text_from_sentence_list(self):
        response = SimpleNamespace(
            output={"sentence": [{"text": "豆一现在怎么看", "sentence_end": True}]},
            get_sentence=lambda: [{"text": "豆一现在怎么看", "sentence_end": True}],
        )

        self.assertEqual(mobile_api._extract_dashscope_asr_text(response), "豆一现在怎么看")

    def test_device_tts_defaults_to_qwen3_instruct_flash(self):
        wav = _wav_bytes(data_bytes=b"\2" * 200)
        qwen_response = SimpleNamespace(output={"audio": {"url": "https://example.com/voice.wav"}})
        download_response = SimpleNamespace(content=wav, raise_for_status=lambda: None)

        with patch.dict(mobile_api.os.environ, {"DASHSCOPE_API_KEY": "test-key"}, clear=False), patch(
            "dashscope.MultiModalConversation.call", return_value=qwen_response
        ) as call_mock, patch("requests.get", return_value=download_response) as get_mock:
            out = mobile_api._device_synthesize_speech_wav("请简短回答。")

        info = mobile_api._read_device_wav_info(out)
        stats = mobile_api._device_wav_signal_stats(out, info)
        self.assertEqual(info["sample_rate"], 16000)
        self.assertEqual(info["channels"], 1)
        self.assertEqual(info["bits_per_sample"], 16)
        self.assertGreater(stats["peak"], 0)
        call_kwargs = call_mock.call_args.kwargs
        self.assertEqual(call_kwargs["model"], "qwen3-tts-instruct-flash")
        self.assertEqual(call_kwargs["voice"], "Cherry")
        self.assertIn("交易助理", call_kwargs["instructions"])
        get_mock.assert_called_once_with("https://example.com/voice.wav", timeout=10)

    def test_device_tts_wav_is_normalized_and_volume_limited(self):
        sample = int(1000).to_bytes(2, "little", signed=True)
        wav = _wav_bytes(sample_rate=8000, data_bytes=sample * 4)

        with patch.dict(mobile_api.os.environ, {"DEVICE_TTS_VOLUME_GAIN": "2.0"}, clear=False):
            out = mobile_api._normalize_device_tts_wav(wav)

        info = mobile_api._read_device_wav_info(out)
        stats = mobile_api._device_wav_signal_stats(out, info)
        self.assertEqual(info["sample_rate"], 16000)
        self.assertEqual(info["channels"], 1)
        self.assertEqual(info["bits_per_sample"], 16)
        self.assertGreaterEqual(stats["peak"], 1900)

        hot_sample = int(30000).to_bytes(2, "little", signed=True)
        hot_wav = _wav_bytes(sample_rate=16000, data_bytes=hot_sample * 8)
        with patch.dict(mobile_api.os.environ, {"DEVICE_TTS_VOLUME_GAIN": "2.0"}, clear=False):
            limited = mobile_api._normalize_device_tts_wav(hot_wav)
        limited_info = mobile_api._read_device_wav_info(limited)
        limited_stats = mobile_api._device_wav_signal_stats(limited, limited_info)
        self.assertLessEqual(limited_stats["peak"], mobile_api._DEVICE_TTS_TARGET_PEAK + 1)

    def test_device_voice_audio_returns_cached_wav(self):
        req = _request_with_headers({"X-Device-Id": "stackchan-voice"})
        audio_bytes = _wav_bytes(data_bytes=b"\1" * 20)
        voice_id = mobile_api._store_device_voice_audio(audio_bytes)

        response = mobile_api.device_voice_audio(voice_id=voice_id, request=req, username="tester")

        self.assertEqual(response.media_type, "audio/wav")
        self.assertEqual(response.body, audio_bytes)


if __name__ == "__main__":
    unittest.main()
