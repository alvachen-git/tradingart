# StackChan Voice Latency Research

Date: 2026-05-09

## Goal

Make StackChan feel responsive while keeping TradingArt as the source of market facts and deep analysis.

Current V2.x voice flow is batch based:

1. Device records a full WAV clip.
2. Device uploads the whole WAV to `POST /api/device/voice/query`.
3. Backend runs STT, route classification, market lookup, optional AI/TTS.
4. Device downloads the whole WAV answer.
5. Device starts playback.

This is reliable, but it creates silent gaps. The fastest improvement is to remove the silent gaps first, then replace the batch audio path with a realtime path.

## Factory And Xiaozhi Pattern

References:

- M5Stack StackChan product docs: https://docs.m5stack.com/en/stackchan
- XiaoZhi WebSocket protocol docs: https://xiaozhi.dev/en/docs/development/websocket/
- Espressif Xiaozhi AI chatbot docs: https://docs.espressif.com/projects/esp-iot-solution/en/latest/ai/xiaozhi.html

The useful pattern is not only "which model is used". The interaction shape matters more:

- Keep a long-lived connection instead of creating a new HTTP request for every turn.
- Send small audio frames instead of one complete WAV after recording.
- Use status events such as listening, thinking, speaking, error.
- Give local feedback within about 150 ms: beep, face change, LED, or short cached prompt.
- Use a compressed audio codec such as Opus for network efficiency where device support is stable.
- Keep HTTP as fallback because embedded audio stacks are fragile.

## Timing Observability Added In V2.4.1

Backend `/api/device/voice/query` now returns `timings_ms`:

- `upload_read_ms`
- `audio_parse_ms`
- `context_parse_ms`
- `stt_ms`
- `route_ms`
- `market_context_ms`
- `llm_ms`
- `answer_ms`
- `deep_submit_ms`
- `tts_ms`
- `server_total_ms`

Backend `/api/device/voice/task/{task_id}` now returns `timings_ms`:

- `status_read_ms`
- `tts_ms`
- `server_total_ms`

Device monitor logs now include:

- `local_listen_cue_ms`
- `record_elapsed_ms`
- `stop_cue_ms`
- `query_build_ms`
- `query_http_ms`
- `query parse_ms`
- backend `server_*` timings
- `audio_fetch_ms`
- `playback_ms`
- task polling timings

## Decision Table

| Route | Expected speed | Workload | Control | Risk | TradingArt fit | Recommendation |
| --- | --- | --- | --- | --- | --- | --- |
| Direct factory/Xiaozhi cloud bridge | High if accepted | Medium | Low | High: vendor lock-in, auth, privacy, unknown commercial terms | Weak to medium, depends on whether tools can be injected | Research only, not production default |
| TradingArt realtime WebSocket | High | High | High | Medium: embedded audio and reconnect logic | Strong, all market and AI routing stays ours | Best V3 direction |
| HTTP with local cues and cached prompts | Medium | Low | High | Low | Strong | Keep as V2 fallback and short-term improvement |

## Realtime Protocol Status

### V3-alpha Implemented: Chunked WebSocket Bridge + Device Client

Backend `/api/device/voice/realtime` now supports a first realtime bridge:

- Keeps a WebSocket connection open.
- Accepts `start` / `audio` / `stop` text events.
- Accepts base64 `pcm_b64` frames and binary pcm16 frames.
- Emits `status`, `audio_ack`, `final_transcript`, `answer_delta`, `audio_url`, `tts_audio_delta`, `task`, and `done` events.
- Emits an 8 second `followup_listening` window after a turn.
- Accepts `barge_in` / `interrupt` to stop the current turn and return to listening.
- Tries DashScope realtime ASR while frames arrive and pushes `partial_transcript` / `final_transcript`; if realtime ASR is unavailable, it falls back to stop-time STT.
- Reuses the stable HTTP voice pipeline at `stop`, so STT and TTS are still batch-finalized.
- Keeps `/api/device/voice/query` as the HTTP fallback.

Device firmware now:

- Opens the realtime WebSocket when tap-to-wake recording starts.
- Sends 16k mono pcm16 frames about every 60 ms while recording.
- Falls back to the existing HTTP upload path when WebSocket connection fails.
- Handles server status/result/task events and plays pcm16 `tts_audio_delta` binary chunks.
- Sends `barge_in` when the user taps while audio is playing.

This removes the device-side "upload only after full WAV" limitation and gives immediate status feedback. The backend can use DashScope realtime ASR when available, but answer generation and TTS are still finalized after the turn; the next latency step is streaming LLM output into TTS while the answer is still being generated.

## Realtime Protocol Sketch

Endpoint:

```text
WS /api/device/voice/realtime
```

Handshake response:

```json
{
  "type": "hello",
  "protocol": "tradingart.stackchan.voice.realtime",
  "version": "research-v1",
  "mode": "prototype",
  "audio_format": {
    "codec": "pcm16",
    "sample_rate": 16000,
    "channels": 1,
    "frame_ms": 60
  },
  "capabilities": {
    "status_events": true,
    "binary_audio_frames": true,
    "json_audio_frames": true,
    "answer_delta": true,
    "tts_audio_delta": true,
    "streaming_tts": true,
    "barge_in": true,
    "followup_window_seconds": 8,
    "finalize_event": "stop",
    "http_fallback": "/api/device/voice/query"
  }
}
```

Planned V3 event shape:

```json
{"type":"status","state":"listening"}
{"type":"start","conversation_id":"rt-1","contract":"AU2606","category":"futures"}
{"type":"audio","seq":1,"format":"pcm16_16k_mono","pcm_b64":"..."}
{"type":"stop"}
{"type":"audio_ack","seq":1,"bytes":1920}
{"type":"status","state":"transcribing","bytes":1920}
{"type":"status","state":"thinking"}
{"type":"final_transcript","text":"黄金现在怎么看"}
{"type":"answer_delta","text":"黄金最新价格..."}
{"type":"audio_url","url":"/api/device/voice/audio/..."}
{"type":"tts_audio_delta","seq":1,"total":3,"encoding":"pcm16","sample_rate":16000,"channels":1,"audio_b64":"..."}
<binary pcm16 audio chunk>
{"type":"done","followup_window_seconds":8}
{"type":"status","state":"followup_listening","timeout_seconds":8}
```

## Recommended Roadmap

### V2.4.1: Measure And Remove Blank Waiting

- Add backend and device timing logs.
- Device plays local beep immediately on listen and send.
- Keep HTTP voice as the stable production path.

### V2.5: Better Turn Detection

- Add simple silence detection on device or backend.
- Stop recording soon after user stops speaking instead of waiting up to 8 seconds.
- Prewarm market alias/context caches at server startup or first config call.

### V3: TradingArt Realtime WebSocket

- Start with status events and text partials.
- Then add binary audio frames.
- Only add streaming TTS playback after current I2S playback crash risks stay quiet under repeated tests.

## Acceptance Metrics

For each test phrase, record:

- user tap to local feedback
- user stop speaking to backend request start
- request start to backend response
- response to first audio byte
- audio byte to playback start
- total turn time

Target:

- tap to local feedback: under 150 ms
- simple hot-cache first answer sound: 2-3 seconds
- complex analysis confirmation sound: under 3 seconds
- no silent wait during deep analysis
