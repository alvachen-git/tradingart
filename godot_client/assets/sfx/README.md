# SFX Placeholder

Drop optional audio files here to enable UI sounds (WAV/OGG):

- `card_pick.wav`
- `card_return.wav`
- `queue_reorder.wav`
- `queue_execute.wav`
- `turn_pass.wav`
- `score_up.wav`
- `score_down.wav`

If a file is missing, client falls back to silent (no error).

Current behavior:
- Missing file now uses built-in procedural beep fallback.
- If you provide the file, real asset audio takes priority.
