#pragma once

// Copy this file to device_secrets.h and fill in your real values.
// device_secrets.h is gitignored.

#define DEVICE_WIFI_SSID "YOUR_WIFI_NAME"
#define DEVICE_WIFI_PASSWORD "YOUR_WIFI_PASSWORD"
#define DEVICE_API_BASE_URL "http://192.168.1.10:8001"
#define DEVICE_API_BEARER_TOKEN "YOUR_BEARER_TOKEN"

#define DEVICE_ID "stackchan-dev-01"
#define DEVICE_MODEL "StackChan"
#define DEVICE_VERSION "v1"

// Realtime WebSocket audio is experimental. Keep it off unless you are
// explicitly debugging V3 capture/playback with serial logs.
#define DEVICE_VOICE_REALTIME_ENABLED 0
