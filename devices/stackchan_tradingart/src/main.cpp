#include <ArduinoJson.h>
#include <HTTPClient.h>
#include <M5Unified.h>
#include <WiFi.h>
#include <WebSocketsClient.h>
#include <esp_heap_caps.h>
#include <math.h>
#include <vector>

#if __has_include("device_secrets.h")
#include "device_secrets.h"
#endif

namespace app {

// Local secrets live in device_secrets.h, which is gitignored.
#ifndef DEVICE_WIFI_SSID
#define DEVICE_WIFI_SSID ""
#endif

#ifndef DEVICE_WIFI_PASSWORD
#define DEVICE_WIFI_PASSWORD ""
#endif

#ifndef DEVICE_API_BASE_URL
#define DEVICE_API_BASE_URL "http://192.168.1.10:8001"
#endif

#ifndef DEVICE_API_BEARER_TOKEN
#define DEVICE_API_BEARER_TOKEN ""
#endif

#ifndef DEVICE_ID
#define DEVICE_ID "stackchan-dev-01"
#endif

#ifndef DEVICE_MODEL
#define DEVICE_MODEL "StackChan"
#endif

#ifndef DEVICE_VERSION
#define DEVICE_VERSION "v1"
#endif

#ifndef DEVICE_SPEAKER_VOLUME
#define DEVICE_SPEAKER_VOLUME 160
#endif

#ifndef DEVICE_VOICE_REALTIME_ENABLED
#define DEVICE_VOICE_REALTIME_ENABLED 0
#endif

static constexpr char WIFI_SSID[] = DEVICE_WIFI_SSID;
static constexpr char WIFI_PASSWORD[] = DEVICE_WIFI_PASSWORD;
static constexpr char API_BASE_URL[] = DEVICE_API_BASE_URL;
static constexpr char API_BEARER_TOKEN[] = DEVICE_API_BEARER_TOKEN;
static constexpr char DEVICE_ID_VALUE[] = DEVICE_ID;
static constexpr char DEVICE_MODEL_VALUE[] = DEVICE_MODEL;
static constexpr char DEVICE_VERSION_VALUE[] = DEVICE_VERSION;
static constexpr uint8_t SPEAKER_VOLUME = DEVICE_SPEAKER_VOLUME;
static constexpr uint8_t SPEAKER_CHANNEL_VOLUME = DEVICE_SPEAKER_VOLUME;
static constexpr bool VOICE_REALTIME_ENABLED_DEFAULT = DEVICE_VOICE_REALTIME_ENABLED != 0;

static constexpr uint32_t HTTP_TIMEOUT_MS = 8000;
static constexpr uint32_t VOICE_QUERY_TIMEOUT_MS = 45000;
static constexpr uint32_t VOICE_AUDIO_TIMEOUT_MS = 15000;
static constexpr uint32_t VOICE_PLAYBACK_MAX_MS = 15000;
static constexpr uint32_t UI_REFRESH_MS = 33;
static constexpr uint32_t TOUCH_LONG_PRESS_MS = 800;
static constexpr int16_t DETAIL_MENU_TAP_Y = 46;
static constexpr int16_t DETAIL_VOICE_TAP_Y = 190;
static constexpr uint8_t MAX_PRODUCTS = 60;
static constexpr uint8_t MAX_CONTRACTS = 80;
static constexpr uint8_t MENU_PAGE_SIZE = 5;
static constexpr uint32_t VOICE_SAMPLE_RATE = 16000;
static constexpr uint32_t VOICE_MAX_SECONDS = 8;
static constexpr size_t VOICE_MAX_SAMPLES = VOICE_SAMPLE_RATE * VOICE_MAX_SECONDS;
static constexpr size_t VOICE_MAX_WAV_BYTES = 44 + VOICE_MAX_SAMPLES * sizeof(int16_t);
static constexpr uint16_t VOICE_CUE_LISTEN_HZ = 1320;
static constexpr uint16_t VOICE_CUE_SEND_HZ = 1760;
static constexpr uint32_t VOICE_CUE_MS = 45;
static constexpr uint32_t VOICE_MIN_RECORD_MS = 900;
static constexpr uint32_t VOICE_SILENCE_STOP_MS = 3000;
static constexpr uint32_t VOICE_SILENCE_SCAN_INTERVAL_MS = 60;
static constexpr uint32_t VOICE_REALTIME_FRAME_MS = 60;
static constexpr uint32_t VOICE_REALTIME_CONNECT_TIMEOUT_MS = 1200;
static constexpr uint32_t VOICE_FOLLOWUP_WINDOW_MS = 5000;
static constexpr uint32_t VOICE_CONVERSATION_IDLE_TIMEOUT_MS = 300000;
static constexpr uint32_t VOICE_SPEECH_PEAK_THRESHOLD = 3200;
static constexpr uint32_t VOICE_SPEECH_SPIKE_PEAK_THRESHOLD = 6500;
static constexpr float VOICE_SPEECH_RMS_THRESHOLD = 260.0f;
static constexpr float VOICE_SPEECH_SPIKE_RMS_THRESHOLD = 120.0f;
static constexpr uint8_t VOICE_MIN_SPEECH_CHUNKS = 4;
static constexpr uint32_t VOICE_UPLOAD_MIN_PEAK = 5000;
static constexpr float VOICE_UPLOAD_MIN_RMS = 100.0f;
static constexpr size_t VOICE_SCAN_CHUNK_SAMPLES = 320;

enum ViewMode {
  VIEW_DETAIL,
  VIEW_CATEGORY_MENU,
  VIEW_PRODUCT_MENU,
  VIEW_CONTRACT_MENU,
};

enum FaceState {
  FACE_IDLE,
  FACE_LISTENING,
  FACE_THINKING,
  FACE_SPEAKING,
  FACE_HAPPY,
  FACE_ERROR,
};

struct DeviceConfig {
  bool autoPollEnabled = false;
  bool voiceEnabled = true;
  uint32_t autoPollSeconds = 0;
  uint32_t recordMaxSeconds = VOICE_MAX_SECONDS;
  uint32_t voiceTaskPollSeconds = 2;
  uint32_t voiceTaskMaxWaitSeconds = 300;
  String briefingMode = "manual";
  String voiceMode = "tap_to_wake";
  String audioFormat = "wav_pcm_16k_mono";
};

struct BriefingPayload {
  bool ok = false;
  String marketState = "neutral";
  String riskLevel = "medium";
  String headline = "Tap the screen to query TradingArt.";
  String speakText = "";
  String latestAlert = "";
  String updatedAt = "";
  String dataFreshness = "degraded";
  int ivTemperature = -1;
  int chaosIndex = -1;
};

struct ContractItem {
  String category;
  String contract;
  String label;
  String productCode;
  float latestPrice = NAN;
  float pricePct = NAN;
  float iv = NAN;
  float ivRank = NAN;
};

struct ProductItem {
  String code;
  String name;
  uint8_t contractStart = 0;
  uint8_t contractCount = 0;
};

struct ContractBriefingPayload {
  bool ok = false;
  String contract = "";
  String productCode = "";
  String productName = "";
  String headline = "Long press to choose contract.";
  String speakText = "";
  String technicalLabel = "待生成";
  String updatedAt = "";
  String dataFreshness = "degraded";
  float latestPrice = NAN;
  float pricePct = NAN;
  float iv = NAN;
  float ivRank = NAN;
};

DeviceConfig g_config;
BriefingPayload g_briefing;
ContractBriefingPayload g_contractBriefing;
String g_voiceTranscript = "";
String g_voiceAnswer = "";
String g_conversationId = "";
String g_voiceTaskId = "";
ProductItem g_products[MAX_PRODUCTS];
ContractItem g_contracts[MAX_CONTRACTS];
uint8_t g_productCount = 0;
uint8_t g_contractCount = 0;
int8_t g_selectedProductIndex = -1;
int8_t g_selectedContractIndex = -1;
uint8_t g_menuPage = 0;
ViewMode g_viewMode = VIEW_DETAIL;
bool g_wifiReady = false;
bool g_serverReachable = false;
bool g_touchActive = false;
bool g_touchLongHandled = false;
int16_t g_touchX = 0;
int16_t g_touchY = 0;
uint32_t g_touchStartedAt = 0;
uint32_t g_lastUiDrawAt = 0;
uint32_t g_lastPollAt = 0;
bool g_uiDirty = true;
String g_statusLine = "Booting...";
String g_currentCategory = "futures";
FaceState g_faceState = FACE_IDLE;
bool g_voiceRecording = false;
bool g_voiceFollowupRecording = false;
bool g_voiceRealtimeEnabled = VOICE_REALTIME_ENABLED_DEFAULT;
bool g_voiceRealtimeConnected = false;
bool g_voiceRealtimeActive = false;
bool g_voiceRealtimeTurnOpen = false;
bool g_voiceRealtimeExpectingPcm = false;
bool g_voiceRealtimeSpeakerStarted = false;
bool g_voiceTaskActive = false;
uint32_t g_voiceStartedAt = 0;
uint32_t g_voiceTaskStartedAt = 0;
uint32_t g_voiceTaskLastPollAt = 0;
uint32_t g_voiceRealtimeLastSendAt = 0;
uint32_t g_voiceFollowupUntilAt = 0;
uint32_t g_voiceConversationUntilAt = 0;
bool g_voiceDetectedSpeech = false;
uint8_t g_voiceSpeechChunkCount = 0;
uint32_t g_voiceLastSpeechAt = 0;
uint32_t g_voiceLastSilenceScanAt = 0;
size_t g_voiceSilenceScanSamples = 0;
size_t g_voiceRealtimeSentSamples = 0;
int16_t* g_voiceBuffer = nullptr;
size_t g_voiceBufferSamples = 0;
std::vector<uint8_t> g_audioPlayback;
WebSocketsClient g_voiceWs;

String buildUrl(const char* path) {
  String base(API_BASE_URL);
  if (base.endsWith("/")) {
    base.remove(base.length() - 1);
  }
  return base + path;
}

String buildUrl(const char* path, const String& query) {
  String url = buildUrl(path);
  if (query.length() > 0) {
    url += "?";
    url += query;
  }
  return url;
}

String buildAbsoluteUrl(const String& value) {
  if (value.startsWith("http://") || value.startsWith("https://")) {
    return value;
  }
  if (value.startsWith("/")) {
    return buildUrl(value.c_str());
  }
  return buildUrl(("/" + value).c_str());
}

bool hasSecretsConfigured() {
  return strlen(WIFI_SSID) > 0 && strlen(API_BEARER_TOKEN) > 0;
}

void setStatus(const String& text) {
  if (g_statusLine == text) {
    return;
  }
  g_statusLine = text;
  g_uiDirty = true;
  Serial.println("[status] " + text);
}

void setFaceState(FaceState state) {
  if (g_faceState == state) {
    return;
  }
  g_faceState = state;
  g_uiDirty = true;
}

const lgfx::IFont* uiFontForSize(uint8_t textSize) {
  if (textSize >= 3) {
    return &fonts::efontCN_24;
  }
  if (textSize >= 2) {
    return &fonts::efontCN_16;
  }
  return &fonts::efontCN_12;
}

void drawWrappedText(const String& text, int16_t x, int16_t y, uint16_t color, uint8_t textSize) {
  M5.Display.setFont(uiFontForSize(textSize));
  M5.Display.setTextColor(color, TFT_BLACK);
  M5.Display.setTextSize(1);
  M5.Display.setTextWrap(true);
  M5.Display.setCursor(x, y);
  M5.Display.print(text);
}

String fmtFloat(float value, const char* suffix = "") {
  if (isnan(value)) {
    return "--";
  }
  String out(value, 1);
  while (out.endsWith("0")) {
    out.remove(out.length() - 1);
  }
  if (out.endsWith(".")) {
    out.remove(out.length() - 1);
  }
  out += suffix;
  return out;
}

String categoryLabel(const String& category) {
  if (category == "etf") return "ETF期权";
  if (category == "favorites") return "常用品种";
  return "商品期权";
}

void drawStackChanFace(int16_t x, int16_t y, int16_t w, int16_t h, FaceState state) {
  uint16_t frame = TFT_DARKGREY;
  uint16_t eye = TFT_WHITE;
  uint16_t mouth = TFT_WHITE;
  if (state == FACE_LISTENING) frame = TFT_GREEN;
  if (state == FACE_THINKING) frame = TFT_CYAN;
  if (state == FACE_SPEAKING) frame = TFT_BLUE;
  if (state == FACE_HAPPY) frame = TFT_GREENYELLOW;
  if (state == FACE_ERROR) {
    frame = TFT_RED;
    eye = TFT_RED;
    mouth = TFT_RED;
  }

  M5.Display.fillRoundRect(x, y, w, h, 8, TFT_BLACK);
  M5.Display.drawRoundRect(x, y, w, h, 8, frame);

  const int16_t leftEyeX = x + w / 3;
  const int16_t rightEyeX = x + (w * 2) / 3;
  const int16_t eyeY = y + h / 3;
  if (state == FACE_THINKING) {
    M5.Display.fillCircle(leftEyeX, eyeY, 3, eye);
    M5.Display.drawCircle(rightEyeX, eyeY, 5, eye);
  } else if (state == FACE_HAPPY) {
    M5.Display.drawArc(leftEyeX, eyeY + 3, 6, 4, 200, 340, eye);
    M5.Display.drawArc(rightEyeX, eyeY + 3, 6, 4, 200, 340, eye);
  } else if (state == FACE_ERROR) {
    M5.Display.drawLine(leftEyeX - 4, eyeY - 4, leftEyeX + 4, eyeY + 4, eye);
    M5.Display.drawLine(leftEyeX + 4, eyeY - 4, leftEyeX - 4, eyeY + 4, eye);
    M5.Display.drawLine(rightEyeX - 4, eyeY - 4, rightEyeX + 4, eyeY + 4, eye);
    M5.Display.drawLine(rightEyeX + 4, eyeY - 4, rightEyeX - 4, eyeY + 4, eye);
  } else {
    M5.Display.fillCircle(leftEyeX, eyeY, 4, eye);
    M5.Display.fillCircle(rightEyeX, eyeY, 4, eye);
  }

  const int16_t mouthY = y + (h * 2) / 3;
  if (state == FACE_SPEAKING) {
    M5.Display.drawRoundRect(x + w / 2 - 10, mouthY - 6, 20, 12, 5, mouth);
  } else if (state == FACE_LISTENING) {
    M5.Display.fillCircle(x + w / 2, mouthY, 4, mouth);
  } else if (state == FACE_HAPPY) {
    M5.Display.drawArc(x + w / 2, mouthY - 4, 16, 10, 20, 160, mouth);
  } else if (state == FACE_ERROR) {
    M5.Display.drawLine(x + w / 2 - 10, mouthY + 4, x + w / 2 + 10, mouthY - 4, mouth);
  } else {
    M5.Display.drawLine(x + w / 2 - 12, mouthY, x + w / 2 + 12, mouthY, mouth);
  }
}

void drawHeader(const String& title) {
  M5.Display.fillRoundRect(8, 8, 304, 34, 8, TFT_DARKGREY);
  drawWrappedText(title, 16, 18, TFT_WHITE, 2);
}

void drawDetailUi() {
  drawHeader("TradingArt 看板");
  drawStackChanFace(238, 50, 66, 56, g_faceState);

  if (g_contractBriefing.ok) {
    drawWrappedText(g_contractBriefing.contract + " " + g_contractBriefing.productName, 14, 52, TFT_WHITE, 2);
    drawWrappedText("价格 " + fmtFloat(g_contractBriefing.latestPrice) + "  " + fmtFloat(g_contractBriefing.pricePct, "%"), 14, 80, TFT_GREENYELLOW, 2);
    drawWrappedText("隐波 " + fmtFloat(g_contractBriefing.iv) + "  分位 " + fmtFloat(g_contractBriefing.ivRank), 14, 108, TFT_CYAN, 2);
    drawWrappedText("技术: " + g_contractBriefing.technicalLabel, 14, 136, TFT_ORANGE, 2);
    drawWrappedText(g_voiceAnswer.length() ? g_voiceAnswer : g_contractBriefing.headline, 14, 164, TFT_LIGHTGREY, 1);
    drawWrappedText("更新: " + (g_contractBriefing.updatedAt.length() ? g_contractBriefing.updatedAt : "-"), 14, 204, TFT_LIGHTGREY, 1);
  } else {
    uint16_t stateColor = TFT_YELLOW;
    if (g_briefing.marketState == "risk_off") stateColor = TFT_RED;
    if (g_briefing.marketState == "risk_on") stateColor = TFT_GREEN;

    drawWrappedText("状态:", 14, 54, TFT_LIGHTGREY, 2);
    drawWrappedText(g_briefing.marketState, 90, 54, stateColor, 2);

    String metricLine = "IV ";
    metricLine += (g_briefing.ivTemperature >= 0) ? String(g_briefing.ivTemperature) : "--";
    metricLine += "  Chaos ";
    metricLine += (g_briefing.chaosIndex >= 0) ? String(g_briefing.chaosIndex) : "--";
    drawWrappedText(metricLine, 14, 82, TFT_CYAN, 2);
    drawWrappedText(g_briefing.headline, 14, 116, TFT_WHITE, 2);
    drawWrappedText("更新: " + (g_briefing.updatedAt.length() ? g_briefing.updatedAt : "-"), 14, 204, TFT_LIGHTGREY, 1);
  }

  drawWrappedText("点标题菜单  点正文刷新  点底部语音", 14, 212, TFT_LIGHTGREY, 1);
  drawWrappedText(g_statusLine, 14, 226, TFT_GREENYELLOW, 1);
}

void drawMenuRow(uint8_t visibleIndex, const String& text, uint16_t color) {
  const int16_t y = 50 + visibleIndex * 30;
  M5.Display.drawRoundRect(10, y - 4, 300, 26, 5, TFT_DARKGREY);
  drawWrappedText(text, 18, y, color, 2);
}

void drawCategoryMenuUi() {
  drawHeader("选择市场");
  drawMenuRow(0, "商品期权", TFT_WHITE);
  drawMenuRow(1, "ETF期权", TFT_WHITE);
  drawMenuRow(2, "常用品种", TFT_WHITE);
  drawWrappedText("点选分类 / 长按返回看板", 12, 212, TFT_LIGHTGREY, 1);
  drawWrappedText(g_statusLine, 12, 226, TFT_GREENYELLOW, 1);
}

void drawProductMenuUi() {
  String title = "选择";
  title += categoryLabel(g_currentCategory);
  drawHeader(title);
  const uint8_t start = g_menuPage * MENU_PAGE_SIZE;
  for (uint8_t i = 0; i < MENU_PAGE_SIZE; ++i) {
    const uint8_t idx = start + i;
    if (idx >= g_productCount) break;
    String code = g_products[idx].code;
    code.toUpperCase();
    String label = g_products[idx].name;
    label += " ";
    label += code;
    drawMenuRow(i, label, TFT_WHITE);
  }
  const uint8_t totalPages = g_productCount == 0 ? 1 : ((g_productCount - 1) / MENU_PAGE_SIZE) + 1;
  String footer = "第" + String(g_menuPage + 1) + "/" + String(totalPages) + "页  点底部翻页";
  drawWrappedText(footer, 12, 212, TFT_LIGHTGREY, 1);
  drawWrappedText(g_statusLine, 12, 226, TFT_GREENYELLOW, 1);
}

void drawContractMenuUi() {
  drawHeader("选择合约");
  if (g_selectedProductIndex < 0 || g_selectedProductIndex >= g_productCount) {
    drawWrappedText("未选择品种", 16, 70, TFT_RED, 2);
    drawWrappedText(g_statusLine, 12, 226, TFT_GREENYELLOW, 1);
    return;
  }

  ProductItem& product = g_products[g_selectedProductIndex];
  const uint8_t start = product.contractStart + g_menuPage * MENU_PAGE_SIZE;
  for (uint8_t i = 0; i < MENU_PAGE_SIZE; ++i) {
    const uint8_t idx = start + i;
    if (i >= product.contractCount || idx >= product.contractStart + product.contractCount || idx >= g_contractCount) break;
    const ContractItem& item = g_contracts[idx];
    String label = item.contract + " IV " + fmtFloat(item.iv) + " R " + fmtFloat(item.ivRank);
    drawMenuRow(i, label, TFT_WHITE);
  }
  const uint8_t totalPages = product.contractCount == 0 ? 1 : ((product.contractCount - 1) / MENU_PAGE_SIZE) + 1;
  String footer = "第" + String(g_menuPage + 1) + "/" + String(totalPages) + "页  点底部翻页";
  drawWrappedText(footer, 12, 212, TFT_LIGHTGREY, 1);
  drawWrappedText(g_statusLine, 12, 226, TFT_GREENYELLOW, 1);
}

void drawUi() {
  if (!g_uiDirty) {
    return;
  }
  if ((millis() - g_lastUiDrawAt) < UI_REFRESH_MS) {
    return;
  }
  g_lastUiDrawAt = millis();

  M5.Display.startWrite();
  M5.Display.fillScreen(TFT_BLACK);

  if (g_viewMode == VIEW_CATEGORY_MENU) {
    drawCategoryMenuUi();
  } else if (g_viewMode == VIEW_PRODUCT_MENU) {
    drawProductMenuUi();
  } else if (g_viewMode == VIEW_CONTRACT_MENU) {
    drawContractMenuUi();
  } else {
    drawDetailUi();
  }

  M5.Display.endWrite();
  g_uiDirty = false;
}

bool beginHttp(HTTPClient& http, const String& url, uint32_t timeoutMs = HTTP_TIMEOUT_MS) {
  if (!http.begin(url)) {
    setStatus("HTTP初始化失败");
    return false;
  }
  http.setTimeout(timeoutMs);
  http.addHeader("Authorization", String("Bearer ") + API_BEARER_TOKEN);
  http.addHeader("X-Device-Id", DEVICE_ID_VALUE);
  http.addHeader("X-Device-Model", DEVICE_MODEL_VALUE);
  http.addHeader("X-Device-Version", DEVICE_VERSION_VALUE);
  return true;
}

bool connectWifi() {
  if (!hasSecretsConfigured()) {
    setStatus("请配置设备密钥");
    return false;
  }

  setStatus("正在连接Wi-Fi...");
  WiFi.mode(WIFI_STA);
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD);

  const uint32_t startedAt = millis();
  while (WiFi.status() != WL_CONNECTED && (millis() - startedAt) < 15000) {
    delay(250);
    M5.update();
  }

  g_wifiReady = (WiFi.status() == WL_CONNECTED);
  if (g_wifiReady) {
    setStatus("Wi-Fi已连接: " + WiFi.localIP().toString());
  } else {
    setStatus("Wi-Fi连接失败");
  }
  return g_wifiReady;
}

bool fetchPing() {
  if (!g_wifiReady) return false;

  HTTPClient http;
  if (!beginHttp(http, buildUrl("/api/device/ping"))) {
    return false;
  }

  const int code = http.GET();
  const String body = http.getString();
  http.end();

  if (code != 200) {
    setStatus("服务检查失败: " + String(code));
    return false;
  }

  JsonDocument doc;
  if (deserializeJson(doc, body) != DeserializationError::Ok) {
    setStatus("服务检查解析失败");
    return false;
  }

  g_serverReachable = bool(doc["ok"] | false);
  setStatus(g_serverReachable ? "Server ping OK" : "Ping response invalid");
  return g_serverReachable;
}

bool fetchConfig() {
  if (!g_wifiReady) return false;

  HTTPClient http;
  if (!beginHttp(http, buildUrl("/api/device/config"))) {
    return false;
  }

  const int code = http.GET();
  const String body = http.getString();
  http.end();

  if (code != 200) {
    setStatus("配置失败: " + String(code));
    return false;
  }

  JsonDocument doc;
  if (deserializeJson(doc, body) != DeserializationError::Ok) {
    setStatus("配置解析失败");
    return false;
  }

  g_config.autoPollEnabled = bool(doc["auto_poll_enabled"] | false);
  g_config.voiceEnabled = bool(doc["voice_enabled"] | false);
  g_config.autoPollSeconds = uint32_t(doc["auto_poll_seconds"] | 0);
  g_config.recordMaxSeconds = uint32_t(doc["record_max_seconds"] | VOICE_MAX_SECONDS);
  if (g_config.recordMaxSeconds == 0 || g_config.recordMaxSeconds > VOICE_MAX_SECONDS) {
    g_config.recordMaxSeconds = VOICE_MAX_SECONDS;
  }
  g_config.voiceTaskPollSeconds = uint32_t(doc["voice_task_poll_seconds"] | 2);
  if (g_config.voiceTaskPollSeconds == 0) {
    g_config.voiceTaskPollSeconds = 2;
  }
  g_config.voiceTaskMaxWaitSeconds = uint32_t(doc["voice_task_max_wait_seconds"] | 60);
  if (g_config.voiceTaskMaxWaitSeconds < g_config.voiceTaskPollSeconds) {
    g_config.voiceTaskMaxWaitSeconds = g_config.voiceTaskPollSeconds;
  }
  if (g_config.voiceTaskMaxWaitSeconds < 300) {
    g_config.voiceTaskMaxWaitSeconds = 300;
  }
  g_config.briefingMode = String(doc["briefing_mode"] | "manual");
  g_config.voiceMode = String(doc["voice_mode"] | "tap_to_wake");
  g_config.audioFormat = String(doc["audio_format"] | "wav_pcm_16k_mono");
  g_uiDirty = true;
  setStatus("配置已加载: " + g_config.briefingMode);
  return true;
}

void announceBriefing() {
  String text = g_contractBriefing.ok ? g_contractBriefing.speakText : g_briefing.speakText;
  if (text.length() == 0) {
    return;
  }
  Serial.println("[speak] " + text);
  M5.Speaker.tone(880, 120);
}

String selectedContract();

void appendString(std::vector<uint8_t>& body, const String& text) {
  const char* raw = text.c_str();
  body.insert(body.end(), raw, raw + text.length());
}

void appendBytes(std::vector<uint8_t>& body, const uint8_t* data, size_t len) {
  body.insert(body.end(), data, data + len);
}

void appendWavHeader(std::vector<uint8_t>& wav, size_t pcmBytes) {
  const uint32_t chunkSize = 36 + pcmBytes;
  const uint32_t sampleRate = VOICE_SAMPLE_RATE;
  const uint16_t channels = 1;
  const uint16_t bitsPerSample = 16;
  const uint32_t byteRate = sampleRate * channels * bitsPerSample / 8;
  const uint16_t blockAlign = channels * bitsPerSample / 8;
  auto put16 = [&wav](uint16_t v) {
    wav.push_back(uint8_t(v & 0xff));
    wav.push_back(uint8_t((v >> 8) & 0xff));
  };
  auto put32 = [&wav](uint32_t v) {
    wav.push_back(uint8_t(v & 0xff));
    wav.push_back(uint8_t((v >> 8) & 0xff));
    wav.push_back(uint8_t((v >> 16) & 0xff));
    wav.push_back(uint8_t((v >> 24) & 0xff));
  };
  appendString(wav, "RIFF");
  put32(chunkSize);
  appendString(wav, "WAVEfmt ");
  put32(16);
  put16(1);
  put16(channels);
  put32(sampleRate);
  put32(byteRate);
  put16(blockAlign);
  put16(bitsPerSample);
  appendString(wav, "data");
  put32(pcmBytes);
}

void computePcmStats(const int16_t* samples, size_t sampleCount, int32_t& peak, float& rms) {
  peak = 0;
  double squareSum = 0.0;
  if (!samples || sampleCount == 0) {
    rms = 0.0f;
    return;
  }

  for (size_t i = 0; i < sampleCount; ++i) {
    const int32_t sample = int32_t(samples[i]);
    const int32_t absSample = sample < 0 ? -sample : sample;
    if (absSample > peak) {
      peak = absSample;
    }
    squareSum += double(sample) * double(sample);
  }
  rms = float(sqrt(squareSum / double(sampleCount)));
}

bool updateVoiceSilenceDetector(uint32_t nowMs, uint32_t elapsedMs) {
  if (!g_voiceBuffer || elapsedMs < 120) {
    return false;
  }
  if (g_voiceLastSilenceScanAt > 0 && (nowMs - g_voiceLastSilenceScanAt) < VOICE_SILENCE_SCAN_INTERVAL_MS) {
    return false;
  }
  g_voiceLastSilenceScanAt = nowMs;

  size_t availableSamples = size_t((uint64_t(elapsedMs) * VOICE_SAMPLE_RATE) / 1000ULL);
  availableSamples = min(availableSamples, VOICE_MAX_SAMPLES);
  if (availableSamples <= g_voiceSilenceScanSamples) {
    return false;
  }

  size_t scanFrom = g_voiceSilenceScanSamples;
  while (scanFrom + VOICE_SCAN_CHUNK_SAMPLES <= availableSamples) {
    int32_t peak = 0;
    float rms = 0.0f;
    computePcmStats(g_voiceBuffer + scanFrom, VOICE_SCAN_CHUNK_SAMPLES, peak, rms);
    const size_t chunkEnd = scanFrom + VOICE_SCAN_CHUNK_SAMPLES;
    const bool sustainedVoice = rms >= VOICE_SPEECH_RMS_THRESHOLD;
    const bool strongVoiceSpike = uint32_t(peak) >= VOICE_SPEECH_SPIKE_PEAK_THRESHOLD && rms >= VOICE_SPEECH_SPIKE_RMS_THRESHOLD;
    if (sustainedVoice || strongVoiceSpike) {
      g_voiceDetectedSpeech = true;
      if (g_voiceSpeechChunkCount < 255) {
        ++g_voiceSpeechChunkCount;
      }
      g_voiceLastSpeechAt = g_voiceStartedAt + uint32_t((uint64_t(chunkEnd) * 1000ULL) / VOICE_SAMPLE_RATE);
    }
    scanFrom = chunkEnd;
  }
  g_voiceSilenceScanSamples = scanFrom;

  if (g_voiceDetectedSpeech && elapsedMs >= VOICE_MIN_RECORD_MS && (nowMs - g_voiceLastSpeechAt) >= VOICE_SILENCE_STOP_MS) {
    Serial.printf(
      "[voice_timing] silence_stop elapsed_ms=%u quiet_ms=%u scanned=%u\n",
      unsigned(elapsedMs),
      unsigned(nowMs - g_voiceLastSpeechAt),
      unsigned(g_voiceSilenceScanSamples)
    );
    return true;
  }
  return false;
}

bool ensureVoiceBuffer() {
  if (g_voiceBuffer) {
    return true;
  }
  g_voiceBufferSamples = VOICE_MAX_SAMPLES;
  g_voiceBuffer = static_cast<int16_t*>(heap_caps_malloc(g_voiceBufferSamples * sizeof(int16_t), MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT));
  if (!g_voiceBuffer) {
    g_voiceBuffer = static_cast<int16_t*>(malloc(g_voiceBufferSamples * sizeof(int16_t)));
  }
  if (!g_voiceBuffer) {
    setStatus("录音内存不足");
    setFaceState(FACE_ERROR);
    return false;
  }
  return true;
}

void quietSpeaker(const char* reason = "") {
  if (M5.Speaker.isPlaying()) {
    M5.Speaker.stop();
  }
  M5.Speaker.setVolume(0);
  M5.Speaker.setAllChannelVolume(0);
  delay(6);
  M5.Speaker.end();
  if (reason && reason[0] != '\0') {
    Serial.printf("[voice_timing] speaker_quiet reason=%s\n", reason);
  }
}

void playLocalVoiceCue(uint16_t hz) {
  if (hz == 0) {
    return;
  }
  quietSpeaker("before_cue");
  delay(8);
  if (!M5.Speaker.begin()) {
    Serial.println("[voice_timing] local_cue_failed=speaker_begin");
    return;
  }
  M5.Speaker.setVolume(SPEAKER_VOLUME);
  M5.Speaker.setAllChannelVolume(SPEAKER_CHANNEL_VOLUME);
  M5.Speaker.tone(hz, VOICE_CUE_MS);
  delay(VOICE_CUE_MS + 8);
  quietSpeaker("after_cue");
}

String jsonStringValue(const JsonDocument& doc);
bool beginVoiceRealtimeSession();
bool sendVoiceRealtimeFrames(size_t availableSamples, bool flushTail);
void finishVoiceRealtimeTurn(size_t samples);
void handleVoiceRealtimeLoop();

void beginVoiceRecording(bool followupMode = false) {
  if (!g_config.voiceEnabled) {
    if (g_wifiReady) {
      setStatus("正在刷新语音配置...");
      fetchConfig();
    }
  }
  if (!g_config.voiceEnabled) {
    setStatus("语音未开启");
    setFaceState(FACE_ERROR);
    return;
  }
  if (g_voiceRecording) {
    return;
  }
  if (!ensureVoiceBuffer()) {
    return;
  }
  if (M5.Speaker.isPlaying()) {
    M5.Speaker.stop();
    if (g_voiceRealtimeConnected) {
      JsonDocument interruptDoc;
      interruptDoc["type"] = "barge_in";
      String interruptMessage = jsonStringValue(interruptDoc);
      g_voiceWs.sendTXT(interruptMessage);
      Serial.println("[voice_ws] barge_in_sent");
    }
  }
  const uint32_t cueStartedAt = millis();
  setFaceState(FACE_LISTENING);
  g_voiceFollowupRecording = followupMode;
  setStatus(followupMode ? "继续问我" : "正在聆听，再点一次发送");
  if (followupMode) {
    quietSpeaker("followup_listen");
  } else {
    playLocalVoiceCue(VOICE_CUE_LISTEN_HZ);
  }
  Serial.printf("[voice_timing] local_listen_cue_ms=%u\n", unsigned(millis() - cueStartedAt));
  delay(20);
  memset(g_voiceBuffer, 0, g_voiceBufferSamples * sizeof(int16_t));
  if (!M5.Mic.begin()) {
    setStatus("麦克风启动失败");
    setFaceState(FACE_ERROR);
    return;
  }
  if (!M5.Mic.record(g_voiceBuffer, g_voiceBufferSamples, VOICE_SAMPLE_RATE, false)) {
    M5.Mic.end();
    setStatus("录音启动失败");
    setFaceState(FACE_ERROR);
    return;
  }
  g_voiceRecording = true;
  g_voiceStartedAt = millis();
  g_voiceDetectedSpeech = false;
  g_voiceSpeechChunkCount = 0;
  g_voiceLastSpeechAt = g_voiceStartedAt;
  g_voiceLastSilenceScanAt = 0;
  g_voiceSilenceScanSamples = 0;
  if (!followupMode) {
    g_voiceAnswer = "";
    g_voiceTranscript = "";
  }
  if (!beginVoiceRealtimeSession()) {
    Serial.println("[voice_ws] realtime_unavailable fallback=http");
  }
}

void startVoiceFollowupListening(bool extendConversation = true) {
  if (!g_config.voiceEnabled || g_voiceRecording || g_viewMode != VIEW_DETAIL) {
    return;
  }
  if (extendConversation || g_voiceConversationUntilAt == 0) {
    g_voiceConversationUntilAt = millis() + VOICE_CONVERSATION_IDLE_TIMEOUT_MS;
  }
  if (int32_t(millis() - g_voiceConversationUntilAt) >= 0) {
    g_voiceConversationUntilAt = 0;
    setFaceState(FACE_IDLE);
    setStatus("待机");
    Serial.println("[voice] followup_conversation_timeout");
    return;
  }
  beginVoiceRecording(true);
}

bool fetchVoiceAudio(const String& audioUrl) {
  if (audioUrl.length() == 0) {
    return false;
  }
  const uint32_t startedAt = millis();
  HTTPClient http;
  if (!beginHttp(http, buildAbsoluteUrl(audioUrl), VOICE_AUDIO_TIMEOUT_MS)) {
    return false;
  }
  const int code = http.GET();
  if (code != 200) {
    http.end();
    setStatus("语音下载失败: " + String(code));
    return false;
  }
  const int len = http.getSize();
  WiFiClient* stream = http.getStreamPtr();
  g_audioPlayback.clear();
  if (len > 0) {
    g_audioPlayback.resize(size_t(len));
    const size_t readLen = stream->readBytes(g_audioPlayback.data(), size_t(len));
    g_audioPlayback.resize(readLen);
  } else {
    uint8_t buf[512];
    while (http.connected()) {
      const int available = stream->available();
      if (available <= 0) break;
      const int readLen = stream->readBytes(buf, min(available, int(sizeof(buf))));
      g_audioPlayback.insert(g_audioPlayback.end(), buf, buf + readLen);
    }
  }
  http.end();
  Serial.printf(
    "[voice_timing] audio_fetch_ms=%u bytes=%u code=%d\n",
    unsigned(millis() - startedAt),
    unsigned(g_audioPlayback.size()),
    code
  );
  return g_audioPlayback.size() > 44;
}

bool playDownloadedVoiceAudio() {
  if (g_audioPlayback.size() <= 44) {
    return false;
  }

  quietSpeaker("before_playback");
  delay(30);
  if (!M5.Speaker.begin()) {
    setStatus("扬声器启动失败");
    return false;
  }
  M5.Speaker.setVolume(SPEAKER_VOLUME);
  M5.Speaker.setAllChannelVolume(SPEAKER_CHANNEL_VOLUME);
  Serial.printf("[voice] playing wav_bytes=%u volume=%u\n", unsigned(g_audioPlayback.size()), unsigned(SPEAKER_VOLUME));
  if (!M5.Speaker.playWav(g_audioPlayback.data(), g_audioPlayback.size(), 1, 0, true)) {
    quietSpeaker("playback_failed");
    return false;
  }

  const uint32_t startedAt = millis();
  while (M5.Speaker.isPlaying() && (millis() - startedAt) < VOICE_PLAYBACK_MAX_MS) {
    M5.update();
    delay(10);
  }
  quietSpeaker("after_playback");
  Serial.printf("[voice_timing] playback_ms=%u\n", unsigned(millis() - startedAt));
  return true;
}

bool playVoicePrompt(const String& promptKey, const String& fallbackStatus) {
  String path = "/api/device/voice/audio-prompt/" + promptKey;
  if (!fetchVoiceAudio(path)) {
    setStatus(fallbackStatus);
    return false;
  }
  setFaceState(FACE_SPEAKING);
  setStatus("正在播报提示");
  if (!playDownloadedVoiceAudio()) {
    setFaceState(FACE_ERROR);
    setStatus("提示音播放失败");
    return false;
  }
  setStatus(fallbackStatus);
  return true;
}

String jsonStringValue(const JsonDocument& doc) {
  String out;
  serializeJson(doc, out);
  return out;
}

void sendVoiceRealtimeStart() {
  JsonDocument doc;
  doc["type"] = "start";
  doc["contract"] = selectedContract();
  doc["category"] = g_currentCategory;
  doc["conversation_id"] = g_conversationId;
  doc["screen_context"] = g_contractBriefing.ok ? g_contractBriefing.headline : g_briefing.headline;
  String message = jsonStringValue(doc);
  g_voiceWs.sendTXT(message);
  g_voiceRealtimeTurnOpen = true;
  Serial.println("[voice_ws] start_sent");
}

void handleVoiceRealtimeText(const String& payloadText) {
  JsonDocument doc;
  if (deserializeJson(doc, payloadText) != DeserializationError::Ok) {
    Serial.println("[voice_ws] bad_json=" + payloadText);
    return;
  }
  const String type = String(doc["type"] | "");
  if (type == "hello" || type == "capabilities") {
    Serial.println("[voice_ws] ws_connected capabilities=" + payloadText);
    return;
  }
  if (type == "audio_ack") {
    Serial.printf("[voice_ws] audio_ack seq=%ld bytes=%ld\n", long(doc["seq"] | 0), long(doc["bytes"] | 0));
    return;
  }
  if (type == "status") {
    const String state = String(doc["state"] | doc["status"] | "");
    if (state == "listening" || state == "user_speaking" || state == "followup_listening") {
      setFaceState(FACE_LISTENING);
      setStatus(state == "followup_listening" ? "可以继续追问" : "正在聆听");
      if (state == "followup_listening") {
        const uint32_t timeoutSeconds = uint32_t(doc["timeout_seconds"] | (VOICE_FOLLOWUP_WINDOW_MS / 1000UL));
        g_voiceFollowupUntilAt = millis() + timeoutSeconds * 1000UL;
      }
    } else if (state == "transcribing" || state == "thinking") {
      setFaceState(FACE_THINKING);
      setStatus(state == "transcribing" ? "正在识别" : "正在思考");
    } else if (state == "speaking") {
      setFaceState(FACE_SPEAKING);
      setStatus("正在播报");
    }
    Serial.println("[voice_ws] status=" + state);
    return;
  }
  if (type == "final_transcript") {
    g_voiceTranscript = String(doc["text"] | "");
    Serial.println("[voice_ws] asr_final=" + g_voiceTranscript);
    return;
  }
  if (type == "answer_delta") {
    const String text = String(doc["text"] | "");
    if (text.length() > 0) {
      g_voiceAnswer = text;
      g_uiDirty = true;
    }
    Serial.println("[voice_ws] answer_delta len=" + String(text.length()));
    return;
  }
  if (type == "result") {
    g_voiceTranscript = String(doc["transcript"] | g_voiceTranscript.c_str());
    g_voiceAnswer = String(doc["answer_text"] | g_voiceAnswer.c_str());
    const String taskId = String(doc["task_id"] | "");
    if (taskId.length() > 0) {
      g_voiceTaskId = taskId;
      g_voiceTaskActive = true;
      g_voiceTaskStartedAt = millis();
      g_voiceTaskLastPollAt = 0;
    }
    Serial.println("[voice_ws] result transcript=" + g_voiceTranscript + " answer=" + g_voiceAnswer);
    return;
  }
  if (type == "audio_url") {
    const String url = String(doc["url"] | "");
    Serial.println("[voice_ws] audio_url fallback=" + url);
    if (url.length() > 0 && fetchVoiceAudio(url)) {
      setFaceState(FACE_SPEAKING);
      setStatus("正在播报");
      if (playDownloadedVoiceAudio()) {
        setFaceState(FACE_HAPPY);
        setStatus("语音回答已播报");
      } else {
        setFaceState(FACE_ERROR);
        setStatus("语音播放失败");
      }
    }
    return;
  }
  if (type == "tts_audio_delta") {
    if (!g_voiceRealtimeExpectingPcm) {
      Serial.println("[voice_ws] tts_audio_delta ignored");
      return;
    }
    g_voiceRealtimeExpectingPcm = true;
    if (!g_voiceRealtimeSpeakerStarted) {
      quietSpeaker("before_realtime_tts");
      delay(10);
      if (M5.Speaker.begin()) {
        M5.Speaker.setVolume(SPEAKER_VOLUME);
        M5.Speaker.setAllChannelVolume(SPEAKER_CHANNEL_VOLUME);
        g_voiceRealtimeSpeakerStarted = true;
      }
    }
    Serial.printf("[voice_ws] tts_audio_delta seq=%ld total=%ld final=%d\n", long(doc["seq"] | 0), long(doc["total"] | 0), bool(doc["is_final"] | false));
    return;
  }
  if (type == "task") {
    g_voiceTaskId = String(doc["task_id"] | "");
    g_voiceTaskActive = g_voiceTaskId.length() > 0;
    g_voiceTaskStartedAt = millis();
    g_voiceTaskLastPollAt = 0;
    const uint32_t pollSeconds = uint32_t(doc["poll_after_seconds"] | g_config.voiceTaskPollSeconds);
    if (pollSeconds > 0) {
      g_config.voiceTaskPollSeconds = pollSeconds;
    }
    Serial.println("[voice_ws] task=" + g_voiceTaskId);
    return;
  }
  if (type == "done") {
    const uint32_t followupSeconds = uint32_t(doc["followup_window_seconds"] | (VOICE_FOLLOWUP_WINDOW_MS / 1000UL));
    g_voiceFollowupUntilAt = millis() + followupSeconds * 1000UL;
    g_voiceRealtimeTurnOpen = false;
    if (g_voiceRealtimeSpeakerStarted) {
      quietSpeaker("realtime_done");
      g_voiceRealtimeSpeakerStarted = false;
    }
    setFaceState(FACE_LISTENING);
    setStatus("可以继续追问");
    Serial.println("[voice_ws] done followup_seconds=" + String(followupSeconds));
    return;
  }
  if (type == "error") {
    setFaceState(FACE_ERROR);
    setStatus(String(doc["message"] | "实时语音错误"));
    Serial.println("[voice_ws] error=" + payloadText);
  }
}

void onVoiceWsEvent(WStype_t type, uint8_t* payload, size_t length) {
  if (type == WStype_CONNECTED) {
    g_voiceRealtimeConnected = true;
    Serial.println("[voice_ws] ws_connected");
    sendVoiceRealtimeStart();
    return;
  }
  if (type == WStype_DISCONNECTED) {
    g_voiceRealtimeConnected = false;
    g_voiceRealtimeActive = false;
    g_voiceRealtimeTurnOpen = false;
    g_voiceRealtimeExpectingPcm = false;
    if (g_voiceRealtimeSpeakerStarted) {
      quietSpeaker("ws_disconnected");
      g_voiceRealtimeSpeakerStarted = false;
    }
    Serial.println("[voice_ws] ws_disconnected");
    return;
  }
  if (type == WStype_TEXT) {
    handleVoiceRealtimeText(String(reinterpret_cast<const char*>(payload), length));
    return;
  }
  if (type == WStype_BIN) {
    if (!g_voiceRealtimeExpectingPcm || length == 0) {
      Serial.printf("[voice_ws] tts_binary_ignored bytes=%u\n", unsigned(length));
      return;
    }
    if (!g_voiceRealtimeSpeakerStarted && M5.Speaker.begin()) {
      M5.Speaker.setVolume(SPEAKER_VOLUME);
      M5.Speaker.setAllChannelVolume(SPEAKER_CHANNEL_VOLUME);
      g_voiceRealtimeSpeakerStarted = true;
    }
    if (g_voiceRealtimeSpeakerStarted) {
      g_audioPlayback.assign(payload, payload + length);
      M5.Speaker.playRaw(reinterpret_cast<const int16_t*>(g_audioPlayback.data()), g_audioPlayback.size() / sizeof(int16_t), VOICE_SAMPLE_RATE, false, 1, 0, false);
      Serial.printf("[voice_ws] tts_binary bytes=%u\n", unsigned(length));
    }
  }
}

bool parseApiBaseForWs(String& host, uint16_t& port, String& prefixPath, bool& secure) {
  String base(API_BASE_URL);
  base.trim();
  secure = base.startsWith("https://");
  if (base.startsWith("http://")) {
    base.remove(0, 7);
  } else if (base.startsWith("https://")) {
    base.remove(0, 8);
  }
  const int slash = base.indexOf('/');
  if (slash >= 0) {
    prefixPath = base.substring(slash);
    base = base.substring(0, slash);
  } else {
    prefixPath = "";
  }
  const int colon = base.lastIndexOf(':');
  port = secure ? 443 : 80;
  if (colon > 0) {
    host = base.substring(0, colon);
    port = uint16_t(base.substring(colon + 1).toInt());
  } else {
    host = base;
  }
  return host.length() > 0 && port > 0;
}

bool beginVoiceRealtimeSession() {
  if (!g_voiceRealtimeEnabled || !g_wifiReady) {
    return false;
  }
  String host;
  String prefixPath;
  uint16_t port = 0;
  bool secure = false;
  if (!parseApiBaseForWs(host, port, prefixPath, secure)) {
    Serial.println("[voice_ws] bad_api_base");
    return false;
  }
  String path = prefixPath + "/api/device/voice/realtime?token=" + String(API_BEARER_TOKEN);
  String headers = String("X-Device-Id: ") + DEVICE_ID_VALUE + "\r\n" +
                   "X-Device-Model: " + DEVICE_MODEL_VALUE + "\r\n" +
                   "X-Device-Version: " + DEVICE_VERSION_VALUE + "\r\n";
  g_voiceWs.disconnect();
  g_voiceWs.onEvent(onVoiceWsEvent);
  g_voiceWs.setReconnectInterval(0);
  g_voiceWs.setExtraHeaders(headers.c_str());
  g_voiceRealtimeConnected = false;
  g_voiceRealtimeActive = true;
  g_voiceRealtimeTurnOpen = false;
  g_voiceRealtimeSentSamples = 0;
  g_voiceRealtimeLastSendAt = 0;
  g_voiceRealtimeExpectingPcm = false;
  g_voiceRealtimeSpeakerStarted = false;
  if (secure) {
    g_voiceWs.beginSSL(host.c_str(), port, path.c_str());
  } else {
    g_voiceWs.begin(host.c_str(), port, path.c_str());
  }
  const uint32_t startedAt = millis();
  while (!g_voiceRealtimeConnected && (millis() - startedAt) < VOICE_REALTIME_CONNECT_TIMEOUT_MS) {
    g_voiceWs.loop();
    delay(10);
  }
  Serial.printf("[voice_ws] connect host=%s port=%u path=%s ok=%d ms=%u\n", host.c_str(), unsigned(port), path.c_str(), g_voiceRealtimeConnected, unsigned(millis() - startedAt));
  return g_voiceRealtimeConnected;
}

bool sendVoiceRealtimeFrames(size_t availableSamples, bool flushTail) {
  if (!g_voiceRealtimeActive || !g_voiceRealtimeConnected || !g_voiceBuffer) {
    return false;
  }
  availableSamples = min(availableSamples, VOICE_MAX_SAMPLES);
  const size_t frameSamples = (VOICE_SAMPLE_RATE * VOICE_REALTIME_FRAME_MS) / 1000UL;
  bool sent = false;
  while (g_voiceRealtimeSentSamples < availableSamples) {
    size_t remaining = availableSamples - g_voiceRealtimeSentSamples;
    if (!flushTail && remaining < frameSamples) {
      break;
    }
    const size_t sendSamples = min(remaining, frameSamples);
    const uint8_t* data = reinterpret_cast<const uint8_t*>(g_voiceBuffer + g_voiceRealtimeSentSamples);
    if (!g_voiceWs.sendBIN(data, sendSamples * sizeof(int16_t))) {
      Serial.println("[voice_ws] send_bin_failed");
      return sent;
    }
    g_voiceRealtimeSentSamples += sendSamples;
    g_voiceRealtimeLastSendAt = millis();
    sent = true;
    Serial.printf("[voice_ws] pcm_frame samples=%u sent_total=%u\n", unsigned(sendSamples), unsigned(g_voiceRealtimeSentSamples));
  }
  return sent;
}

void finishVoiceRealtimeTurn(size_t samples) {
  if (!g_voiceRealtimeActive || !g_voiceRealtimeConnected) {
    return;
  }
  sendVoiceRealtimeFrames(samples, true);
  JsonDocument doc;
  doc["type"] = "stop";
  String message = jsonStringValue(doc);
  g_voiceWs.sendTXT(message);
  g_voiceRealtimeActive = false;
  g_voiceRealtimeTurnOpen = false;
  Serial.printf("[voice_ws] stop_sent samples=%u\n", unsigned(samples));
}

void handleVoiceRealtimeLoop() {
  if (g_voiceRealtimeActive || g_voiceRealtimeConnected) {
    g_voiceWs.loop();
  }
  if (g_voiceFollowupUntilAt > 0 && millis() > g_voiceFollowupUntilAt && !g_voiceRecording) {
    g_voiceFollowupUntilAt = 0;
    setFaceState(FACE_IDLE);
    setStatus("待机");
  }
}

bool postVoiceQuery(size_t sampleCount) {
  if (!g_wifiReady || !g_voiceBuffer || sampleCount == 0) {
    setStatus("无录音可发送");
    setFaceState(FACE_ERROR);
    return false;
  }

  const uint32_t queryStartedAt = millis();
  setFaceState(FACE_THINKING);
  setStatus("正在思考...");

  const uint32_t buildStartedAt = millis();
  const size_t pcmBytes = min(sampleCount, VOICE_MAX_SAMPLES) * sizeof(int16_t);
  int32_t localPeak = 0;
  float localRms = 0.0f;
  computePcmStats(g_voiceBuffer, sampleCount, localPeak, localRms);
  Serial.printf(
    "[voice] local samples=%u duration_ms=%u peak=%ld rms=%.1f wav_bytes=%u\n",
    unsigned(sampleCount),
    unsigned((uint64_t(sampleCount) * 1000ULL) / VOICE_SAMPLE_RATE),
    long(localPeak),
    double(localRms),
    unsigned(44 + pcmBytes)
  );

  std::vector<uint8_t> wav;
  wav.reserve(44 + pcmBytes);
  appendWavHeader(wav, pcmBytes);
  appendBytes(wav, reinterpret_cast<const uint8_t*>(g_voiceBuffer), pcmBytes);

  const String boundary = "----TradingArtStackChanBoundary";
  std::vector<uint8_t> body;
  body.reserve(wav.size() + 512);
  auto addField = [&body, &boundary](const String& name, const String& value) {
    appendString(body, "--" + boundary + "\r\n");
    appendString(body, "Content-Disposition: form-data; name=\"" + name + "\"\r\n\r\n");
    appendString(body, value + "\r\n");
  };
  addField("contract", selectedContract());
  addField("category", g_currentCategory);
  addField("screen_context", g_contractBriefing.ok ? g_contractBriefing.headline : g_briefing.headline);
  addField("conversation_id", g_conversationId);
  addField("client_audio_peak", String(localPeak));
  addField("client_audio_rms", String(localRms, 1));
  appendString(body, "--" + boundary + "\r\n");
  appendString(body, "Content-Disposition: form-data; name=\"audio\"; filename=\"stackchan.wav\"\r\n");
  appendString(body, "Content-Type: audio/wav\r\n\r\n");
  appendBytes(body, wav.data(), wav.size());
  appendString(body, "\r\n--" + boundary + "--\r\n");
  Serial.printf(
    "[voice_timing] query_build_ms=%u body_bytes=%u wav_bytes=%u\n",
    unsigned(millis() - buildStartedAt),
    unsigned(body.size()),
    unsigned(wav.size())
  );

  HTTPClient http;
  if (!beginHttp(http, buildUrl("/api/device/voice/query"), VOICE_QUERY_TIMEOUT_MS)) {
    setFaceState(FACE_ERROR);
    return false;
  }
  http.addHeader("Content-Type", "multipart/form-data; boundary=" + boundary);
  const uint32_t httpStartedAt = millis();
  const int code = http.POST(body.data(), body.size());
  const String responseBody = http.getString();
  http.end();
  const uint32_t httpMs = millis() - httpStartedAt;

  if (code != 200) {
    setStatus("语音问答失败: " + String(code));
    setFaceState(FACE_ERROR);
    if (code == -11 || code == -4 || code == -1) {
      playVoicePrompt(code == -4 ? "voice_network_error" : "voice_timeout", "请再问一次");
      setFaceState(FACE_ERROR);
    }
    return false;
  }

  const uint32_t parseStartedAt = millis();
  JsonDocument doc;
  if (deserializeJson(doc, responseBody) != DeserializationError::Ok) {
    setStatus("语音结果解析失败");
    setFaceState(FACE_ERROR);
    return false;
  }
  const uint32_t parseMs = millis() - parseStartedAt;

  g_conversationId = String(doc["conversation_id"] | g_conversationId.c_str());
  g_voiceTranscript = String(doc["transcript"] | "");
  g_voiceAnswer = String(doc["answer_text"] | "");
  const String routeType = String(doc["route_type"] | "");
  const String action = String(doc["action"] | "");
  const String taskId = String(doc["task_id"] | "");
  const String emotion = String(doc["emotion"] | "speaking");
  const String audioUrl = String(doc["audio_url"] | "");
  const String sttStatus = String(doc["stt_status"] | "");
  const String sttError = String(doc["stt_error"] | "");
  const bool stopListening = action == "stop_listening";
  const int serverPeak = int(doc["audio_peak"] | 0);
  const float serverRms = float(doc["audio_rms"] | 0.0);

  Serial.println("[voice] Q: " + g_voiceTranscript);
  Serial.println("[voice] A: " + g_voiceAnswer);
  Serial.printf("[voice] STT=%s server_peak=%d server_rms=%.1f\n", sttStatus.c_str(), serverPeak, double(serverRms));
  Serial.printf(
    "[voice_timing] query_http_ms=%u parse_ms=%u total_to_json_ms=%u code=%d\n",
    unsigned(httpMs),
    unsigned(parseMs),
    unsigned(millis() - queryStartedAt),
    code
  );
  JsonObject timings = doc["timings_ms"].as<JsonObject>();
  if (!timings.isNull()) {
    Serial.printf(
      "[voice_timing] server upload_read=%ld audio_parse=%ld stt=%ld route=%ld market=%ld llm=%ld answer=%ld tts=%ld total=%ld\n",
      long(timings["upload_read_ms"] | -1),
      long(timings["audio_parse_ms"] | -1),
      long(timings["stt_ms"] | -1),
      long(timings["route_ms"] | -1),
      long(timings["market_context_ms"] | -1),
      long(timings["llm_ms"] | -1),
      long(timings["answer_ms"] | -1),
      long(timings["tts_ms"] | -1),
      long(timings["server_total_ms"] | -1)
    );
  }
  if (sttError.length() > 0) {
    Serial.println("[voice] STT error: " + sttError);
  }

  if (action == "thinking" && taskId.length() > 0) {
    g_voiceTaskId = taskId;
    g_voiceTaskActive = true;
    g_voiceTaskStartedAt = millis();
    g_voiceTaskLastPollAt = 0;
    const uint32_t pollSeconds = uint32_t(doc["poll_after_seconds"] | g_config.voiceTaskPollSeconds);
    if (pollSeconds > 0) {
      g_config.voiceTaskPollSeconds = pollSeconds;
    }
    const uint32_t maxWaitSeconds = uint32_t(doc["task_max_wait_seconds"] | g_config.voiceTaskMaxWaitSeconds);
    if (maxWaitSeconds >= g_config.voiceTaskPollSeconds) {
      g_config.voiceTaskMaxWaitSeconds = maxWaitSeconds;
    }
    if (audioUrl.length() > 0 && fetchVoiceAudio(audioUrl)) {
      setFaceState(FACE_SPEAKING);
      setStatus("正在确认");
      playDownloadedVoiceAudio();
    }
    setFaceState(FACE_THINKING);
    setStatus("深度分析中...");
    g_uiDirty = true;
    Serial.println("[voice] deep task started: " + g_voiceTaskId + " route=" + routeType);
    startVoiceFollowupListening();
    return true;
  }

  if (audioUrl.length() > 0 && fetchVoiceAudio(audioUrl)) {
    setFaceState(FACE_SPEAKING);
    setStatus("正在播报");
    if (!playDownloadedVoiceAudio()) {
      setFaceState(FACE_ERROR);
      setStatus("语音播放失败");
    } else {
      setFaceState(FACE_HAPPY);
      setStatus("语音回答已播报");
    }
  } else {
    setFaceState(emotion == "error" ? FACE_ERROR : FACE_HAPPY);
    setStatus("语音回答已显示");
  }
  g_uiDirty = true;
  if (stopListening) {
    g_voiceFollowupUntilAt = 0;
    g_voiceConversationUntilAt = 0;
    setFaceState(FACE_IDLE);
    setStatus("语音已结束");
    Serial.println("[voice] stop_listening_command");
    return true;
  }
  startVoiceFollowupListening();
  return true;
}

void finishVoiceRecording(const char* stopReason = "manual") {
  if (!g_voiceRecording) {
    return;
  }
  const uint32_t elapsedMs = millis() - g_voiceStartedAt;
  M5.Mic.end();
  g_voiceRecording = false;
  const bool wasFollowup = g_voiceFollowupRecording;
  g_voiceFollowupRecording = false;
  const uint32_t cueStartedAt = millis();
  setFaceState(FACE_THINKING);
  Serial.printf(
    "[voice_timing] record_elapsed_ms=%u stop_cue_ms=%u reason=%s\n",
    unsigned(elapsedMs),
    unsigned(millis() - cueStartedAt),
    stopReason ? stopReason : "unknown"
  );
  const uint32_t maxRecordMs = uint32_t(g_config.recordMaxSeconds * 1000UL);
  const uint32_t clippedMs = elapsedMs < maxRecordMs ? elapsedMs : maxRecordMs;
  size_t samples = size_t((uint64_t(clippedMs) * VOICE_SAMPLE_RATE) / 1000ULL);
  samples = min(samples, VOICE_MAX_SAMPLES);
  int32_t localPeak = 0;
  float localRms = 0.0f;
  if (samples > 0) {
    computePcmStats(g_voiceBuffer, samples, localPeak, localRms);
  }
  const bool confirmedSpeech =
    g_voiceDetectedSpeech &&
    g_voiceSpeechChunkCount >= VOICE_MIN_SPEECH_CHUNKS &&
    (uint32_t(localPeak) >= VOICE_UPLOAD_MIN_PEAK || localRms >= VOICE_UPLOAD_MIN_RMS);
  if (!confirmedSpeech) {
    quietSpeaker(wasFollowup ? "followup_no_speech" : "no_speech");
    Serial.printf(
      "[voice] no_speech_local_skip followup=%d chunks=%u peak=%ld rms=%.1f\n",
      wasFollowup ? 1 : 0,
      unsigned(g_voiceSpeechChunkCount),
      long(localPeak),
      double(localRms)
    );
    if (wasFollowup && g_voiceConversationUntilAt > 0 && int32_t(millis() - g_voiceConversationUntilAt) < 0) {
      setFaceState(FACE_LISTENING);
      setStatus("继续聆听");
      startVoiceFollowupListening(false);
      return;
    }
    g_voiceConversationUntilAt = 0;
    setFaceState(wasFollowup ? FACE_IDLE : FACE_LISTENING);
    setStatus(wasFollowup ? "待机" : "没听到声音");
    return;
  }
  if (samples < 1600) {
    setStatus(wasFollowup ? "待机" : "录音太短");
    setFaceState(wasFollowup ? FACE_IDLE : FACE_ERROR);
    return;
  }
  setStatus("收到，正在发送...");
  playLocalVoiceCue(VOICE_CUE_SEND_HZ);
  if (g_voiceRealtimeActive && g_voiceRealtimeConnected) {
    finishVoiceRealtimeTurn(samples);
    return;
  }
  postVoiceQuery(samples);
}

void handleVoiceRecording() {
  if (!g_voiceRecording) {
    return;
  }
  const uint32_t seconds = g_config.recordMaxSeconds == 0 ? 1 : g_config.recordMaxSeconds;
  const uint32_t maxMs = seconds * 1000UL;
  const uint32_t nowMs = millis();
  const uint32_t elapsedMs = nowMs - g_voiceStartedAt;
  if (g_voiceFollowupRecording && !g_voiceDetectedSpeech && elapsedMs >= VOICE_FOLLOWUP_WINDOW_MS) {
    Serial.printf("[voice_timing] followup_timeout elapsed_ms=%u\n", unsigned(elapsedMs));
    finishVoiceRecording("followup_timeout");
    return;
  }
  if (g_voiceRealtimeActive && g_voiceRealtimeConnected) {
    size_t availableSamples = size_t((uint64_t(elapsedMs) * VOICE_SAMPLE_RATE) / 1000ULL);
    sendVoiceRealtimeFrames(availableSamples, false);
  }
  if (elapsedMs >= maxMs) {
    Serial.printf("[voice_timing] record_max_stop elapsed_ms=%u max_ms=%u\n", unsigned(elapsedMs), unsigned(maxMs));
    finishVoiceRecording("max_duration");
    return;
  }
  if (elapsedMs > 300 && M5.Mic.isRecording() == 0) {
    Serial.printf("[voice_timing] mic_auto_stop elapsed_ms=%u\n", unsigned(elapsedMs));
    finishVoiceRecording("mic_stopped");
    return;
  }
  if (updateVoiceSilenceDetector(nowMs, elapsedMs)) {
    finishVoiceRecording("silence_stop");
  }
}

void pollVoiceTask() {
  if (!g_voiceTaskActive || g_voiceTaskId.length() == 0 || g_voiceRecording) {
    return;
  }
  const uint32_t nowMs = millis();
  const uint32_t pollMs = g_config.voiceTaskPollSeconds * 1000UL;
  if (g_voiceTaskLastPollAt > 0 && (nowMs - g_voiceTaskLastPollAt) < pollMs) {
    return;
  }
  g_voiceTaskLastPollAt = nowMs;

  HTTPClient http;
  String path = "/api/device/voice/task/" + g_voiceTaskId;
  if (!beginHttp(http, buildUrl(path.c_str()), HTTP_TIMEOUT_MS)) {
    setStatus("任务状态连接失败");
    return;
  }

  const uint32_t taskHttpStartedAt = millis();
  const int code = http.GET();
  const String body = http.getString();
  http.end();
  Serial.printf("[voice_timing] task_poll_http_ms=%u code=%d\n", unsigned(millis() - taskHttpStartedAt), code);
  if (code != 200) {
    setStatus("任务状态失败: " + String(code));
    return;
  }

  JsonDocument doc;
  if (deserializeJson(doc, body) != DeserializationError::Ok) {
    setStatus("任务状态解析失败");
    return;
  }

  const String status = String(doc["status"] | "");
  const String action = String(doc["action"] | "");
  const String emotion = String(doc["emotion"] | "thinking");
  const String audioUrl = String(doc["audio_url"] | "");
  const String answerText = String(doc["answer_text"] | "");
  const String speakText = String(doc["speak_text"] | "");
  JsonObject timings = doc["timings_ms"].as<JsonObject>();
  if (!timings.isNull()) {
    Serial.printf(
      "[voice_timing] task_server status_read=%ld tts=%ld total=%ld\n",
      long(timings["status_read_ms"] | -1),
      long(timings["tts_ms"] | -1),
      long(timings["server_total_ms"] | -1)
    );
  }
  const uint32_t nextPollSeconds = uint32_t(doc["poll_after_seconds"] | g_config.voiceTaskPollSeconds);
  if (nextPollSeconds > 0) {
    g_config.voiceTaskPollSeconds = nextPollSeconds;
  }
  const uint32_t maxWaitSeconds = uint32_t(doc["task_max_wait_seconds"] | g_config.voiceTaskMaxWaitSeconds);
  if (maxWaitSeconds >= 300) {
    g_config.voiceTaskMaxWaitSeconds = maxWaitSeconds;
  }

  if (action == "thinking" || status == "queued" || status == "pending" || status == "processing") {
    if (answerText.length() > 0) {
      g_voiceAnswer = answerText;
    }
    setFaceState(FACE_THINKING);
    setStatus(answerText.length() > 0 ? answerText : "分析团队还在看");
    g_uiDirty = true;
    return;
  }

  g_voiceTaskActive = false;
  g_voiceTaskId = "";
  g_voiceAnswer = speakText.length() > 0 ? speakText : answerText;
  if (audioUrl.length() > 0 && fetchVoiceAudio(audioUrl)) {
    setFaceState(FACE_SPEAKING);
    setStatus("正在播报");
    if (playDownloadedVoiceAudio()) {
      setFaceState(status == "success" ? FACE_HAPPY : FACE_ERROR);
      setStatus(status == "success" ? "分析摘要已播报" : "错误提示已播报");
    } else {
      setFaceState(FACE_ERROR);
      setStatus("语音播放失败");
    }
  } else {
    setFaceState(emotion == "error" ? FACE_ERROR : FACE_HAPPY);
    setStatus(status == "success" ? "分析摘要已显示" : "分析状态已显示");
  }
  g_uiDirty = true;
  startVoiceFollowupListening();
}

bool fetchBriefing(bool announce) {
  if (!g_wifiReady) {
    setStatus("无Wi-Fi");
    return false;
  }

  HTTPClient http;
  if (!beginHttp(http, buildUrl("/api/device/briefing"))) {
    return false;
  }

  const int code = http.GET();
  const String body = http.getString();
  http.end();

  if (code != 200) {
    setStatus("简报失败: " + String(code));
    return false;
  }

  JsonDocument doc;
  if (deserializeJson(doc, body) != DeserializationError::Ok) {
    setStatus("简报解析失败");
    return false;
  }

  g_briefing.ok = true;
  g_briefing.marketState = String(doc["market_state"] | "neutral");
  g_briefing.riskLevel = String(doc["risk_level"] | "medium");
  g_briefing.headline = String(doc["headline"] | "No headline");
  g_briefing.speakText = String(doc["speak_text"] | "");
  g_briefing.latestAlert = String(doc["latest_alert"] | "");
  g_briefing.updatedAt = String(doc["updated_at"] | "");
  g_briefing.dataFreshness = String(doc["data_freshness"] | "degraded");
  g_briefing.ivTemperature = doc["iv_temperature"].is<int>() ? int(doc["iv_temperature"]) : -1;
  g_briefing.chaosIndex = doc["chaos_index"].is<int>() ? int(doc["chaos_index"]) : -1;

  g_lastPollAt = millis();
  g_uiDirty = true;
  setStatus("简报已更新");
  if (announce) {
    announceBriefing();
  }
  return true;
}

bool fetchProductsMenu(const String& category = "futures") {
  if (!g_wifiReady) {
    setStatus("无Wi-Fi");
    return false;
  }

  HTTPClient http;
  String query = "category=" + category + "&max_products=60&max_contracts=1";
  if (!beginHttp(http, buildUrl("/api/device/contracts/menu", query))) {
    return false;
  }

  const int code = http.GET();
  const String body = http.getString();
  http.end();

  if (code != 200) {
    setStatus("菜单失败: " + String(code));
    return false;
  }

  JsonDocument doc;
  if (deserializeJson(doc, body) != DeserializationError::Ok) {
    setStatus("菜单解析失败");
    return false;
  }

  g_productCount = 0;
  g_contractCount = 0;
  g_selectedProductIndex = -1;
  g_selectedContractIndex = -1;
  g_currentCategory = String(doc["category"] | category);

  JsonArray products = doc["products"].as<JsonArray>();
  for (JsonObject product : products) {
    if (g_productCount >= MAX_PRODUCTS) break;
    ProductItem& p = g_products[g_productCount];
    p.code = String(product["product_code"] | "");
    p.name = String(product["product_name"] | "");
    p.contractStart = 0;
    p.contractCount = 0;
    if (p.code.length() > 0) {
      ++g_productCount;
    }
  }

  if (g_productCount > 0) {
    g_selectedProductIndex = 0;
  }

  g_uiDirty = true;
  setStatus(g_productCount > 0 ? "品种数: " + String(g_productCount) : "菜单为空");
  return g_productCount > 0;
}

bool fetchProductContracts(uint8_t productIndex) {
  if (!g_wifiReady) {
    setStatus("无Wi-Fi");
    return false;
  }
  if (productIndex >= g_productCount) {
    setStatus("未选择品种");
    return false;
  }

  ProductItem& selectedProduct = g_products[productIndex];
  HTTPClient http;
  String query = "category=" + g_currentCategory + "&product=" + selectedProduct.code + "&max_products=1&max_contracts=8";
  if (!beginHttp(http, buildUrl("/api/device/contracts/menu", query))) {
    return false;
  }

  const int code = http.GET();
  const String body = http.getString();
  http.end();

  if (code != 200) {
    setStatus("合约失败: " + String(code));
    return false;
  }

  JsonDocument doc;
  if (deserializeJson(doc, body) != DeserializationError::Ok) {
    setStatus("合约解析失败");
    return false;
  }

  JsonArray products = doc["products"].as<JsonArray>();
  if (products.size() == 0) {
    setStatus("无合约");
    return false;
  }

  g_contractCount = 0;
  selectedProduct.contractStart = 0;
  selectedProduct.contractCount = 0;

  JsonArray contracts = products[0]["contracts"].as<JsonArray>();
  for (JsonObject contract : contracts) {
    if (g_contractCount >= MAX_CONTRACTS) break;
    ContractItem& c = g_contracts[g_contractCount];
    c.category = g_currentCategory;
    c.contract = String(contract["contract"] | "");
    c.label = String(contract["label"] | "");
    c.productCode = selectedProduct.code;
    c.latestPrice = contract["latest_price"].is<float>() ? float(contract["latest_price"]) : NAN;
    c.pricePct = contract["price_pct"].is<float>() ? float(contract["price_pct"]) : NAN;
    c.iv = contract["iv"].is<float>() ? float(contract["iv"]) : NAN;
    c.ivRank = contract["iv_rank"].is<float>() ? float(contract["iv_rank"]) : NAN;
    if (c.contract.length() > 0) {
      ++g_contractCount;
      ++selectedProduct.contractCount;
    }
  }

  g_selectedProductIndex = int8_t(productIndex);
  g_selectedContractIndex = selectedProduct.contractCount > 0 ? 0 : -1;
  g_uiDirty = true;
  setStatus(selectedProduct.contractCount > 0 ? "合约已加载" : "无合约");
  return selectedProduct.contractCount > 0;
}

bool fetchContractBriefing(const String& contract, bool announce) {
  if (!g_wifiReady) {
    setStatus("无Wi-Fi");
    return false;
  }
  if (contract.length() == 0) {
    setStatus("未选择合约");
    return false;
  }

  HTTPClient http;
  String query = "category=" + g_currentCategory + "&contract=" + contract;
  if (!beginHttp(http, buildUrl("/api/device/contracts/briefing", query))) {
    return false;
  }

  const int code = http.GET();
  const String body = http.getString();
  http.end();

  if (code != 200) {
    setStatus("看板失败: " + String(code));
    return false;
  }

  JsonDocument doc;
  if (deserializeJson(doc, body) != DeserializationError::Ok) {
    setStatus("看板解析失败");
    return false;
  }

  g_contractBriefing.ok = true;
  g_contractBriefing.contract = String(doc["contract"] | "");
  g_contractBriefing.productCode = String(doc["product_code"] | "");
  g_contractBriefing.productName = String(doc["product_name"] | "");
  g_contractBriefing.latestPrice = doc["latest_price"].is<float>() ? float(doc["latest_price"]) : NAN;
  g_contractBriefing.pricePct = doc["price_pct"].is<float>() ? float(doc["price_pct"]) : NAN;
  g_contractBriefing.iv = doc["iv"].is<float>() ? float(doc["iv"]) : NAN;
  g_contractBriefing.ivRank = doc["iv_rank"].is<float>() ? float(doc["iv_rank"]) : NAN;
  g_contractBriefing.technicalLabel = String(doc["technical_label"] | "待生成");
  g_contractBriefing.headline = String(doc["headline"] | "");
  g_contractBriefing.speakText = String(doc["speak_text"] | "");
  g_contractBriefing.updatedAt = String(doc["updated_at"] | "");
  g_contractBriefing.dataFreshness = String(doc["data_freshness"] | "degraded");

  g_lastPollAt = millis();
  g_viewMode = VIEW_DETAIL;
  g_uiDirty = true;
  setStatus("看板已更新");
  if (announce) {
    announceBriefing();
  }
  return true;
}

String selectedContract() {
  if (g_selectedContractIndex < 0 || g_selectedContractIndex >= g_contractCount) {
    return "";
  }
  return g_contracts[g_selectedContractIndex].contract;
}

void showCategoryMenu() {
  g_viewMode = VIEW_CATEGORY_MENU;
  g_menuPage = 0;
  g_uiDirty = true;
  setStatus("选择分类");
}

uint8_t visibleRowFromY(int16_t y) {
  if (y < 44 || y > 202) {
    return 255;
  }
  return uint8_t((y - 44) / 30);
}

void showProductMenu() {
  if (g_productCount == 0) {
    fetchProductsMenu(g_currentCategory);
  }
  g_viewMode = VIEW_PRODUCT_MENU;
  g_menuPage = 0;
  g_uiDirty = true;
  setStatus("选择品种");
}

void showContractMenu(uint8_t productIndex) {
  if (productIndex >= g_productCount) return;
  fetchProductContracts(productIndex);
  g_selectedProductIndex = int8_t(productIndex);
  g_viewMode = VIEW_CONTRACT_MENU;
  g_menuPage = 0;
  g_uiDirty = true;
  setStatus("选择合约");
}

void handleMenuTap(int16_t x, int16_t y) {
  (void)x;
  if (y >= 206) {
    ++g_menuPage;
    if (g_viewMode == VIEW_PRODUCT_MENU) {
      const uint8_t maxPage = g_productCount == 0 ? 0 : (g_productCount - 1) / MENU_PAGE_SIZE;
      if (g_menuPage > maxPage) g_menuPage = 0;
    } else if (g_viewMode == VIEW_CONTRACT_MENU && g_selectedProductIndex >= 0) {
      ProductItem& p = g_products[g_selectedProductIndex];
      const uint8_t maxPage = p.contractCount == 0 ? 0 : (p.contractCount - 1) / MENU_PAGE_SIZE;
      if (g_menuPage > maxPage) g_menuPage = 0;
    }
    g_uiDirty = true;
    return;
  }

  const uint8_t row = visibleRowFromY(y);
  if (row == 255 || row >= MENU_PAGE_SIZE) return;

  if (g_viewMode == VIEW_CATEGORY_MENU) {
    if (row == 0) {
      if (fetchProductsMenu("futures")) showProductMenu();
    } else if (row == 1) {
      if (fetchProductsMenu("etf")) showProductMenu();
    } else if (row == 2) {
      if (fetchProductsMenu("favorites")) showProductMenu();
    }
    return;
  }

  if (g_viewMode == VIEW_PRODUCT_MENU) {
    const uint8_t idx = g_menuPage * MENU_PAGE_SIZE + row;
    if (idx < g_productCount) {
      showContractMenu(idx);
    }
    return;
  }

  if (g_viewMode == VIEW_CONTRACT_MENU && g_selectedProductIndex >= 0) {
    ProductItem& p = g_products[g_selectedProductIndex];
    const uint8_t localIdx = g_menuPage * MENU_PAGE_SIZE + row;
    if (localIdx < p.contractCount) {
      g_selectedContractIndex = p.contractStart + localIdx;
      setStatus("读取 " + selectedContract());
      fetchContractBriefing(selectedContract(), true);
    }
  }
}

void handleTouchInput() {
  const bool touched = M5.Touch.getCount() > 0;
  if (touched && !g_touchActive) {
    g_touchActive = true;
    g_touchLongHandled = false;
    g_touchStartedAt = millis();
    auto touch = M5.Touch.getDetail(0);
    g_touchX = touch.x;
    g_touchY = touch.y;
  }

  if (touched && g_touchActive && !g_touchLongHandled) {
    auto touch = M5.Touch.getDetail(0);
    g_touchX = touch.x;
    g_touchY = touch.y;
    if ((millis() - g_touchStartedAt) >= TOUCH_LONG_PRESS_MS) {
      g_touchLongHandled = true;
      if (g_viewMode == VIEW_DETAIL) {
        showCategoryMenu();
      } else if (g_viewMode == VIEW_CONTRACT_MENU) {
        showProductMenu();
      } else if (g_viewMode == VIEW_PRODUCT_MENU) {
        showCategoryMenu();
      } else {
        g_viewMode = VIEW_DETAIL;
        g_uiDirty = true;
        setStatus("返回看板");
      }
    }
  }

  if (!touched && g_touchActive) {
    const uint32_t pressedFor = millis() - g_touchStartedAt;
    if (!g_touchLongHandled && pressedFor < TOUCH_LONG_PRESS_MS) {
      if (g_viewMode == VIEW_CATEGORY_MENU || g_viewMode == VIEW_PRODUCT_MENU || g_viewMode == VIEW_CONTRACT_MENU) {
        handleMenuTap(g_touchX, g_touchY);
      } else if (g_voiceRecording) {
        finishVoiceRecording("manual_tap");
      } else if (g_touchY <= DETAIL_MENU_TAP_Y) {
        showCategoryMenu();
      } else if (g_touchY >= DETAIL_VOICE_TAP_Y) {
        beginVoiceRecording();
      } else if (g_contractBriefing.ok || selectedContract().length() > 0) {
        setStatus("刷新 " + selectedContract());
        fetchContractBriefing(selectedContract(), true);
      } else {
        setStatus("请求简报...");
        fetchBriefing(true);
      }
    }
    g_touchActive = false;
    g_touchLongHandled = false;
    g_touchStartedAt = 0;
  }
}

void handleSerialInput() {
  while (Serial.available() > 0) {
    const char ch = char(Serial.read());
    if (ch == 'f' || ch == 'F') {
      setStatus("串口刷新看板");
      fetchContractBriefing(selectedContract(), true);
    } else if (ch == 'c' || ch == 'C') {
      setStatus("串口刷新配置");
      fetchConfig();
    } else if (ch == 'm' || ch == 'M') {
      setStatus("串口打开菜单");
      showCategoryMenu();
    }
  }
}

void maybeAutoPoll() {
  if (!g_config.autoPollEnabled || g_config.autoPollSeconds == 0) {
    return;
  }
  const uint32_t intervalMs = g_config.autoPollSeconds * 1000UL;
  if ((millis() - g_lastPollAt) >= intervalMs) {
    setStatus("自动刷新...");
    if (selectedContract().length() > 0) {
      fetchContractBriefing(selectedContract(), false);
    } else {
      fetchBriefing(false);
    }
  }
}

}  // namespace app

void setup() {
  auto cfg = M5.config();
  auto spkCfg = M5.Speaker.config();
  spkCfg.dma_buf_len = 1024;
  M5.Speaker.config(spkCfg);
  M5.begin(cfg);
  M5.Speaker.setVolume(app::SPEAKER_VOLUME);
  M5.Speaker.setAllChannelVolume(app::SPEAKER_CHANNEL_VOLUME);
  Serial.begin(115200);

  M5.Display.setRotation(1);
  M5.Display.setTextSize(2);
  M5.Display.fillScreen(TFT_BLACK);

  app::drawUi();

  if (!app::hasSecretsConfigured()) {
    app::setStatus("请配置Wi-Fi/API");
    app::drawUi();
    return;
  }

  app::connectWifi();
  app::drawUi();
  if (app::g_wifiReady) {
    app::fetchPing();
    app::fetchConfig();
    if (app::fetchProductsMenu() && app::fetchProductContracts(0)) {
      app::fetchContractBriefing(app::selectedContract(), false);
    }
  }
  app::drawUi();
}

void loop() {
  M5.update();
  app::handleTouchInput();
  app::handleSerialInput();
  app::handleVoiceRecording();
  app::handleVoiceRealtimeLoop();
  app::pollVoiceTask();
  app::maybeAutoPoll();
  app::drawUi();
  delay(20);
}
