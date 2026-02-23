extends Node
class_name ApiClient

const CLIENT_BUILD := "2026-02-21-r37"

var base_url: String = "http://127.0.0.1:8787"
var username: String = ""
var token: String = ""
var _request_inflight: bool = false


func client_build() -> String:
	return CLIENT_BUILD


func set_auth(user: String, session_token: String) -> void:
	username = user.strip_edges()
	token = session_token.strip_edges()


func _build_headers(with_json: bool = true) -> PackedStringArray:
	var headers := PackedStringArray()
	if with_json:
		headers.append("Content-Type: application/json")
	if not username.is_empty():
		headers.append("X-Username: %s" % username)
	if not token.is_empty():
		headers.append("X-Token: %s" % token)
	return headers


func _parse_body(raw: PackedByteArray) -> Dictionary:
	var text := raw.get_string_from_utf8()
	if text.strip_edges().is_empty():
		return {}
	var parsed: Variant = JSON.parse_string(text)
	if typeof(parsed) == TYPE_DICTIONARY:
		return parsed
	return {"ok": false, "message": "invalid json body", "raw_text": text}


func _acquire_request_slot() -> void:
	while _request_inflight:
		await get_tree().process_frame
	_request_inflight = true


func _release_request_slot() -> void:
	_request_inflight = false


func _timeout_for_path(path: String) -> float:
	if path.begins_with("/v1/card/stage/start"):
		return 90.0
	if path.begins_with("/v1/card/run/state"):
		return 35.0
	return 25.0


func _result_code_text(result_code: int) -> String:
	match result_code:
		HTTPRequest.RESULT_SUCCESS:
			return "success"
		HTTPRequest.RESULT_CHUNKED_BODY_SIZE_MISMATCH:
			return "chunked_body_size_mismatch"
		HTTPRequest.RESULT_CANT_CONNECT:
			return "cant_connect"
		HTTPRequest.RESULT_CANT_RESOLVE:
			return "cant_resolve"
		HTTPRequest.RESULT_CONNECTION_ERROR:
			return "connection_error"
		HTTPRequest.RESULT_TLS_HANDSHAKE_ERROR:
			return "tls_handshake_error"
		HTTPRequest.RESULT_NO_RESPONSE:
			return "no_response"
		HTTPRequest.RESULT_BODY_SIZE_LIMIT_EXCEEDED:
			return "body_size_limit_exceeded"
		HTTPRequest.RESULT_BODY_DECOMPRESS_FAILED:
			return "body_decompress_failed"
		HTTPRequest.RESULT_REQUEST_FAILED:
			return "request_failed"
		HTTPRequest.RESULT_DOWNLOAD_FILE_CANT_OPEN:
			return "download_file_cant_open"
		HTTPRequest.RESULT_DOWNLOAD_FILE_WRITE_ERROR:
			return "download_file_write_error"
		HTTPRequest.RESULT_REDIRECT_LIMIT_REACHED:
			return "redirect_limit_reached"
		HTTPRequest.RESULT_TIMEOUT:
			return "timeout"
		_:
			return "unknown"


func _request_json(path: String, method: int, with_json_header: bool, body_text: String = "", attempt: int = 0) -> Dictionary:
	await _acquire_request_slot()
	var http := HTTPRequest.new()
	http.timeout = _timeout_for_path(path)
	add_child(http)
	var err := http.request("%s%s" % [base_url, path], _build_headers(with_json_header), method, body_text)
	if err != OK:
		http.queue_free()
		_release_request_slot()
		return {"ok": false, "message": "http request failed: %s" % err, "err_code": err}
	var result: Array = await http.request_completed
	http.queue_free()
	_release_request_slot()
	var request_result: int = int(result[0])
	var status_code: int = int(result[1])
	if request_result == HTTPRequest.RESULT_TIMEOUT and attempt < 1:
		await get_tree().create_timer(0.35).timeout
		return await _request_json(path, method, with_json_header, body_text, attempt + 1)
	if request_result != HTTPRequest.RESULT_SUCCESS:
		var result_text: String = _result_code_text(request_result)
		return {
			"ok": false,
			"message": "network request failed: %s(%s)" % [request_result, result_text],
			"result_code": request_result,
			"result_text": result_text,
			"status_code": status_code,
		}
	var payload: Dictionary = _parse_body(result[3])
	if payload.is_empty():
		return {
			"ok": false,
			"message": "empty response body",
			"status_code": status_code,
		}
	if status_code >= 400:
		return {
			"ok": false,
			"status_code": status_code,
			"message": str(payload.get("detail", "http error")),
			"detail": payload,
		}
	return payload


func get_json(path: String) -> Dictionary:
	return await _request_json(path, HTTPClient.METHOD_GET, false, "")


func post_json(path: String, body: Dictionary) -> Dictionary:
	return await _request_json(path, HTTPClient.METHOD_POST, true, JSON.stringify(body))
