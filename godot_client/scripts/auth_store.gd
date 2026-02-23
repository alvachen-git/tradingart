extends RefCounted
class_name AuthStore

const AUTH_FILE := "user://auth.cfg"
const DEFAULT_BASE_URL := "http://127.0.0.1:8787"


static func load_session() -> Dictionary:
	var cfg := ConfigFile.new()
	var err := cfg.load(AUTH_FILE)
	if err != OK:
		return {}
	return {
		"base_url": str(cfg.get_value("auth", "base_url", DEFAULT_BASE_URL)),
		"username": str(cfg.get_value("auth", "username", "")),
		"token": str(cfg.get_value("auth", "token", "")),
	}


static func save_session(base_url: String, username: String, token: String) -> void:
	var cfg := ConfigFile.new()
	cfg.set_value("auth", "base_url", base_url.strip_edges())
	cfg.set_value("auth", "username", username.strip_edges())
	cfg.set_value("auth", "token", token.strip_edges())
	cfg.save(AUTH_FILE)


static func clear_session() -> void:
	var cfg := ConfigFile.new()
	cfg.clear()
	cfg.save(AUTH_FILE)
