extends Control

signal login_success(base_url: String, username: String, token: String, entry_mode: String)

const API_CLIENT_SCRIPT := preload("res://scripts/api_client.gd")
const AUTH_STORE_SCRIPT := preload("res://scripts/auth_store.gd")

var _api: Node
var _status_label: Label
var _base_url_input: LineEdit
var _account_input: LineEdit
var _password_input: LineEdit
var _start_new_btn: Button
var _continue_btn: Button
var _clear_btn: Button
var _quit_btn: Button
var _cached_username: String = ""
var _cached_token: String = ""


func _ready() -> void:
	_api = API_CLIENT_SCRIPT.new()
	add_child(_api)
	_build_ui()
	_status("请输入账号密码登录。")
	call_deferred("_try_auto_restore")


func _build_ui() -> void:
	var bg := ColorRect.new()
	bg.set_anchors_preset(Control.PRESET_FULL_RECT)
	bg.color = Color(0.05, 0.08, 0.14, 1.0)
	add_child(bg)

	var shell := MarginContainer.new()
	shell.set_anchors_preset(Control.PRESET_FULL_RECT)
	shell.add_theme_constant_override("margin_left", 180)
	shell.add_theme_constant_override("margin_right", 180)
	shell.add_theme_constant_override("margin_top", 120)
	shell.add_theme_constant_override("margin_bottom", 120)
	add_child(shell)

	var panel := PanelContainer.new()
	shell.add_child(panel)
	var sb := StyleBoxFlat.new()
	sb.bg_color = Color(0.11, 0.16, 0.25, 0.96)
	sb.border_color = Color(0.30, 0.47, 0.72, 1.0)
	sb.set_corner_radius_all(12)
	sb.set_border_width_all(1)
	sb.content_margin_left = 16
	sb.content_margin_right = 16
	sb.content_margin_top = 14
	sb.content_margin_bottom = 14
	panel.add_theme_stylebox_override("panel", sb)

	var box := VBoxContainer.new()
	box.add_theme_constant_override("separation", 10)
	panel.add_child(box)

	var title := Label.new()
	title.text = "TradingArt K线卡牌"
	title.add_theme_font_size_override("font_size", 34)
	title.add_theme_color_override("font_color", Color(0.93, 0.97, 1.0, 1.0))
	box.add_child(title)

	var subtitle := Label.new()
	subtitle.text = "登录后可选择新开一局或继续游戏"
	subtitle.add_theme_color_override("font_color", Color(0.72, 0.84, 0.97, 1.0))
	box.add_child(subtitle)

	_status_label = Label.new()
	_status_label.add_theme_color_override("font_color", Color(0.78, 0.89, 1.0, 1.0))
	box.add_child(_status_label)

	_base_url_input = _line("API Base URL", AUTH_STORE_SCRIPT.DEFAULT_BASE_URL)
	_account_input = _line("账号（用户名或邮箱）", "")
	_password_input = _line("密码", "")
	_password_input.secret = true

	box.add_child(_row("API地址", _base_url_input))
	box.add_child(_row("账号", _account_input))
	box.add_child(_row("密码", _password_input))

	var btn_row1 := HBoxContainer.new()
	btn_row1.add_theme_constant_override("separation", 8)
	box.add_child(btn_row1)
	_start_new_btn = Button.new()
	_start_new_btn.text = "重新开始"
	_start_new_btn.pressed.connect(_on_start_new_pressed)
	_style_btn(_start_new_btn, true)
	btn_row1.add_child(_start_new_btn)
	_continue_btn = Button.new()
	_continue_btn.text = "继续游戏"
	_continue_btn.pressed.connect(_on_continue_pressed)
	_style_btn(_continue_btn, true)
	btn_row1.add_child(_continue_btn)

	var btn_row2 := HBoxContainer.new()
	btn_row2.add_theme_constant_override("separation", 8)
	box.add_child(btn_row2)
	_quit_btn = Button.new()
	_quit_btn.text = "离开游戏"
	_quit_btn.pressed.connect(_on_quit_pressed)
	_style_btn(_quit_btn, false)
	btn_row2.add_child(_quit_btn)
	_clear_btn = Button.new()
	_clear_btn.text = "清除本地会话"
	_clear_btn.pressed.connect(_on_clear_pressed)
	_style_btn(_clear_btn, false)
	btn_row2.add_child(_clear_btn)


func _line(ph: String, text: String) -> LineEdit:
	var le := LineEdit.new()
	le.placeholder_text = ph
	le.text = text
	return le


func _row(name: String, field: Control) -> HBoxContainer:
	var row := HBoxContainer.new()
	row.add_theme_constant_override("separation", 8)
	var lb := Label.new()
	lb.text = "%s:" % name
	lb.custom_minimum_size = Vector2(90, 0)
	lb.add_theme_color_override("font_color", Color(0.82, 0.90, 0.99, 1.0))
	row.add_child(lb)
	field.size_flags_horizontal = Control.SIZE_EXPAND_FILL
	row.add_child(field)
	return row


func _style_btn(btn: Button, primary: bool) -> void:
	btn.custom_minimum_size = Vector2(0, 34)
	var normal := StyleBoxFlat.new()
	normal.set_corner_radius_all(8)
	normal.set_border_width_all(1)
	var hover := StyleBoxFlat.new()
	hover.set_corner_radius_all(8)
	hover.set_border_width_all(1)
	if primary:
		normal.bg_color = Color(0.22, 0.34, 0.53, 1.0)
		normal.border_color = Color(0.38, 0.58, 0.86, 1.0)
		hover.bg_color = Color(0.26, 0.40, 0.62, 1.0)
		hover.border_color = Color(0.46, 0.68, 0.96, 1.0)
	else:
		normal.bg_color = Color(0.16, 0.22, 0.34, 1.0)
		normal.border_color = Color(0.30, 0.42, 0.58, 1.0)
		hover.bg_color = Color(0.20, 0.28, 0.42, 1.0)
		hover.border_color = Color(0.36, 0.50, 0.70, 1.0)
	btn.add_theme_stylebox_override("normal", normal)
	btn.add_theme_stylebox_override("hover", hover)
	btn.add_theme_color_override("font_color", Color(0.94, 0.98, 1.0, 1.0))


func _status(text: String) -> void:
	if _status_label != null:
		_status_label.text = "状态：%s" % text


func _set_loading(loading: bool) -> void:
	_start_new_btn.disabled = loading
	_continue_btn.disabled = loading
	_clear_btn.disabled = loading
	_quit_btn.disabled = loading


func _try_auto_restore() -> void:
	var saved: Dictionary = AUTH_STORE_SCRIPT.load_session()
	var base_url := str(saved.get("base_url", "")).strip_edges()
	var username := str(saved.get("username", "")).strip_edges()
	var token := str(saved.get("token", "")).strip_edges()
	if not base_url.is_empty():
		_base_url_input.text = base_url
	if username.is_empty() or token.is_empty():
		return
	_status("检测到历史会话，尝试自动续登...")
	_set_loading(true)
	_api.base_url = _base_url_input.text.strip_edges()
	var res: Dictionary = await _api.post_json(
		"/v1/card/auth/restore",
		{
			"username": username,
			"token": token,
		}
	)
	_set_loading(false)
	if not res.get("ok", false):
		_status("自动续登失败，请重新登录账号密码。")
		AUTH_STORE_SCRIPT.clear_session()
		_cached_username = ""
		_cached_token = ""
		return
	_cached_username = username
	_cached_token = token
	_status("自动续登成功，请选择“新开一局”或“继续游戏”。")


func _ensure_session_and_enter(entry_mode: String) -> void:
	if not _cached_username.is_empty() and not _cached_token.is_empty():
		login_success.emit(_base_url_input.text.strip_edges(), _cached_username, _cached_token, entry_mode)
		return
	var account := _account_input.text.strip_edges()
	var password := _password_input.text
	if account.is_empty() or password.is_empty():
		_status("请输入账号和密码。")
		return
	_set_loading(true)
	_status("登录中...")
	_api.base_url = _base_url_input.text.strip_edges()
	var res: Dictionary = await _api.post_json(
		"/v1/card/auth/login",
		{
			"account": account,
			"password": password,
		}
	)
	_set_loading(false)
	if not res.get("ok", false):
		_status("登录失败：%s" % str(res.get("message", "unknown")))
		return
	var username := str(res.get("username", "")).strip_edges()
	var token := str(res.get("token", "")).strip_edges()
	if username.is_empty() or token.is_empty():
		_status("登录失败：返回会话为空。")
		return
	AUTH_STORE_SCRIPT.save_session(_base_url_input.text.strip_edges(), username, token)
	_cached_username = username
	_cached_token = token
	_status("登录成功。")
	login_success.emit(_base_url_input.text.strip_edges(), username, token, entry_mode)


func _on_start_new_pressed() -> void:
	await _ensure_session_and_enter("create")


func _on_continue_pressed() -> void:
	await _ensure_session_and_enter("resume")


func _on_clear_pressed() -> void:
	AUTH_STORE_SCRIPT.clear_session()
	_cached_username = ""
	_cached_token = ""
	_status("已清除本地会话。")


func _on_quit_pressed() -> void:
	get_tree().quit()
