extends Control

const LOGIN_SCENE := preload("res://scenes/Login.tscn")
const MAP_SCENE := preload("res://scenes/Map.tscn")
const GAME_SCENE := preload("res://scenes/Main.tscn")
const AUTH_STORE_SCRIPT := preload("res://scripts/auth_store.gd")

var _active_scene: Control
var _session: Dictionary = {}
var _loading_overlay: ColorRect
var _loading_title: Label
var _loading_dots: Label
var _loading_progress: ProgressBar
var _loading_animating: bool = false
var _loading_elapsed: float = 0.0
var _map_cached_state: Dictionary = {}


func _ready() -> void:
	_build_loading_overlay()
	set_process(false)
	_show_login()


func _process(delta: float) -> void:
	if not _loading_animating:
		return
	_loading_elapsed += max(0.0, delta)
	if _loading_dots != null:
		var dot_count := (int(floor(_loading_elapsed * 3.6)) % 6) + 1
		_loading_dots.text = ".".repeat(dot_count)
	if _loading_progress != null:
		var speed := 36.0 if _loading_progress.value < 70.0 else 11.0
		_loading_progress.value = min(92.0, _loading_progress.value + delta * speed)


func _clear_active_scene() -> void:
	if _active_scene == null:
		return
	_active_scene.queue_free()
	_active_scene = null


func _build_loading_overlay() -> void:
	_loading_overlay = ColorRect.new()
	_loading_overlay.set_anchors_preset(Control.PRESET_FULL_RECT)
	_loading_overlay.color = Color(0.02, 0.07, 0.12, 0.94)
	_loading_overlay.mouse_filter = Control.MOUSE_FILTER_STOP
	_loading_overlay.z_index = 980
	_loading_overlay.visible = false
	add_child(_loading_overlay)

	var center := CenterContainer.new()
	center.set_anchors_preset(Control.PRESET_FULL_RECT)
	center.mouse_filter = Control.MOUSE_FILTER_IGNORE
	_loading_overlay.add_child(center)

	var card := PanelContainer.new()
	card.custom_minimum_size = Vector2(640, 280)
	center.add_child(card)
	var style := StyleBoxFlat.new()
	style.bg_color = Color(0.04, 0.12, 0.22, 0.98)
	style.border_color = Color(0.36, 0.76, 0.96, 0.95)
	style.set_border_width_all(2)
	style.set_corner_radius_all(14)
	style.shadow_color = Color(0.0, 0.0, 0.0, 0.42)
	style.shadow_size = 12
	style.shadow_offset = Vector2(0, 4)
	style.content_margin_left = 24
	style.content_margin_right = 24
	style.content_margin_top = 24
	style.content_margin_bottom = 20
	card.add_theme_stylebox_override("panel", style)

	var box := VBoxContainer.new()
	box.set_anchors_preset(Control.PRESET_FULL_RECT)
	box.add_theme_constant_override("separation", 14)
	card.add_child(box)

	_loading_title = Label.new()
	_loading_title.text = "正在读取游戏数据"
	_loading_title.horizontal_alignment = HORIZONTAL_ALIGNMENT_CENTER
	_loading_title.add_theme_font_size_override("font_size", 42)
	_loading_title.add_theme_color_override("font_color", Color(0.82, 0.97, 1.0, 1.0))
	box.add_child(_loading_title)

	_loading_dots = Label.new()
	_loading_dots.text = "."
	_loading_dots.horizontal_alignment = HORIZONTAL_ALIGNMENT_CENTER
	_loading_dots.add_theme_font_size_override("font_size", 30)
	_loading_dots.add_theme_color_override("font_color", Color(0.64, 0.90, 1.0, 0.95))
	box.add_child(_loading_dots)

	var spacer := Control.new()
	spacer.custom_minimum_size = Vector2(0, 16)
	box.add_child(spacer)

	var tip := Label.new()
	tip.text = "正在同步战斗状态，请稍候"
	tip.horizontal_alignment = HORIZONTAL_ALIGNMENT_CENTER
	tip.add_theme_font_size_override("font_size", 20)
	tip.add_theme_color_override("font_color", Color(0.70, 0.86, 0.98, 0.92))
	box.add_child(tip)

	_loading_progress = ProgressBar.new()
	_loading_progress.custom_minimum_size = Vector2(0, 30)
	_loading_progress.size_flags_horizontal = Control.SIZE_EXPAND_FILL
	_loading_progress.min_value = 0.0
	_loading_progress.max_value = 100.0
	_loading_progress.value = 0.0
	_loading_progress.show_percentage = false
	box.add_child(_loading_progress)


func _set_loading_overlay(open: bool, title: String = "正在读取游戏数据") -> void:
	if _loading_overlay == null:
		return
	if open:
		_loading_animating = true
		_loading_elapsed = 0.0
		if _loading_title != null:
			_loading_title.text = title
		if _loading_dots != null:
			_loading_dots.text = "."
		if _loading_progress != null:
			_loading_progress.value = 8.0
		_loading_overlay.visible = true
		set_process(true)
	else:
		_loading_animating = false
		_loading_overlay.visible = false
		set_process(false)


func _finish_loading_overlay() -> void:
	if _loading_overlay == null or not _loading_overlay.visible:
		return
	_loading_animating = false
	if _loading_progress != null:
		_loading_progress.value = 100.0
	await get_tree().create_timer(0.08).timeout
	_set_loading_overlay(false)


func _show_login() -> void:
	_set_loading_overlay(false)
	_clear_active_scene()
	var scene: Control = LOGIN_SCENE.instantiate()
	scene.set_anchors_preset(Control.PRESET_FULL_RECT)
	add_child(scene)
	_active_scene = scene
	if scene.has_signal("login_success"):
		scene.login_success.connect(_on_login_success)


func _on_login_success(base_url: String, username: String, token: String, entry_mode: String) -> void:
	_session = {
		"base_url": base_url,
		"username": username,
		"token": token,
		"entry_mode": entry_mode,
	}
	_map_cached_state = {}
	await _show_map(entry_mode)


func _show_map(entry_mode: String = "resume", preset_map_run_id: int = 0, returned_battle_run_id: int = 0) -> void:
	_set_loading_overlay(true, "正在读取地图数据")
	_clear_active_scene()
	var scene: Control = MAP_SCENE.instantiate()
	scene.set_anchors_preset(Control.PRESET_FULL_RECT)
	add_child(scene)
	_active_scene = scene
	if scene.has_signal("launch_battle"):
		scene.launch_battle.connect(_on_map_launch_battle)
	if scene.has_signal("battle_loading"):
		scene.battle_loading.connect(_on_map_battle_loading)
	if scene.has_signal("map_state_updated"):
		scene.map_state_updated.connect(_on_map_state_updated)
	if scene.has_signal("logout_requested"):
		scene.logout_requested.connect(_on_game_logout_requested)
	if scene.has_method("set_boot_cached_state"):
		var cache_to_pass: Dictionary = {}
		var preset_id := int(preset_map_run_id)
		var cached_id := int(_map_cached_state.get("map_run_id", 0))
		if preset_id > 0 and cached_id == preset_id:
			cache_to_pass = _map_cached_state.duplicate(true)
		elif preset_id <= 0 and cached_id > 0 and entry_mode.strip_edges().to_lower() == "resume":
			cache_to_pass = _map_cached_state.duplicate(true)
		scene.set_boot_cached_state(cache_to_pass)
	if scene.has_method("boot_with_session"):
		await scene.boot_with_session(
			str(_session.get("base_url", "")),
			str(_session.get("username", "")),
			str(_session.get("token", "")),
			entry_mode,
			int(preset_map_run_id),
			int(returned_battle_run_id),
		)
	await _finish_loading_overlay()


func _on_map_launch_battle(map_run_id: int, battle_run_id: int) -> void:
	if _active_scene != null and _active_scene.has_method("export_cached_state"):
		var snapshot_v: Variant = _active_scene.call("export_cached_state")
		if snapshot_v is Dictionary and not (snapshot_v as Dictionary).is_empty():
			_map_cached_state = (snapshot_v as Dictionary).duplicate(true)
	await _show_game_for_map(map_run_id, battle_run_id)


func _on_map_battle_loading(active: bool, title: String) -> void:
	if active:
		var t := title.strip_edges()
		_set_loading_overlay(true, "正在读取战斗数据" if t.is_empty() else t)
	else:
		_set_loading_overlay(false)


func _on_map_state_updated(map_state: Dictionary) -> void:
	if typeof(map_state) != TYPE_DICTIONARY:
		return
	if map_state.is_empty():
		return
	_map_cached_state = map_state.duplicate(true)


func _show_game_for_map(map_run_id: int, battle_run_id: int) -> void:
	_set_loading_overlay(true, "正在读取战斗数据")
	_clear_active_scene()
	var scene: Control = GAME_SCENE.instantiate()
	scene.set_anchors_preset(Control.PRESET_FULL_RECT)
	add_child(scene)
	_active_scene = scene
	if scene.has_signal("logout_requested"):
		scene.logout_requested.connect(_on_game_logout_requested)
	if scene.has_signal("battle_exit_requested"):
		scene.battle_exit_requested.connect(_on_battle_exit_requested)
	if scene.has_method("boot_from_map_battle"):
		await scene.boot_from_map_battle(
			str(_session.get("base_url", "")),
			str(_session.get("username", "")),
			str(_session.get("token", "")),
			int(battle_run_id),
			int(map_run_id),
		)
	await _finish_loading_overlay()


func _on_battle_exit_requested(map_run_id: int, battle_run_id: int) -> void:
	await _show_map("resume", int(map_run_id), int(battle_run_id))


func _on_game_logout_requested(clear_saved_session: bool) -> void:
	if clear_saved_session:
		AUTH_STORE_SCRIPT.clear_session()
	_map_cached_state = {}
	_show_login()
