extends Control

signal launch_battle(map_run_id: int, battle_run_id: int)
signal battle_loading(active: bool, title: String)
signal map_state_updated(map_state: Dictionary)
signal logout_requested(clear_saved_session: bool)

const API_CLIENT_SCRIPT := preload("res://scripts/api_client.gd")

const CARD_NAMES := {
	"short_long_novice": "日内短线多-新手",
	"short_long_skilled": "日内短线多-熟练",
	"short_long_veteran": "日内短线多-老手",
	"short_long_master": "日内短线多-大师",
	"short_short_novice": "日内短线空-新手",
	"short_short_skilled": "日内短线空-熟练",
	"short_short_veteran": "日内短线空-老手",
	"short_short_master": "日内短线空-大师",
	"trend_long_novice": "顺势做多-新手",
	"trend_long_skilled": "顺势做多-熟练",
	"trend_long_veteran": "顺势做多-老手",
	"trend_long_master": "顺势做多-大师",
	"trend_short_novice": "顺势做空-新手",
	"trend_short_skilled": "顺势做空-熟练",
	"trend_short_veteran": "顺势做空-老手",
	"trend_short_master": "顺势做空-大师",
	"breakout_long_novice": "突破追多-新手",
	"breakout_long_veteran": "突破追多-老手",
	"breakout_short_novice": "突破追空-新手",
	"breakout_short_veteran": "突破追空-老手",
	"tactic_quick_cancel": "快速撤单",
	"tactic_scalp_cycle": "剥头皮循环",
	"tactic_leverage": "借钱加杠杆",
	"tactic_risk_control": "风险控制",
	"tactic_meditation": "冥想思考",
	"tactic_dynamic_adjust": "动态调整",
	"tactic_self_confidence": "自信下单",
	"tactic_fast_stop": "快速止损",
	"arb_east_novice": "跨期套利东-新手",
	"arb_east_veteran": "跨期套利东-老手",
	"arb_west_novice": "跨期套利西-新手",
	"arb_west_veteran": "跨期套利西-老手",
	"arb_south_novice": "跨期套利南-新手",
	"arb_south_veteran": "跨期套利南-老手",
	"arb_north_novice": "跨期套利北-新手",
	"arb_north_veteran": "跨期套利北-老手",
	"option_buy_call_novice": "买看涨做多-新手",
	"option_buy_call_skilled": "买看涨做多-熟练",
	"option_buy_call_veteran": "买看涨做多-老手",
	"option_buy_call_master": "买看涨做多-大师",
	"option_buy_put_novice": "买看跌期权-新手",
	"option_buy_put_skilled": "买看跌期权-熟练",
	"option_buy_put_veteran": "买看跌期权-老手",
	"option_buy_put_master": "买看跌期权-大师",
	"option_sell_call_novice": "卖看涨期权-新手",
	"option_sell_call_skilled": "卖看涨期权-熟练",
	"option_sell_call_veteran": "卖看涨期权-老手",
	"option_sell_call_master": "卖看涨期权-大师",
	"option_sell_put_novice": "卖看跌期权-新手",
	"option_sell_put_skilled": "卖看跌期权-熟练",
	"option_sell_put_veteran": "卖看跌期权-老手",
	"option_sell_put_master": "卖看跌期权-大师",
}

const HOME_VIEW_HOME := "home"
const HOME_VIEW_ATTR := "attr"
const HOME_VIEW_DECK := "deck"
const HOME_VIEW_OUT := "out"
const SETUP_STEP_NAME := 1
const SETUP_STEP_TRAITS := 2
const SETUP_STEP_STYLE := 3
const SETUP_STEP_CONFIRM := 4
const DECK_AUTOSAVE_DELAY_SEC := 0.35
const POOL_COPIES_PER_CARD := 5
const TRAIT_OPTIONS := [
	{"key": "social", "title": "社交倾向", "options": ["外向", "内向"]},
	{"key": "ego", "title": "自我认知", "options": ["谦虚", "自信"]},
	{"key": "rule", "title": "行为偏好", "options": ["喜欢规则", "喜欢改变"]},
	{"key": "value", "title": "价值倾向", "options": ["看重自由", "看重平等"]},
]
const STYLE_QUESTIONS := [
	{"key": "horizon_preference", "title": "偏好交易周期", "options": [{"code": "short", "label": "短线"}, {"code": "long", "label": "长线"}]},
	{"key": "risk_preference", "title": "风险收益取向", "options": [{"code": "avoid_loss", "label": "规避亏损"}, {"code": "seek_profit", "label": "追求获利"}]},
	{"key": "priority_preference", "title": "你更看重", "options": [{"code": "skill", "label": "交易技巧"}, {"code": "mindset", "label": "心理素质"}]},
]
const OUTING_POI_DEFS := [
	{"id": "home", "label": "住宅", "x": 0.18, "y": 0.72, "open_state": "open", "location_code": "home", "stamina_cost": 0},
	{"id": "association", "label": "基金业协会", "x": 0.76, "y": 0.38, "open_state": "open", "location_code": "association", "stamina_cost": 10},
	{"id": "exchange", "label": "证券交易所", "x": 0.58, "y": 0.28, "open_state": "placeholder", "location_code": "", "stamina_cost": 10},
	{"id": "bank", "label": "商业银行", "x": 0.66, "y": 0.64, "open_state": "placeholder", "location_code": "", "stamina_cost": 10},
	{"id": "broker", "label": "券商营业部", "x": 0.42, "y": 0.53, "open_state": "placeholder", "location_code": "", "stamina_cost": 10},
	{"id": "media", "label": "财经媒体", "x": 0.31, "y": 0.22, "open_state": "placeholder", "location_code": "", "stamina_cost": 10},
	{"id": "research", "label": "研究院", "x": 0.84, "y": 0.16, "open_state": "placeholder", "location_code": "", "stamina_cost": 10},
]

var _api: ApiClient
var _actions_enabled: bool = false
var _entry_mode: String = "resume"
var _map_run_id: int = 0
var _state: Dictionary = {}
var _boot_cached_state: Dictionary = {}
var _editing_deck: Array[String] = []
var _pool_ids: Array[String] = []
var _pool_cards: Array[String] = []
var _sort_mode: String = "text"
var _end_log_marker: String = ""
var _home_view_mode: String = HOME_VIEW_HOME
var _battle_start_inflight: bool = false

var _status_label: Label
var _log_box: RichTextLabel

var _create_btn: Button
var _resume_btn: Button
var _logout_btn: Button

var _move_home_btn: Button
var _move_assoc_btn: Button
var _battle_btn: Button
var _rest_btn: Button
var _add_card_btn: Button
var _remove_card_btn: Button

var _home_scene: Control
var _assoc_scene: Control

var _home_attr_panel: PanelContainer
var _home_deck_panel: PanelContainer
var _home_out_panel: PanelContainer
var _home_home_panel: PanelContainer
var _home_attr_label: Label
var _home_scene_title_label: Label
var _home_scene_stats_panel: PanelContainer
var _home_view_host: PanelContainer
var _home_menu_panel_ref: PanelContainer

var _pool_list: ItemList
var _deck_list: ItemList
var _deck_list_title_label: Label
var _deck_hint_label: Label
var _deck_detail_panel: PanelContainer
var _deck_detail_title: Label
var _deck_detail_body: RichTextLabel
var _direction_sort_btn: Button
var _level_sort_btn: Button
var _detail_source_zone: String = ""
var _detail_card_id: String = ""
var _pool_stack_entries: Array[Dictionary] = []
var _deck_stack_entries: Array[Dictionary] = []
var _deck_autosave_timer: Timer
var _deck_autosave_dirty: bool = false
var _deck_autosave_inflight: bool = false

var _home_attr_btn: Button
var _home_deck_btn: Button
var _home_out_btn: Button
var _assoc_back_home_btn: Button
var _assoc_battle_btn: Button
var _assoc_desc_label: Label
var _home_scene_stats_label: Label
var _outing_info_label: Label
var _outing_hint_label: Label
var _outing_map_buttons: Array[Button] = []
var _outing_poi_buttons: Dictionary = {}
var _outing_poi_glows: Dictionary = {}
var _outing_map_assets: Dictionary = {}
var _outing_map_root: Control
var _outing_map_canvas: Control
var _outing_route_layer: Control
var _outing_back_btn: Button
var _outing_battle_fab: Button

var _settings_overlay: ColorRect
var _settings_modal: PanelContainer
var _settings_new_map_btn: Button
var _settings_home_btn: Button
var _settings_quit_btn: Button
var _settings_run_summary_label: Label
var _settings_version_label: Label

var _new_game_setup_overlay: ColorRect
var _new_game_setup_modal: PanelContainer
var _new_game_setup_title: Label
var _new_game_setup_step_label: Label
var _new_game_setup_content: VBoxContainer
var _new_game_setup_error_label: Label
var _new_game_setup_prev_btn: Button
var _new_game_setup_next_btn: Button
var _new_game_setup_confirm_btn: Button
var _new_game_setup_cancel_btn: Button

var _setup_step: int = SETUP_STEP_NAME
var _setup_inflight: bool = false
var _setup_entry_reason: String = ""
var _setup_form_player_name: String = ""
var _setup_form_traits: Array[String] = ["外向", "谦虚", "喜欢规则", "看重自由"]
var _setup_form_style_answers: Dictionary = {
	"horizon_preference": "long",
	"risk_preference": "avoid_loss",
	"priority_preference": "skill",
}
var _setup_form_god_mode: bool = false

var _setup_name_input: LineEdit
var _setup_trait_selects: Array[OptionButton] = []
var _setup_style_selects: Dictionary = {}
var _setup_god_mode_check: CheckBox


func _ready() -> void:
	_api = API_CLIENT_SCRIPT.new()
	add_child(_api)
	_deck_autosave_timer = Timer.new()
	_deck_autosave_timer.one_shot = true
	_deck_autosave_timer.wait_time = DECK_AUTOSAVE_DELAY_SEC
	_deck_autosave_timer.timeout.connect(_on_deck_autosave_timeout)
	add_child(_deck_autosave_timer)
	_build_ui()
	_init_card_pool()
	_render_deck_lists()
	_update_sort_mode_buttons()
	_status("地图客户端就绪。等待登录会话初始化。")
	_set_action_enabled(false)


func boot_with_session(
	base_url: String,
	username: String,
	session_token: String,
	entry_mode: String = "resume",
	preset_map_run_id: int = 0,
	returned_battle_run_id: int = 0
) -> void:
	_entry_mode = entry_mode.strip_edges().to_lower()
	_map_run_id = int(preset_map_run_id)
	var has_cached_state := false
	if _map_run_id > 0 and not _boot_cached_state.is_empty():
		var cached_run_id := int(_boot_cached_state.get("map_run_id", 0))
		if cached_run_id == _map_run_id:
			has_cached_state = _apply_state_snapshot(_boot_cached_state, true)
	_api.base_url = base_url.strip_edges()
	_api.set_auth(username.strip_edges(), session_token.strip_edges())
	await _connect_and_boot(has_cached_state)
	if returned_battle_run_id > 0 and _map_run_id > 0:
		await _commit_returned_battle(returned_battle_run_id)


func set_boot_cached_state(cached_state: Dictionary) -> void:
	if typeof(cached_state) == TYPE_DICTIONARY:
		_boot_cached_state = cached_state.duplicate(true)
	else:
		_boot_cached_state = {}


func export_cached_state() -> Dictionary:
	return _state.duplicate(true)


func _apply_state_snapshot(state: Dictionary, from_cache: bool = false) -> bool:
	if state.is_empty():
		return false
	_state = state.duplicate(true)
	_render_state()
	map_state_updated.emit(_state.duplicate(true))
	if from_cache:
		_status("地图缓存已加载，正在同步最新状态...")
	return true


func _apply_map_run_from_payload(payload: Dictionary, from_cache: bool = false) -> bool:
	var map_state_v: Variant = payload.get("map_run", {})
	if not (map_state_v is Dictionary):
		return false
	var map_state: Dictionary = map_state_v
	if map_state.is_empty():
		return false
	return _apply_state_snapshot(map_state, from_cache)


func _build_ui() -> void:
	var scene_host := Control.new()
	scene_host.set_anchors_preset(Control.PRESET_FULL_RECT)
	add_child(scene_host)

	_home_scene = Control.new()
	_home_scene.set_anchors_preset(Control.PRESET_FULL_RECT)
	scene_host.add_child(_home_scene)
	_build_home_scene(_home_scene)

	_assoc_scene = Control.new()
	_assoc_scene.set_anchors_preset(Control.PRESET_FULL_RECT)
	scene_host.add_child(_assoc_scene)
	_build_association_scene(_assoc_scene)

	_render_location_panels("home")
	_set_home_view(HOME_VIEW_HOME)
	_build_settings_overlay()


func _build_home_scene(parent: Control) -> void:
	var bg := TextureRect.new()
	bg.set_anchors_preset(Control.PRESET_FULL_RECT)
	bg.stretch_mode = TextureRect.STRETCH_SCALE
	bg.texture = _make_scene_bg_texture("home")
	parent.add_child(bg)

	var veil := ColorRect.new()
	veil.set_anchors_preset(Control.PRESET_FULL_RECT)
	veil.color = Color(0.02, 0.05, 0.09, 0.22)
	parent.add_child(veil)

	_home_scene_title_label = Label.new()
	_home_scene_title_label.text = "住宅｜交易员公寓"
	_home_scene_title_label.anchor_left = 0.03
	_home_scene_title_label.anchor_top = 0.12
	_home_scene_title_label.anchor_right = 0.55
	_home_scene_title_label.anchor_bottom = 0.18
	_home_scene_title_label.add_theme_font_size_override("font_size", 30)
	_home_scene_title_label.add_theme_color_override("font_color", Color(0.94, 0.97, 1.0, 1.0))
	parent.add_child(_home_scene_title_label)

	_home_scene_stats_panel = PanelContainer.new()
	_home_scene_stats_panel.anchor_left = 0.03
	_home_scene_stats_panel.anchor_top = 0.03
	_home_scene_stats_panel.anchor_right = 0.34
	_home_scene_stats_panel.anchor_bottom = 0.11
	_style_panel(_home_scene_stats_panel, Color(0.08, 0.12, 0.20, 0.86), Color(0.76, 0.65, 0.38, 0.96), 14)
	parent.add_child(_home_scene_stats_panel)
	_home_scene_stats_label = Label.new()
	_home_scene_stats_label.text = "日期：-\n体力：-   金钱：-"
	_home_scene_stats_label.add_theme_font_size_override("font_size", 18)
	_home_scene_stats_label.add_theme_color_override("font_color", Color(0.95, 0.90, 0.74, 1.0))
	_home_scene_stats_label.vertical_alignment = VERTICAL_ALIGNMENT_CENTER
	_home_scene_stats_panel.add_child(_home_scene_stats_label)

	_home_view_panel(parent)
	_home_menu_panel(parent)


func _home_view_panel(parent: Control) -> void:
	_home_view_host = PanelContainer.new()
	_home_view_host.anchor_left = 0.03
	_home_view_host.anchor_top = 0.20
	_home_view_host.anchor_right = 0.70
	_home_view_host.anchor_bottom = 0.95
	_style_panel(_home_view_host, Color(0.07, 0.11, 0.19, 0.90), Color(0.34, 0.52, 0.74, 0.92), 12)
	parent.add_child(_home_view_host)

	var stack := Control.new()
	stack.set_anchors_preset(Control.PRESET_FULL_RECT)
	_home_view_host.add_child(stack)

	_home_home_panel = PanelContainer.new()
	_home_home_panel.set_anchors_preset(Control.PRESET_FULL_RECT)
	_style_panel(_home_home_panel, Color(0.08, 0.14, 0.22, 0.92), Color(0.30, 0.47, 0.66, 0.85), 8)
	stack.add_child(_home_home_panel)
	_build_home_landing_panel(_home_home_panel)

	_home_attr_panel = PanelContainer.new()
	_home_attr_panel.set_anchors_preset(Control.PRESET_FULL_RECT)
	_style_panel(_home_attr_panel, Color(0.08, 0.14, 0.22, 0.92), Color(0.30, 0.47, 0.66, 0.85), 8)
	stack.add_child(_home_attr_panel)
	var attr_box := VBoxContainer.new()
	attr_box.add_theme_constant_override("separation", 8)
	_home_attr_panel.add_child(attr_box)
	var attr_header := HBoxContainer.new()
	attr_box.add_child(attr_header)
	var attr_title := Label.new()
	attr_title.text = "属性查看"
	attr_title.add_theme_font_size_override("font_size", 20)
	attr_header.add_child(attr_title)
	var attr_spacer := Control.new()
	attr_spacer.size_flags_horizontal = Control.SIZE_EXPAND_FILL
	attr_header.add_child(attr_spacer)
	var attr_back := _make_back_home_btn(_on_home_back_pressed)
	attr_header.add_child(attr_back)
	_home_attr_label = Label.new()
	_home_attr_label.autowrap_mode = TextServer.AUTOWRAP_WORD_SMART
	_home_attr_label.size_flags_vertical = Control.SIZE_EXPAND_FILL
	_home_attr_label.add_theme_font_size_override("font_size", 18)
	attr_box.add_child(_home_attr_label)

	_home_deck_panel = PanelContainer.new()
	_home_deck_panel.set_anchors_preset(Control.PRESET_FULL_RECT)
	_style_panel(_home_deck_panel, Color(0.08, 0.14, 0.22, 0.92), Color(0.30, 0.47, 0.66, 0.85), 8)
	stack.add_child(_home_deck_panel)
	_build_deck_editor(_home_deck_panel)

	_home_out_panel = PanelContainer.new()
	_home_out_panel.set_anchors_preset(Control.PRESET_FULL_RECT)
	_style_panel(_home_out_panel, Color(0.08, 0.14, 0.22, 0.92), Color(0.30, 0.47, 0.66, 0.85), 8)
	stack.add_child(_home_out_panel)
	_build_outing_panel(_home_out_panel)

	for p in [_home_home_panel, _home_attr_panel, _home_deck_panel, _home_out_panel]:
		if p != null:
			p.modulate = Color(1, 1, 1, 1)


func _build_home_landing_panel(parent: PanelContainer) -> void:
	var root := HBoxContainer.new()
	root.size_flags_vertical = Control.SIZE_EXPAND_FILL
	root.add_theme_constant_override("separation", 16)
	parent.add_child(root)

	var portrait_shell := PanelContainer.new()
	portrait_shell.custom_minimum_size = Vector2(320, 0)
	portrait_shell.size_flags_vertical = Control.SIZE_EXPAND_FILL
	_style_panel(portrait_shell, Color(0.09, 0.13, 0.21, 0.92), Color(0.74, 0.63, 0.38, 0.96), 10)
	root.add_child(portrait_shell)

	var portrait := TextureRect.new()
	portrait.set_anchors_preset(Control.PRESET_FULL_RECT)
	portrait.stretch_mode = TextureRect.STRETCH_SCALE
	portrait.texture = _make_home_portrait_texture()
	portrait_shell.add_child(portrait)

	var right := VBoxContainer.new()
	right.size_flags_horizontal = Control.SIZE_EXPAND_FILL
	right.size_flags_vertical = Control.SIZE_EXPAND_FILL
	right.alignment = BoxContainer.ALIGNMENT_CENTER
	right.add_theme_constant_override("separation", 14)
	root.add_child(right)

	var title := Label.new()
	title.text = "交易员待命"
	title.add_theme_font_size_override("font_size", 34)
	title.add_theme_color_override("font_color", Color(0.95, 0.91, 0.75, 1.0))
	right.add_child(title)

	var line := ColorRect.new()
	line.custom_minimum_size = Vector2(460, 2)
	line.color = Color(0.73, 0.63, 0.37, 0.72)
	right.add_child(line)

	var desc := Label.new()
	desc.text = "请选择右侧按钮安排本回合：\n查看属性、调整卡组、外出行动或先休息恢复。"
	desc.autowrap_mode = TextServer.AUTOWRAP_WORD_SMART
	desc.horizontal_alignment = HORIZONTAL_ALIGNMENT_LEFT
	desc.add_theme_font_size_override("font_size", 20)
	desc.add_theme_color_override("font_color", Color(0.80, 0.90, 0.99, 1.0))
	right.add_child(desc)

	var tip := Label.new()
	tip.text = "提示：子页面均支持返回住宅主页。"
	tip.add_theme_color_override("font_color", Color(0.67, 0.81, 0.97, 1.0))
	tip.add_theme_font_size_override("font_size", 16)
	right.add_child(tip)


func _home_menu_panel(parent: Control) -> void:
	_home_menu_panel_ref = PanelContainer.new()
	_home_menu_panel_ref.anchor_left = 0.73
	_home_menu_panel_ref.anchor_top = 0.20
	_home_menu_panel_ref.anchor_right = 0.97
	_home_menu_panel_ref.anchor_bottom = 0.95
	_style_panel(_home_menu_panel_ref, Color(0.04, 0.08, 0.14, 0.80), Color(0.74, 0.63, 0.38, 0.96), 14)
	parent.add_child(_home_menu_panel_ref)

	var box := VBoxContainer.new()
	box.add_theme_constant_override("separation", 12)
	_home_menu_panel_ref.add_child(box)

	_home_attr_btn = _make_menu_btn("属性查看", _on_home_attr_view_pressed)
	box.add_child(_home_attr_btn)
	_home_deck_btn = _make_menu_btn("卡牌编组", _on_home_deck_view_pressed)
	box.add_child(_home_deck_btn)
	_home_out_btn = _make_menu_btn("外出", _on_home_out_view_pressed)
	box.add_child(_home_out_btn)
	_rest_btn = _make_menu_btn("休息（恢复体力）", _on_rest_pressed)
	box.add_child(_rest_btn)


func _build_deck_editor(parent: PanelContainer) -> void:
	var v := VBoxContainer.new()
	v.add_theme_constant_override("separation", 8)
	parent.add_child(v)

	var head := HBoxContainer.new()
	v.add_child(head)
	var title := Label.new()
	title.text = "卡牌编组（10~15）"
	title.add_theme_font_size_override("font_size", 20)
	head.add_child(title)
	var head_spacer := Control.new()
	head_spacer.size_flags_horizontal = Control.SIZE_EXPAND_FILL
	head.add_child(head_spacer)
	var back_btn := _make_back_home_btn(_on_home_back_pressed)
	head.add_child(back_btn)

	_deck_hint_label = Label.new()
	_deck_hint_label.text = "保存后下次新战斗生效。"
	_deck_hint_label.add_theme_color_override("font_color", Color(0.74, 0.86, 1.0, 1.0))
	v.add_child(_deck_hint_label)

	var row := HBoxContainer.new()
	row.add_theme_constant_override("separation", 10)
	row.size_flags_vertical = Control.SIZE_EXPAND_FILL
	v.add_child(row)

	var pool_panel := PanelContainer.new()
	pool_panel.custom_minimum_size = Vector2(380, 0)
	_style_panel(pool_panel, Color(0.10, 0.16, 0.26, 0.96), Color(0.28, 0.40, 0.58, 0.92), 8)
	row.add_child(pool_panel)
	var pbox := VBoxContainer.new()
	pool_panel.add_child(pbox)
	var ptitle := Label.new()
	ptitle.text = "卡池（堆叠）"
	pbox.add_child(ptitle)
	_pool_list = ItemList.new()
	_pool_list.select_mode = ItemList.SELECT_SINGLE
	_pool_list.allow_reselect = true
	_pool_list.size_flags_vertical = Control.SIZE_EXPAND_FILL
	_pool_list.item_selected.connect(_on_pool_item_selected)
	pbox.add_child(_pool_list)

	var middle := VBoxContainer.new()
	middle.custom_minimum_size = Vector2(310, 0)
	middle.add_theme_constant_override("separation", 8)
	row.add_child(middle)
	var detail_head := HBoxContainer.new()
	detail_head.add_theme_constant_override("separation", 6)
	middle.add_child(detail_head)
	var detail_title := Label.new()
	detail_title.text = "卡牌详情"
	detail_title.size_flags_horizontal = Control.SIZE_EXPAND_FILL
	detail_head.add_child(detail_title)
	_direction_sort_btn = _make_action_btn("⇅ 多空", _on_sort_direction_pressed)
	_direction_sort_btn.custom_minimum_size = Vector2(84, 32)
	detail_head.add_child(_direction_sort_btn)
	_level_sort_btn = _make_action_btn("★ 等级", _on_sort_level_pressed)
	_level_sort_btn.custom_minimum_size = Vector2(84, 32)
	detail_head.add_child(_level_sort_btn)
	_deck_detail_panel = PanelContainer.new()
	_deck_detail_panel.size_flags_vertical = Control.SIZE_EXPAND_FILL
	_style_panel(_deck_detail_panel, Color(0.10, 0.15, 0.24, 0.96), Color(0.44, 0.60, 0.80, 0.92), 8)
	middle.add_child(_deck_detail_panel)
	var detail_box := VBoxContainer.new()
	detail_box.add_theme_constant_override("separation", 6)
	_deck_detail_panel.add_child(detail_box)
	_deck_detail_title = Label.new()
	_deck_detail_title.text = "请选择卡池或上场卡组中的卡牌"
	_deck_detail_title.add_theme_font_size_override("font_size", 17)
	detail_box.add_child(_deck_detail_title)
	_deck_detail_body = RichTextLabel.new()
	_deck_detail_body.fit_content = false
	_deck_detail_body.scroll_active = true
	_deck_detail_body.size_flags_vertical = Control.SIZE_EXPAND_FILL
	_deck_detail_body.custom_minimum_size = Vector2(0, 220)
	detail_box.add_child(_deck_detail_body)
	_add_card_btn = _make_action_btn("添加 ->", _on_add_card_pressed)
	middle.add_child(_add_card_btn)
	_remove_card_btn = _make_action_btn("<- 移除", _on_remove_card_pressed)
	middle.add_child(_remove_card_btn)
	var drag_tip := Label.new()
	drag_tip.text = "支持拖拽：卡池 -> 上场；上场 -> 卡池。"
	drag_tip.autowrap_mode = TextServer.AUTOWRAP_WORD_SMART
	drag_tip.add_theme_color_override("font_color", Color(0.69, 0.82, 0.98, 1.0))
	middle.add_child(drag_tip)

	var deck_panel := PanelContainer.new()
	deck_panel.custom_minimum_size = Vector2(380, 0)
	_style_panel(deck_panel, Color(0.10, 0.16, 0.26, 0.96), Color(0.28, 0.40, 0.58, 0.92), 8)
	row.add_child(deck_panel)
	var dbox := VBoxContainer.new()
	deck_panel.add_child(dbox)
	var dtitle := Label.new()
	dtitle.text = "上场卡组（0）"
	dbox.add_child(dtitle)
	_deck_list_title_label = dtitle
	_deck_list = ItemList.new()
	_deck_list.select_mode = ItemList.SELECT_SINGLE
	_deck_list.allow_reselect = true
	_deck_list.size_flags_vertical = Control.SIZE_EXPAND_FILL
	_deck_list.item_selected.connect(_on_deck_item_selected)
	dbox.add_child(_deck_list)

	_pool_list.set_drag_forwarding(_pool_get_drag_data, _pool_can_drop_data, _pool_drop_data)
	_deck_list.set_drag_forwarding(_deck_get_drag_data, _deck_can_drop_data, _deck_drop_data)

func _build_outing_panel(parent: PanelContainer) -> void:
	var root := Control.new()
	root.set_anchors_preset(Control.PRESET_FULL_RECT)
	parent.add_child(root)
	_outing_map_root = root

	var assets := _try_load_outing_map_assets()
	var has_real_base := assets.get("base", null) is Texture2D

	var map_shell := PanelContainer.new()
	map_shell.set_anchors_preset(Control.PRESET_FULL_RECT)
	map_shell.mouse_filter = Control.MOUSE_FILTER_PASS
	_style_panel(map_shell, Color(0.03, 0.06, 0.10, 0.95), Color(0.77, 0.66, 0.39, 0.92), 10)
	root.add_child(map_shell)

	var map_host := Control.new()
	map_host.set_anchors_preset(Control.PRESET_FULL_RECT)
	map_host.mouse_filter = Control.MOUSE_FILTER_PASS
	map_shell.add_child(map_host)
	_outing_map_canvas = map_host

	var map_bg := TextureRect.new()
	map_bg.set_anchors_preset(Control.PRESET_FULL_RECT)
	map_bg.stretch_mode = TextureRect.STRETCH_KEEP_ASPECT_COVERED
	map_bg.mouse_filter = Control.MOUSE_FILTER_IGNORE
	map_bg.texture = assets.get("base", null)
	if map_bg.texture == null:
		map_bg.texture = _make_city_map_texture()
	map_host.add_child(map_bg)

	var tint_bg: Variant = assets.get("tint", null)
	if tint_bg is Texture2D:
		var tint_tex := TextureRect.new()
		tint_tex.set_anchors_preset(Control.PRESET_FULL_RECT)
		tint_tex.stretch_mode = TextureRect.STRETCH_KEEP_ASPECT_COVERED
		tint_tex.texture = tint_bg
		tint_tex.modulate = Color(1, 1, 1, 0.55)
		tint_tex.mouse_filter = Control.MOUSE_FILTER_IGNORE
		map_host.add_child(tint_tex)

	var dark_veil := ColorRect.new()
	dark_veil.set_anchors_preset(Control.PRESET_FULL_RECT)
	dark_veil.color = Color(0.02, 0.05, 0.09, 0.12) if has_real_base else Color(0.02, 0.05, 0.09, 0.38)
	dark_veil.mouse_filter = Control.MOUSE_FILTER_IGNORE
	map_host.add_child(dark_veil)

	if not has_real_base:
		var top_grad := ColorRect.new()
		top_grad.anchor_left = 0.0
		top_grad.anchor_top = 0.0
		top_grad.anchor_right = 1.0
		top_grad.anchor_bottom = 0.30
		top_grad.color = Color(0.02, 0.05, 0.10, 0.34)
		top_grad.mouse_filter = Control.MOUSE_FILTER_IGNORE
		map_host.add_child(top_grad)

		var bottom_grad := ColorRect.new()
		bottom_grad.anchor_left = 0.0
		bottom_grad.anchor_top = 0.72
		bottom_grad.anchor_right = 1.0
		bottom_grad.anchor_bottom = 1.0
		bottom_grad.color = Color(0.01, 0.04, 0.08, 0.28)
		bottom_grad.mouse_filter = Control.MOUSE_FILTER_IGNORE
		map_host.add_child(bottom_grad)

	var routes_tex := TextureRect.new()
	routes_tex.set_anchors_preset(Control.PRESET_FULL_RECT)
	routes_tex.stretch_mode = TextureRect.STRETCH_KEEP_ASPECT_COVERED
	routes_tex.texture = assets.get("routes", null)
	routes_tex.modulate = Color(1, 1, 1, 0.85)
	routes_tex.mouse_filter = Control.MOUSE_FILTER_IGNORE
	map_host.add_child(routes_tex)

	var fog_tex: Variant = assets.get("fog_top", null)
	if fog_tex is Texture2D:
		var fog_rect := TextureRect.new()
		fog_rect.set_anchors_preset(Control.PRESET_FULL_RECT)
		fog_rect.stretch_mode = TextureRect.STRETCH_KEEP_ASPECT_COVERED
		fog_rect.texture = fog_tex
		fog_rect.modulate = Color(1, 1, 1, 0.66)
		fog_rect.mouse_filter = Control.MOUSE_FILTER_IGNORE
		map_host.add_child(fog_rect)

	var vignette_tex: Variant = assets.get("vignette", null)
	if vignette_tex is Texture2D:
		var vig_rect := TextureRect.new()
		vig_rect.set_anchors_preset(Control.PRESET_FULL_RECT)
		vig_rect.stretch_mode = TextureRect.STRETCH_KEEP_ASPECT_COVERED
		vig_rect.texture = vignette_tex
		vig_rect.modulate = Color(1, 1, 1, 0.82)
		vig_rect.mouse_filter = Control.MOUSE_FILTER_IGNORE
		map_host.add_child(vig_rect)

	var poi_layer := Control.new()
	poi_layer.set_anchors_preset(Control.PRESET_FULL_RECT)
	poi_layer.mouse_filter = Control.MOUSE_FILTER_PASS
	map_host.add_child(poi_layer)
	_outing_route_layer = poi_layer

	_outing_map_buttons.clear()
	_outing_poi_buttons.clear()
	_outing_poi_glows.clear()
	_move_home_btn = null
	_move_assoc_btn = null
	for poi_v in OUTING_POI_DEFS:
		if not (poi_v is Dictionary):
			continue
		var poi: Dictionary = poi_v
		var poi_id := str(poi.get("id", ""))
		var label := str(poi.get("label", poi_id))
		var open_state := str(poi.get("open_state", "placeholder"))
		var cb := _on_locked_location_pressed.bind(label)
		if poi_id == "home":
			cb = _on_move_home_pressed
		elif poi_id == "association":
			cb = _on_move_assoc_pressed
		var btn := _make_city_node_btn(label, cb, open_state)
		btn.set_meta("poi_id", poi_id)
		btn.set_meta("poi_label", label)
		btn.set_meta("poi_open_state", open_state)
		btn.set_meta("poi_location_code", str(poi.get("location_code", "")))
		btn.set_meta("poi_stamina_cost", int(poi.get("stamina_cost", 10)))
		var x_ratio := float(poi.get("x", 0.5))
		var y_ratio := float(poi.get("y", 0.5))
		var glow := _make_city_node_glow(assets.get("poi_glow", null))
		_place_city_glow(poi_layer, glow, x_ratio, y_ratio)
		_place_city_node(poi_layer, btn, x_ratio, y_ratio)
		_outing_poi_glows[poi_id] = glow
		_outing_poi_buttons[poi_id] = btn
		_outing_map_buttons.append(btn)
		if poi_id == "home":
			_move_home_btn = btn
		elif poi_id == "association":
			_move_assoc_btn = btn

	var top_left_panel := PanelContainer.new()
	top_left_panel.anchor_left = 0.02
	top_left_panel.anchor_top = 0.02
	top_left_panel.anchor_right = 0.33
	top_left_panel.anchor_bottom = 0.24
	_style_panel(top_left_panel, Color(0.05, 0.10, 0.17, 0.60), Color(0.80, 0.69, 0.41, 0.88), 10)
	root.add_child(top_left_panel)

	var info_box := VBoxContainer.new()
	info_box.add_theme_constant_override("separation", 6)
	top_left_panel.add_child(info_box)

	var info_title := Label.new()
	info_title.text = "陆家嘴城市版图"
	info_title.add_theme_font_size_override("font_size", 16)
	info_title.add_theme_color_override("font_color", Color(0.96, 0.93, 0.81, 1.0))
	info_box.add_child(info_title)

	_outing_info_label = Label.new()
	_outing_info_label.autowrap_mode = TextServer.AUTOWRAP_WORD_SMART
	_outing_info_label.add_theme_font_size_override("font_size", 14)
	_outing_info_label.add_theme_color_override("font_color", Color(0.90, 0.95, 0.99, 0.96))
	_outing_info_label.text = "当前地点：住宅"
	info_box.add_child(_outing_info_label)

	_outing_hint_label = Label.new()
	_outing_hint_label.autowrap_mode = TextServer.AUTOWRAP_WORD_SMART
	_outing_hint_label.add_theme_font_size_override("font_size", 12)
	_outing_hint_label.add_theme_color_override("font_color", Color(0.78, 0.87, 0.97, 0.92))
	_outing_hint_label.text = "非住宅地点移动体力 -10"
	info_box.add_child(_outing_hint_label)

	var top_right_panel := PanelContainer.new()
	top_right_panel.anchor_left = 0.78
	top_right_panel.anchor_top = 0.80
	top_right_panel.anchor_right = 0.98
	top_right_panel.anchor_bottom = 0.98
	_style_panel(top_right_panel, Color(0.04, 0.09, 0.15, 0.84), Color(0.71, 0.63, 0.39, 0.94), 10)
	root.add_child(top_right_panel)

	var action_box := VBoxContainer.new()
	action_box.add_theme_constant_override("separation", 8)
	top_right_panel.add_child(action_box)

	_outing_back_btn = _make_back_home_btn(_on_home_back_pressed)
	_outing_back_btn.custom_minimum_size = Vector2(0, 30)
	_outing_back_btn.add_theme_font_size_override("font_size", 13)
	action_box.add_child(_outing_back_btn)

	_battle_btn = _make_menu_btn("进入卡牌战斗测试", _on_start_battle_pressed)
	_battle_btn.custom_minimum_size = Vector2(0, 34)
	_battle_btn.add_theme_font_size_override("font_size", 15)
	action_box.add_child(_battle_btn)
	_outing_battle_fab = _battle_btn

	var bottom_badge := PanelContainer.new()
	bottom_badge.anchor_left = 0.02
	bottom_badge.anchor_top = 0.90
	bottom_badge.anchor_right = 0.36
	bottom_badge.anchor_bottom = 0.98
	_style_panel(bottom_badge, Color(0.03, 0.07, 0.12, 0.80), Color(0.34, 0.58, 0.82, 0.92), 10)
	root.add_child(bottom_badge)

	var badge_label := Label.new()
	badge_label.autowrap_mode = TextServer.AUTOWRAP_WORD_SMART
	badge_label.add_theme_font_size_override("font_size", 12)
	badge_label.add_theme_color_override("font_color", Color(0.80, 0.90, 0.99, 1.0))
	badge_label.text = "点击地图地点进入场景。未开放地点仅显示占位提示。"
	bottom_badge.add_child(badge_label)


func _build_association_scene(parent: Control) -> void:
	var bg := TextureRect.new()
	bg.set_anchors_preset(Control.PRESET_FULL_RECT)
	bg.stretch_mode = TextureRect.STRETCH_SCALE
	bg.texture = _make_scene_bg_texture("association")
	parent.add_child(bg)

	var veil := ColorRect.new()
	veil.set_anchors_preset(Control.PRESET_FULL_RECT)
	veil.color = Color(0.02, 0.05, 0.10, 0.30)
	parent.add_child(veil)

	var scene_title := Label.new()
	scene_title.text = "基金业协会｜监管大厅"
	scene_title.anchor_left = 0.03
	scene_title.anchor_top = 0.03
	scene_title.anchor_right = 0.55
	scene_title.anchor_bottom = 0.10
	scene_title.add_theme_font_size_override("font_size", 28)
	scene_title.add_theme_color_override("font_color", Color(0.94, 0.97, 1.0, 1.0))
	parent.add_child(scene_title)

	var info_panel := PanelContainer.new()
	info_panel.anchor_left = 0.05
	info_panel.anchor_top = 0.20
	info_panel.anchor_right = 0.70
	info_panel.anchor_bottom = 0.88
	_style_panel(info_panel, Color(0.06, 0.10, 0.17, 0.84), Color(0.39, 0.56, 0.78, 0.94), 12)
	parent.add_child(info_panel)
	_assoc_desc_label = Label.new()
	_assoc_desc_label.autowrap_mode = TextServer.AUTOWRAP_WORD_SMART
	_assoc_desc_label.size_flags_vertical = Control.SIZE_EXPAND_FILL
	_assoc_desc_label.add_theme_font_size_override("font_size", 18)
	_assoc_desc_label.text = "本版仅提供场景占位，用于验证地点切换与体力消耗。"
	info_panel.add_child(_assoc_desc_label)

	var menu_panel := PanelContainer.new()
	menu_panel.anchor_left = 0.74
	menu_panel.anchor_top = 0.20
	menu_panel.anchor_right = 0.97
	menu_panel.anchor_bottom = 0.88
	_style_panel(menu_panel, Color(0.04, 0.08, 0.14, 0.82), Color(0.74, 0.63, 0.38, 0.96), 12)
	parent.add_child(menu_panel)

	var mbox := VBoxContainer.new()
	mbox.add_theme_constant_override("separation", 10)
	menu_panel.add_child(mbox)
	var mtitle := Label.new()
	mtitle.text = "协会菜单"
	mtitle.add_theme_font_size_override("font_size", 20)
	mtitle.add_theme_color_override("font_color", Color(0.96, 0.90, 0.72, 1.0))
	mbox.add_child(mtitle)
	_assoc_back_home_btn = _make_menu_btn("返回住宅", _on_move_home_pressed)
	mbox.add_child(_assoc_back_home_btn)
	_assoc_battle_btn = _make_menu_btn("战斗测试", _on_start_battle_pressed)
	mbox.add_child(_assoc_battle_btn)

	var tip := Label.new()
	tip.text = "后续会在此加入：\n1) 申请建立私募基金\n2) 备案成立私募产品"
	tip.autowrap_mode = TextServer.AUTOWRAP_WORD_SMART
	tip.size_flags_vertical = Control.SIZE_EXPAND_FILL
	tip.add_theme_color_override("font_color", Color(0.78, 0.89, 0.98, 1.0))
	mbox.add_child(tip)


func _make_scene_bg_texture(kind: String) -> Texture2D:
	var w := 1440
	var h := 900
	var image := Image.create(w, h, false, Image.FORMAT_RGBA8)
	if kind == "association":
		_paint_vertical_gradient(image, Color(0.04, 0.08, 0.14, 1.0), Color(0.13, 0.17, 0.25, 1.0))
		_draw_rect(image, Rect2i(0, 0, w, 120), Color(0.08, 0.11, 0.19, 1.0))
		_draw_rect(image, Rect2i(0, 760, w, 140), Color(0.10, 0.12, 0.18, 1.0))
		for i in range(11):
			var x := 70 + i * 126
			_draw_rect(image, Rect2i(x, 155, 58, 610), Color(0.16, 0.20, 0.29, 0.95))
			_draw_rect(image, Rect2i(x + 8, 160, 42, 590), Color(0.09, 0.12, 0.18, 0.40))
		_draw_rect(image, Rect2i(130, 210, 1180, 34), Color(0.73, 0.63, 0.37, 0.36))
		_draw_rect(image, Rect2i(130, 330, 1180, 34), Color(0.73, 0.63, 0.37, 0.32))
		_draw_rect(image, Rect2i(130, 450, 1180, 34), Color(0.73, 0.63, 0.37, 0.28))
		_draw_rect(image, Rect2i(130, 570, 1180, 34), Color(0.73, 0.63, 0.37, 0.26))
		for i in range(24):
			_draw_rect(image, Rect2i(18 + i * 60, 792, 28, 64), Color(0.20, 0.23, 0.31, 0.92))
	else:
		_paint_vertical_gradient(image, Color(0.03, 0.07, 0.12, 1.0), Color(0.12, 0.17, 0.25, 1.0))
		_draw_rect(image, Rect2i(0, 0, w, 110), Color(0.06, 0.11, 0.18, 1.0))
		_draw_rect(image, Rect2i(0, 620, w, 280), Color(0.08, 0.11, 0.16, 1.0))
		_draw_rect(image, Rect2i(90, 120, 760, 400), Color(0.10, 0.16, 0.24, 0.28))
		_draw_rect(image, Rect2i(110, 140, 720, 360), Color(0.07, 0.12, 0.19, 0.32))
		for i in range(18):
			var bw := 34 + int((i % 6) * 12)
			var bh := 150 + int((i % 5) * 56)
			var bx := 20 + i * 80
			var by := 620 - bh
			_draw_rect(image, Rect2i(bx, by, bw, bh), Color(0.10, 0.15, 0.22, 0.95))
			for wy in range(by + 12, by + bh - 10, 18):
				for wx in range(bx + 6, bx + bw - 6, 12):
					if ((wx + wy) % 2) == 0:
						_draw_rect(image, Rect2i(wx, wy, 5, 7), Color(0.83, 0.74, 0.40, 0.86))
		_draw_rect(image, Rect2i(940, 120, 400, 480), Color(0.05, 0.09, 0.15, 0.56))
		for i in range(16):
			_draw_rect(image, Rect2i(960, 150 + i * 24, 360, 2), Color(0.78, 0.67, 0.40, 0.20))
		_draw_rect(image, Rect2i(960, 590, 360, 24), Color(0.74, 0.63, 0.36, 0.42))

	return ImageTexture.create_from_image(image)


func _paint_vertical_gradient(image: Image, c_top: Color, c_bottom: Color) -> void:
	var w := image.get_width()
	var h := image.get_height()
	for y in range(h):
		var t := float(y) / float(max(1, h - 1))
		var c := Color(
			lerpf(c_top.r, c_bottom.r, t),
			lerpf(c_top.g, c_bottom.g, t),
			lerpf(c_top.b, c_bottom.b, t),
			lerpf(c_top.a, c_bottom.a, t)
		)
		for x in range(w):
			image.set_pixel(x, y, c)


func _draw_rect(image: Image, rect: Rect2i, color: Color) -> void:
	var x0 := clampi(rect.position.x, 0, image.get_width())
	var y0 := clampi(rect.position.y, 0, image.get_height())
	var x1 := clampi(rect.position.x + rect.size.x, 0, image.get_width())
	var y1 := clampi(rect.position.y + rect.size.y, 0, image.get_height())
	for y in range(y0, y1):
		for x in range(x0, x1):
			image.set_pixel(x, y, color)


func _style_panel(panel: PanelContainer, bg_color: Color, border_color: Color, radius: int = 10) -> void:
	var sb := StyleBoxFlat.new()
	sb.bg_color = bg_color
	sb.border_color = border_color
	sb.set_border_width_all(1)
	sb.set_corner_radius_all(radius)
	sb.content_margin_left = 10
	sb.content_margin_right = 10
	sb.content_margin_top = 8
	sb.content_margin_bottom = 8
	panel.add_theme_stylebox_override("panel", sb)


func _make_action_btn(text: String, cb: Callable) -> Button:
	var btn := Button.new()
	btn.text = text
	btn.custom_minimum_size = Vector2(0, 34)
	btn.pressed.connect(cb)
	return btn


func _make_menu_btn(text: String, cb: Callable) -> Button:
	var btn := Button.new()
	btn.text = text
	btn.custom_minimum_size = Vector2(0, 48)
	btn.pressed.connect(cb)

	var normal := StyleBoxFlat.new()
	normal.bg_color = Color(0.14, 0.22, 0.34, 0.94)
	normal.border_color = Color(0.82, 0.70, 0.41, 0.98)
	normal.set_border_width_all(2)
	normal.set_corner_radius_all(8)
	normal.content_margin_left = 12
	normal.content_margin_right = 12
	normal.content_margin_top = 8
	normal.content_margin_bottom = 8
	btn.add_theme_stylebox_override("normal", normal)

	var hover := normal.duplicate()
	hover.bg_color = Color(0.18, 0.27, 0.40, 0.98)
	hover.border_color = Color(0.91, 0.79, 0.49, 1.0)
	btn.add_theme_stylebox_override("hover", hover)

	var pressed := normal.duplicate()
	pressed.bg_color = Color(0.11, 0.19, 0.30, 0.98)
	pressed.border_color = Color(0.86, 0.75, 0.46, 1.0)
	btn.add_theme_stylebox_override("pressed", pressed)

	btn.add_theme_color_override("font_color", Color(0.96, 0.96, 0.93, 1.0))
	btn.add_theme_font_size_override("font_size", 20)
	return btn


func _make_back_home_btn(cb: Callable) -> Button:
	var btn := Button.new()
	btn.text = "返回住宅主页"
	btn.custom_minimum_size = Vector2(188, 40)
	btn.pressed.connect(cb)
	var normal := StyleBoxFlat.new()
	normal.set_corner_radius_all(9)
	normal.set_border_width_all(2)
	normal.bg_color = Color(0.17, 0.24, 0.35, 0.96)
	normal.border_color = Color(0.86, 0.74, 0.44, 0.98)
	var hover := normal.duplicate()
	hover.bg_color = Color(0.21, 0.30, 0.42, 0.98)
	hover.border_color = Color(0.95, 0.82, 0.52, 1.0)
	var pressed := normal.duplicate()
	pressed.bg_color = Color(0.12, 0.18, 0.28, 0.98)
	pressed.border_color = Color(0.82, 0.70, 0.40, 1.0)
	btn.add_theme_stylebox_override("normal", normal)
	btn.add_theme_stylebox_override("hover", hover)
	btn.add_theme_stylebox_override("pressed", pressed)
	btn.add_theme_color_override("font_color", Color(0.97, 0.94, 0.83, 1.0))
	btn.add_theme_font_size_override("font_size", 16)
	return btn


func _make_city_node_glow(glow_tex: Variant) -> CanvasItem:
	if glow_tex is Texture2D:
		var t := TextureRect.new()
		t.texture = glow_tex
		t.expand_mode = TextureRect.EXPAND_IGNORE_SIZE
		t.stretch_mode = TextureRect.STRETCH_KEEP_ASPECT_CENTERED
		t.mouse_filter = Control.MOUSE_FILTER_IGNORE
		t.modulate = Color(0.92, 0.86, 0.58, 0.0)
		return t
	var ring := ColorRect.new()
	ring.mouse_filter = Control.MOUSE_FILTER_IGNORE
	ring.color = Color(0.93, 0.82, 0.49, 0.0)
	return ring


func _place_city_glow(parent: Control, glow: CanvasItem, x_ratio: float, y_ratio: float) -> void:
	if glow == null:
		return
	if glow is Control:
		var c := glow as Control
		c.anchor_left = x_ratio
		c.anchor_right = x_ratio
		c.anchor_top = y_ratio
		c.anchor_bottom = y_ratio
		c.offset_left = -92
		c.offset_right = 92
		c.offset_top = -44
		c.offset_bottom = 44
	parent.add_child(glow)


func _make_city_node_btn(text: String, cb: Callable, open_state: String) -> Button:
	var btn := Button.new()
	btn.text = text
	btn.custom_minimum_size = Vector2(164, 52)
	btn.pressed.connect(cb)
	btn.clip_text = true
	btn.size_flags_horizontal = Control.SIZE_SHRINK_CENTER
	var unlocked := open_state == "open"

	var normal := StyleBoxFlat.new()
	normal.set_corner_radius_all(9)
	normal.set_border_width_all(2)
	normal.bg_color = Color(0.06, 0.13, 0.21, 0.92) if unlocked else Color(0.12, 0.14, 0.19, 0.88)
	normal.border_color = Color(0.83, 0.72, 0.44, 0.94) if unlocked else Color(0.52, 0.59, 0.68, 0.85)
	normal.content_margin_left = 10
	normal.content_margin_right = 10
	normal.content_margin_top = 8
	normal.content_margin_bottom = 8
	btn.add_theme_stylebox_override("normal", normal)

	var hover := normal.duplicate()
	hover.bg_color = Color(0.08, 0.18, 0.28, 0.97) if unlocked else Color(0.16, 0.18, 0.24, 0.94)
	hover.border_color = Color(0.95, 0.84, 0.54, 1.0) if unlocked else Color(0.70, 0.78, 0.88, 0.96)
	btn.add_theme_stylebox_override("hover", hover)

	var pressed := normal.duplicate()
	pressed.bg_color = Color(0.04, 0.11, 0.18, 0.98) if unlocked else Color(0.11, 0.13, 0.17, 0.95)
	pressed.border_color = Color(0.88, 0.77, 0.47, 1.0) if unlocked else hover.border_color
	btn.add_theme_stylebox_override("pressed", pressed)

	btn.add_theme_color_override("font_color", Color(0.96, 0.97, 0.94, 1.0) if unlocked else Color(0.84, 0.88, 0.95, 0.92))
	btn.add_theme_font_size_override("font_size", 13)
	return btn


func _place_city_node(parent: Control, btn: Button, x_ratio: float, y_ratio: float) -> void:
	btn.anchor_left = x_ratio
	btn.anchor_right = x_ratio
	btn.anchor_top = y_ratio
	btn.anchor_bottom = y_ratio
	btn.offset_left = -82
	btn.offset_right = 82
	btn.offset_top = -26
	btn.offset_bottom = 26
	parent.add_child(btn)


func _outing_poi_def_by_id(poi_id: String) -> Dictionary:
	for poi_v in OUTING_POI_DEFS:
		if poi_v is Dictionary and str((poi_v as Dictionary).get("id", "")) == poi_id:
			return poi_v
	return {}


func _outing_poi_subtitle(open_state: String, is_current: bool, stamina_cost: int) -> String:
	if is_current:
		return "当前地点"
	if open_state != "open":
		return "未开放"
	return "体力-%d" % stamina_cost


func _style_city_node_runtime(btn: Button, open_state: String, is_current: bool) -> void:
	if btn == null:
		return
	var unlocked := open_state == "open"
	var normal := (btn.get_theme_stylebox("normal") as StyleBoxFlat)
	if normal != null:
		var n := normal.duplicate()
		if is_current:
			n.bg_color = Color(0.10, 0.20, 0.33, 0.96)
			n.border_color = Color(0.98, 0.86, 0.56, 1.0)
			n.set_border_width_all(2)
		elif unlocked:
			n.bg_color = Color(0.06, 0.13, 0.21, 0.92)
			n.border_color = Color(0.83, 0.72, 0.44, 0.94)
		else:
			n.bg_color = Color(0.12, 0.14, 0.19, 0.88)
			n.border_color = Color(0.52, 0.59, 0.68, 0.85)
		btn.add_theme_stylebox_override("normal", n)
	var hover := (btn.get_theme_stylebox("hover") as StyleBoxFlat)
	if hover != null:
		var h := hover.duplicate()
		if is_current:
			h.border_color = Color(1.0, 0.90, 0.62, 1.0)
			h.bg_color = Color(0.12, 0.23, 0.36, 0.98)
		btn.add_theme_stylebox_override("hover", h)
	var pressed := (btn.get_theme_stylebox("pressed") as StyleBoxFlat)
	if pressed != null:
		var p := pressed.duplicate()
		if is_current:
			p.border_color = Color(0.96, 0.85, 0.56, 1.0)
			p.bg_color = Color(0.08, 0.17, 0.28, 0.98)
		btn.add_theme_stylebox_override("pressed", p)
	btn.add_theme_color_override("font_color", Color(0.98, 0.95, 0.84, 1.0) if is_current else (Color(0.96, 0.97, 0.94, 1.0) if unlocked else Color(0.84, 0.88, 0.95, 0.92)))
	var glow_v: Variant = _outing_poi_glows.get(str(btn.get_meta("poi_id", "")), null)
	if glow_v != null and glow_v is CanvasItem:
		var glow := glow_v as CanvasItem
		glow.modulate = Color(0.95, 0.83, 0.48, 0.36) if is_current else Color(1, 1, 1, 0.0)


func _outing_refresh_poi_buttons() -> void:
	var location := str(_state.get("location", "home"))
	var ended := str(_state.get("status", "")) == "ended"
	for poi_v in OUTING_POI_DEFS:
		if not (poi_v is Dictionary):
			continue
		var poi := poi_v as Dictionary
		var poi_id := str(poi.get("id", ""))
		var btn_v: Variant = _outing_poi_buttons.get(poi_id, null)
		if not (btn_v is Button):
			continue
		var btn := btn_v as Button
		var open_state := str(poi.get("open_state", "placeholder"))
		var loc_code := str(poi.get("location_code", ""))
		var is_current := (loc_code != "" and loc_code == location)
		var stamina_cost := int(poi.get("stamina_cost", 10))
		var subtitle := _outing_poi_subtitle(open_state, is_current, stamina_cost)
		btn.text = "%s\n%s" % [str(poi.get("label", poi_id)), subtitle]
		var can_press := _actions_enabled and (not ended)
		if open_state == "open":
			if loc_code == "home":
				btn.disabled = not can_press
			else:
				btn.disabled = not can_press
		else:
			btn.disabled = not can_press
		btn.modulate = Color(1, 1, 1, 1.0) if (is_current or open_state == "open") else Color(0.95, 0.96, 1.0, 0.92)
		_style_city_node_runtime(btn, open_state, is_current)


func _make_home_portrait_texture() -> Texture2D:
	var w := 420
	var h := 560
	var image := Image.create(w, h, false, Image.FORMAT_RGBA8)
	_paint_vertical_gradient(image, Color(0.06, 0.10, 0.16, 1.0), Color(0.11, 0.15, 0.22, 1.0))
	_draw_rect(image, Rect2i(0, 430, w, 130), Color(0.08, 0.10, 0.15, 1.0))
	_draw_rect(image, Rect2i(34, 68, 352, 452), Color(0.04, 0.07, 0.12, 0.36))

	var suit := Color(0.15, 0.23, 0.35, 1.0)
	var shirt := Color(0.86, 0.90, 0.96, 1.0)
	var skin := Color(0.83, 0.73, 0.62, 1.0)
	var tie := Color(0.70, 0.16, 0.18, 1.0)
	var gold := Color(0.82, 0.72, 0.43, 1.0)

	_draw_rect(image, Rect2i(148, 196, 124, 188), suit)
	_draw_rect(image, Rect2i(170, 206, 80, 128), shirt)
	_draw_rect(image, Rect2i(196, 234, 28, 78), tie)
	_draw_rect(image, Rect2i(120, 230, 50, 146), suit)
	_draw_rect(image, Rect2i(250, 230, 50, 146), suit)
	_draw_rect(image, Rect2i(168, 128, 84, 74), skin)
	_draw_rect(image, Rect2i(182, 184, 56, 22), skin)
	_draw_rect(image, Rect2i(160, 104, 100, 30), Color(0.18, 0.14, 0.12, 1.0))
	_draw_rect(image, Rect2i(146, 118, 20, 42), Color(0.18, 0.14, 0.12, 1.0))
	_draw_rect(image, Rect2i(252, 118, 20, 42), Color(0.18, 0.14, 0.12, 1.0))
	_draw_rect(image, Rect2i(184, 146, 10, 6), Color(0.10, 0.08, 0.07, 1.0))
	_draw_rect(image, Rect2i(224, 146, 10, 6), Color(0.10, 0.08, 0.07, 1.0))
	_draw_rect(image, Rect2i(196, 170, 28, 5), Color(0.44, 0.26, 0.22, 1.0))

	_draw_rect(image, Rect2i(266, 250, 108, 132), Color(0.12, 0.20, 0.31, 1.0))
	_draw_rect(image, Rect2i(274, 260, 92, 114), Color(0.05, 0.09, 0.15, 1.0))
	_draw_rect(image, Rect2i(274, 260, 92, 6), gold)
	_draw_rect(image, Rect2i(274, 368, 92, 6), gold)
	_draw_rect(image, Rect2i(274, 266, 6, 102), gold)
	_draw_rect(image, Rect2i(360, 266, 6, 102), gold)
	_draw_rect(image, Rect2i(290, 302, 60, 38), Color(0.14, 0.39, 0.30, 1.0))
	_draw_rect(image, Rect2i(300, 314, 40, 14), Color(0.80, 0.90, 0.98, 1.0))

	for i in range(8):
		_draw_rect(image, Rect2i(48 + i * 44, 470 - i * 6, 20, 70 + i * 6), Color(0.12, 0.18, 0.27, 0.92))
		_draw_rect(image, Rect2i(52 + i * 44, 484 - i * 6, 10, 8), Color(0.84, 0.75, 0.43, 0.86))
	return ImageTexture.create_from_image(image)


func _load_outing_map_asset(path: String) -> Texture2D:
	if not ResourceLoader.exists(path):
		return null
	var res_v: Variant = load(path)
	if res_v is Texture2D:
		return res_v
	return null


func _try_load_outing_map_assets() -> Dictionary:
	if not _outing_map_assets.is_empty():
		return _outing_map_assets
	var base := "res://assets/ui/map/lujiazui/"
	var assets := {
		"base": _load_outing_map_asset(base + "bg_lujiazui_base.png"),
		"tint": _load_outing_map_asset(base + "bg_lujiazui_tint.png"),
		"routes": _load_outing_map_asset(base + "overlay_routes.png"),
		"fog_top": _load_outing_map_asset(base + "overlay_fog_top.png"),
		"vignette": _load_outing_map_asset(base + "overlay_vignette.png"),
		"scanline": _load_outing_map_asset(base + "overlay_scanline.png"),
		"poi_glow": _load_outing_map_asset(base + "poi_glow.png"),
		"panel_glass_corner": _load_outing_map_asset(base + "panel_glass_corner.png"),
	}
	# 只有在没有真实底图时，才使用程序化路线叠层 fallback。
	# 一旦接入真实陆家嘴底图，若未提供 overlay_routes.png，就不叠加这些线条。
	if assets["routes"] == null and assets["base"] == null:
		assets["routes"] = _make_city_routes_overlay_texture()
	_outing_map_assets = assets
	return _outing_map_assets


func _make_city_routes_overlay_texture() -> Texture2D:
	var w := 1180
	var h := 760
	var image := Image.create(w, h, false, Image.FORMAT_RGBA8)
	image.fill(Color(0, 0, 0, 0))
	var route_gold := Color(0.86, 0.75, 0.49, 0.22)
	var route_blue := Color(0.48, 0.84, 0.96, 0.16)
	for i in range(5):
		_draw_rect(image, Rect2i(80 + i * 125, 160 + (i % 2) * 52, 430, 4), route_gold)
		_draw_rect(image, Rect2i(180 + i * 90, 280 + (i % 3) * 34, 380, 3), route_blue)
	for i in range(6):
		_draw_rect(image, Rect2i(170 + i * 96, 110 + i * 72, 4, 220), route_gold)
	for i in range(10):
		_draw_rect(image, Rect2i(60 + i * 110, 480 + ((i + 1) % 3) * 16, 46, 2), Color(0.88, 0.82, 0.62, 0.18))
	return ImageTexture.create_from_image(image)


func _make_city_map_texture() -> Texture2D:
	var w := 1180
	var h := 760
	var image := Image.create(w, h, false, Image.FORMAT_RGBA8)
	_paint_vertical_gradient(image, Color(0.05, 0.08, 0.13, 1.0), Color(0.02, 0.05, 0.08, 1.0))
	_draw_rect(image, Rect2i(0, 0, w, h), Color(0.03, 0.08, 0.13, 0.18))

	# 黄浦江（右侧江面与水雾反光）
	for i in range(0, h, 8):
		var river_x := 760 + int((i - 220) * 0.08)
		river_x = clampi(river_x, 640, 920)
		var river_w := w - river_x
		var alpha := 0.34 + float(i) / float(h) * 0.18
		_draw_rect(image, Rect2i(river_x, i, river_w, 8), Color(0.08, 0.28, 0.42, alpha))
	for i in range(8):
		_draw_rect(image, Rect2i(760 + i * 14, 0, 8, h), Color(0.18, 0.46, 0.64, 0.08))

	# 江面高光条
	for i in range(14):
		var y := 430 + int((i * 23) % 240)
		var x := 820 + int((i * 67) % 300)
		_draw_rect(image, Rect2i(x, y, 32 + (i % 3) * 18, 2), Color(0.78, 0.92, 1.0, 0.14))

	# 左岸城市街区底色
	_draw_rect(image, Rect2i(0, 470, 760, 290), Color(0.03, 0.07, 0.11, 0.40))

	# 陆家嘴远景天际线（顶部轮廓）
	var skyline := [
		[460, 86, 44, 180], [514, 62, 52, 210], [580, 78, 42, 196], [636, 54, 58, 236],
		[708, 70, 40, 198], [760, 44, 66, 250], [840, 68, 50, 214], [905, 36, 64, 274]
	]
	for b in skyline:
		_draw_rect(image, Rect2i(b[0], b[1], b[2], b[3]), Color(0.10, 0.12, 0.18, 0.88))
		_draw_rect(image, Rect2i(b[0] + 4, b[1] + 4, b[2] - 8, b[3] - 8), Color(0.14, 0.18, 0.27, 0.34))

	# 上海中心/环球金融中心/金茂（抽象轮廓）
	_draw_rect(image, Rect2i(840, 40, 26, 300), Color(0.08, 0.10, 0.16, 0.96))  # 上海中心细长
	_draw_rect(image, Rect2i(875, 62, 34, 258), Color(0.08, 0.10, 0.16, 0.94))  # 环球金融中心
	_draw_rect(image, Rect2i(883, 74, 18, 16), Color(0.03, 0.05, 0.08, 1.0))   # 开瓶器缺口
	_draw_rect(image, Rect2i(918, 86, 28, 226), Color(0.09, 0.11, 0.17, 0.92))  # 金茂

	# 东方明珠（抽象塔体+球）
	_draw_rect(image, Rect2i(790, 96, 10, 250), Color(0.18, 0.22, 0.33, 0.92))
	_draw_rect(image, Rect2i(776, 118, 38, 24), Color(0.28, 0.20, 0.31, 0.90))
	_draw_rect(image, Rect2i(782, 124, 26, 12), Color(0.42, 0.28, 0.46, 0.84))
	_draw_rect(image, Rect2i(784, 176, 30, 18), Color(0.29, 0.21, 0.33, 0.88))
	_draw_rect(image, Rect2i(788, 238, 22, 14), Color(0.25, 0.19, 0.29, 0.82))

	# 城市地面区块（左侧与中部），强化“地图”感
	for i in range(18):
		var bx := 70 + int((i * 69) % 650)
		var by := 486 + int((i * 31) % 170)
		var bw := 24 + (i % 4) * 10
		var bh := 18 + (i % 3) * 8
		_draw_rect(image, Rect2i(bx, by, bw, bh), Color(0.25, 0.16, 0.14, 0.42))
		_draw_rect(image, Rect2i(bx + 3, by + 3, max(2, bw - 6), max(2, bh - 6)), Color(0.83, 0.67, 0.47, 0.12))

	# 金融路线和区域连接（保留地图导视）
	var road_gold := Color(0.83, 0.74, 0.50, 0.34)
	var road_blue := Color(0.38, 0.76, 0.92, 0.18)
	for i in range(4):
		_draw_rect(image, Rect2i(220 + i * 62, 166 + ((i + 1) % 2) * 44, 590 - i * 38, 4), road_gold)
	for i in range(5):
		_draw_rect(image, Rect2i(260 + i * 52, 322 + (i % 3) * 36, 500 - i * 28, 4), road_gold)
	for i in range(6):
		_draw_rect(image, Rect2i(180 + i * 92, 210 + i * 58, 4, 200 - i * 10), road_gold)
	for i in range(5):
		_draw_rect(image, Rect2i(150 + i * 120, 260 + i * 72, 420, 2), road_blue)

	# 远处雾层与城市泛光
	_draw_rect(image, Rect2i(0, 0, w, 120), Color(0.40, 0.44, 0.52, 0.08))
	_draw_rect(image, Rect2i(0, 560, w, 200), Color(0.03, 0.05, 0.08, 0.16))
	_draw_rect(image, Rect2i(730, 120, 210, 340), Color(0.12, 0.32, 0.46, 0.08))

	return ImageTexture.create_from_image(image)


func _build_settings_overlay() -> void:
	_settings_overlay = ColorRect.new()
	_settings_overlay.set_anchors_preset(Control.PRESET_FULL_RECT)
	_settings_overlay.color = Color(0.0, 0.0, 0.0, 0.58)
	_settings_overlay.mouse_filter = Control.MOUSE_FILTER_STOP
	_settings_overlay.visible = false
	_settings_overlay.z_index = 400
	_settings_overlay.gui_input.connect(func(event: InputEvent) -> void:
		if event is InputEventMouseButton and event.pressed:
			_toggle_settings(false)
	)
	add_child(_settings_overlay)

	var center := CenterContainer.new()
	center.set_anchors_preset(Control.PRESET_FULL_RECT)
	center.mouse_filter = Control.MOUSE_FILTER_IGNORE
	_settings_overlay.add_child(center)

	_settings_modal = PanelContainer.new()
	_settings_modal.custom_minimum_size = Vector2(380, 0)
	center.add_child(_settings_modal)
	_style_panel(_settings_modal, Color(0.07, 0.10, 0.15, 0.98), Color(0.60, 0.72, 0.88, 0.95))

	var box := VBoxContainer.new()
	box.add_theme_constant_override("separation", 10)
	_settings_modal.add_child(box)

	var title := Label.new()
	title.text = "系统设置"
	title.add_theme_font_size_override("font_size", 24)
	title.add_theme_color_override("font_color", Color(0.94, 0.98, 1.0, 1.0))
	box.add_child(title)

	_settings_new_map_btn = Button.new()
	_settings_new_map_btn.text = "新开地图局"
	_settings_new_map_btn.pressed.connect(_on_settings_new_map_pressed)
	box.add_child(_settings_new_map_btn)

	_settings_home_btn = Button.new()
	_settings_home_btn.text = "回到主选单"
	_settings_home_btn.pressed.connect(_on_settings_home_pressed)
	box.add_child(_settings_home_btn)

	_settings_quit_btn = Button.new()
	_settings_quit_btn.text = "离开游戏"
	_settings_quit_btn.pressed.connect(_on_settings_quit_pressed)
	box.add_child(_settings_quit_btn)

	_settings_run_summary_label = Label.new()
	_settings_run_summary_label.text = "当前局：未开始"
	_settings_run_summary_label.autowrap_mode = TextServer.AUTOWRAP_WORD_SMART
	_settings_run_summary_label.add_theme_color_override("font_color", Color(0.82, 0.90, 0.98, 1.0))
	_settings_run_summary_label.add_theme_font_size_override("font_size", 13)
	box.add_child(_settings_run_summary_label)

	_settings_version_label = Label.new()
	_settings_version_label.text = "客户端版本：build=%s" % str(_api.client_build())
	_settings_version_label.add_theme_color_override("font_color", Color(0.70, 0.82, 0.96, 1.0))
	_settings_version_label.add_theme_font_size_override("font_size", 13)
	box.add_child(_settings_version_label)

	_style_action_btn_for_settings(_settings_new_map_btn)
	_style_action_btn_for_settings(_settings_home_btn)
	_style_action_btn_for_settings(_settings_quit_btn)
	_refresh_settings_overlay_info()
	_build_new_game_setup_overlay()


func _build_new_game_setup_overlay() -> void:
	_new_game_setup_overlay = ColorRect.new()
	_new_game_setup_overlay.set_anchors_preset(Control.PRESET_FULL_RECT)
	_new_game_setup_overlay.color = Color(0.0, 0.0, 0.0, 0.68)
	_new_game_setup_overlay.mouse_filter = Control.MOUSE_FILTER_STOP
	_new_game_setup_overlay.visible = false
	_new_game_setup_overlay.z_index = 420
	add_child(_new_game_setup_overlay)

	var center := CenterContainer.new()
	center.set_anchors_preset(Control.PRESET_FULL_RECT)
	center.mouse_filter = Control.MOUSE_FILTER_IGNORE
	_new_game_setup_overlay.add_child(center)

	_new_game_setup_modal = PanelContainer.new()
	_new_game_setup_modal.custom_minimum_size = Vector2(720, 540)
	_style_panel(_new_game_setup_modal, Color(0.05, 0.08, 0.13, 0.98), Color(0.62, 0.74, 0.90, 0.95), 14)
	center.add_child(_new_game_setup_modal)

	var root := VBoxContainer.new()
	root.size_flags_vertical = Control.SIZE_EXPAND_FILL
	root.add_theme_constant_override("separation", 10)
	_new_game_setup_modal.add_child(root)

	var header := VBoxContainer.new()
	header.add_theme_constant_override("separation", 4)
	root.add_child(header)

	_new_game_setup_title = Label.new()
	_new_game_setup_title.text = "新开局角色设定"
	_new_game_setup_title.add_theme_font_size_override("font_size", 24)
	_new_game_setup_title.add_theme_color_override("font_color", Color(0.94, 0.98, 1.0, 1.0))
	header.add_child(_new_game_setup_title)

	_new_game_setup_step_label = Label.new()
	_new_game_setup_step_label.text = "步骤 1/4"
	_new_game_setup_step_label.add_theme_font_size_override("font_size", 14)
	_new_game_setup_step_label.add_theme_color_override("font_color", Color(0.76, 0.87, 0.99, 1.0))
	header.add_child(_new_game_setup_step_label)

	var line := ColorRect.new()
	line.custom_minimum_size = Vector2(0, 1)
	line.color = Color(0.30, 0.47, 0.66, 0.85)
	root.add_child(line)

	_new_game_setup_content = VBoxContainer.new()
	_new_game_setup_content.size_flags_vertical = Control.SIZE_EXPAND_FILL
	_new_game_setup_content.add_theme_constant_override("separation", 10)
	root.add_child(_new_game_setup_content)

	_new_game_setup_error_label = Label.new()
	_new_game_setup_error_label.autowrap_mode = TextServer.AUTOWRAP_WORD_SMART
	_new_game_setup_error_label.visible = false
	_new_game_setup_error_label.add_theme_color_override("font_color", Color(1.0, 0.55, 0.55, 1.0))
	root.add_child(_new_game_setup_error_label)

	var footer := HBoxContainer.new()
	footer.add_theme_constant_override("separation", 8)
	root.add_child(footer)

	_new_game_setup_cancel_btn = Button.new()
	_new_game_setup_cancel_btn.text = "取消"
	_new_game_setup_cancel_btn.pressed.connect(_on_new_game_setup_cancel_pressed)
	footer.add_child(_new_game_setup_cancel_btn)

	var spacer := Control.new()
	spacer.size_flags_horizontal = Control.SIZE_EXPAND_FILL
	footer.add_child(spacer)

	_new_game_setup_prev_btn = Button.new()
	_new_game_setup_prev_btn.text = "上一步"
	_new_game_setup_prev_btn.pressed.connect(_on_new_game_setup_prev_pressed)
	footer.add_child(_new_game_setup_prev_btn)

	_new_game_setup_next_btn = Button.new()
	_new_game_setup_next_btn.text = "下一步"
	_new_game_setup_next_btn.pressed.connect(_on_new_game_setup_next_pressed)
	footer.add_child(_new_game_setup_next_btn)

	_new_game_setup_confirm_btn = Button.new()
	_new_game_setup_confirm_btn.text = "确认创建"
	_new_game_setup_confirm_btn.pressed.connect(_on_new_game_setup_confirm_pressed)
	footer.add_child(_new_game_setup_confirm_btn)

	for b in [_new_game_setup_cancel_btn, _new_game_setup_prev_btn, _new_game_setup_next_btn, _new_game_setup_confirm_btn]:
		_style_action_btn_for_settings(b)
		b.custom_minimum_size = Vector2(110, 40)


func _clear_all_children(node: Node) -> void:
	if node == null:
		return
	for c in node.get_children():
		c.queue_free()


func _reset_new_game_setup_form_defaults() -> void:
	_setup_step = SETUP_STEP_NAME
	_setup_inflight = false
	_setup_form_player_name = str(_api.username).strip_edges() if _api != null else ""
	if _setup_form_player_name.is_empty():
		_setup_form_player_name = "玩家"
	_setup_form_player_name = _setup_form_player_name.substr(0, 20)
	_setup_form_traits = ["外向", "谦虚", "喜欢规则", "看重自由"]
	_setup_form_style_answers = {
		"horizon_preference": "long",
		"risk_preference": "avoid_loss",
		"priority_preference": "skill",
	}
	_setup_form_god_mode = false
	_setup_trait_selects.clear()
	_setup_style_selects.clear()
	_setup_name_input = null
	_setup_god_mode_check = null


func _toggle_new_game_setup_overlay(open: bool) -> void:
	if _new_game_setup_overlay == null:
		return
	_new_game_setup_overlay.visible = open
	if not open:
		_setup_inflight = false
		return
	_render_new_game_setup_step()
	if _new_game_setup_modal != null:
		_new_game_setup_modal.scale = Vector2(0.96, 0.96)
		_new_game_setup_modal.modulate = Color(1, 1, 1, 0.0)
		var tw := create_tween()
		tw.tween_property(_new_game_setup_modal, "scale", Vector2(1.0, 1.0), 0.14).set_trans(Tween.TRANS_BACK).set_ease(Tween.EASE_OUT)
		tw.parallel().tween_property(_new_game_setup_modal, "modulate", Color(1, 1, 1, 1.0), 0.14).set_trans(Tween.TRANS_SINE).set_ease(Tween.EASE_OUT)


func _open_new_game_setup_overlay(reason: String = "") -> void:
	_setup_entry_reason = reason
	_reset_new_game_setup_form_defaults()
	_toggle_new_game_setup_overlay(true)


func _set_setup_error(msg: String) -> void:
	if _new_game_setup_error_label == null:
		return
	var text := msg.strip_edges()
	_new_game_setup_error_label.text = text
	_new_game_setup_error_label.visible = not text.is_empty()


func _make_setup_section_title(text_value: String) -> Label:
	var lb := Label.new()
	lb.text = text_value
	lb.add_theme_font_size_override("font_size", 18)
	lb.add_theme_color_override("font_color", Color(0.94, 0.96, 0.99, 1.0))
	return lb


func _make_setup_desc(text_value: String) -> Label:
	var lb := Label.new()
	lb.text = text_value
	lb.autowrap_mode = TextServer.AUTOWRAP_WORD_SMART
	lb.add_theme_font_size_override("font_size", 14)
	lb.add_theme_color_override("font_color", Color(0.76, 0.86, 0.98, 1.0))
	return lb


func _render_new_game_setup_step() -> void:
	if _new_game_setup_content == null:
		return
	_clear_all_children(_new_game_setup_content)
	_setup_trait_selects.clear()
	_setup_style_selects.clear()
	_setup_name_input = null
	_setup_god_mode_check = null
	_set_setup_error("")

	if _new_game_setup_step_label != null:
		_new_game_setup_step_label.text = "步骤 %d/4" % _setup_step
	if _new_game_setup_title != null:
		_new_game_setup_title.text = "新开局角色设定"
		if not _setup_entry_reason.is_empty():
			_new_game_setup_title.text = "新开局角色设定｜%s" % _setup_entry_reason

	match _setup_step:
		SETUP_STEP_NAME:
			_render_setup_step_name()
		SETUP_STEP_TRAITS:
			_render_setup_step_traits()
		SETUP_STEP_STYLE:
			_render_setup_step_style()
		_:
			_render_setup_step_confirm()

	if _new_game_setup_prev_btn != null:
		_new_game_setup_prev_btn.visible = _setup_step > SETUP_STEP_NAME
		_new_game_setup_prev_btn.disabled = _setup_inflight
	if _new_game_setup_next_btn != null:
		_new_game_setup_next_btn.visible = _setup_step < SETUP_STEP_CONFIRM
		_new_game_setup_next_btn.disabled = _setup_inflight
	if _new_game_setup_confirm_btn != null:
		_new_game_setup_confirm_btn.visible = _setup_step == SETUP_STEP_CONFIRM
		_new_game_setup_confirm_btn.disabled = _setup_inflight
	if _new_game_setup_cancel_btn != null:
		_new_game_setup_cancel_btn.disabled = _setup_inflight


func _render_setup_step_name() -> void:
	_new_game_setup_content.add_child(_make_setup_section_title("第一步：输入玩家名称"))
	_new_game_setup_content.add_child(_make_setup_desc("用于属性页显示，长度 1~20。"))
	var input := LineEdit.new()
	input.placeholder_text = "请输入玩家名称"
	input.text = _setup_form_player_name
	input.max_length = 20
	input.text_changed.connect(func(v: String) -> void:
		_setup_form_player_name = v
	)
	_new_game_setup_content.add_child(input)
	_setup_name_input = input


func _render_setup_step_traits() -> void:
	_new_game_setup_content.add_child(_make_setup_section_title("第二步：选择性格（4组二选一）"))
	_new_game_setup_content.add_child(_make_setup_desc("这些标签本轮先用于属性展示与后续事件系统占位。"))
	for i in range(TRAIT_OPTIONS.size()):
		var one: Dictionary = TRAIT_OPTIONS[i]
		var row := HBoxContainer.new()
		row.add_theme_constant_override("separation", 8)
		_new_game_setup_content.add_child(row)
		var title := Label.new()
		title.custom_minimum_size = Vector2(180, 0)
		title.text = str(one.get("title", ""))
		row.add_child(title)
		var select := OptionButton.new()
		select.size_flags_horizontal = Control.SIZE_EXPAND_FILL
		var opts: Array = one.get("options", [])
		for opt in opts:
			select.add_item(str(opt))
		var current := str(_setup_form_traits[i]) if i < _setup_form_traits.size() else str(opts[0] if opts.size() > 0 else "")
		var picked_idx := 0
		for j in range(opts.size()):
			if str(opts[j]) == current:
				picked_idx = j
				break
		select.select(picked_idx)
		row.add_child(select)
		_setup_trait_selects.append(select)


func _render_setup_step_style() -> void:
	_new_game_setup_content.add_child(_make_setup_section_title("第三步：选择操盘风格（3组二选一）"))
	_new_game_setup_content.add_child(_make_setup_desc("你的选择会影响本地图局的初始上场卡组构成。"))
	for one_v in STYLE_QUESTIONS:
		if not (one_v is Dictionary):
			continue
		var one := one_v as Dictionary
		var key := str(one.get("key", ""))
		var row := HBoxContainer.new()
		row.add_theme_constant_override("separation", 8)
		_new_game_setup_content.add_child(row)
		var title := Label.new()
		title.custom_minimum_size = Vector2(180, 0)
		title.text = str(one.get("title", ""))
		row.add_child(title)
		var select := OptionButton.new()
		select.size_flags_horizontal = Control.SIZE_EXPAND_FILL
		var opts: Array = one.get("options", [])
		var current_code := str(_setup_form_style_answers.get(key, ""))
		var picked_idx := 0
		for i in range(opts.size()):
			var opt_v: Variant = opts[i]
			if not (opt_v is Dictionary):
				continue
			var opt := opt_v as Dictionary
			select.add_item(str(opt.get("label", opt.get("code", ""))))
			select.set_item_metadata(i, str(opt.get("code", "")))
			if str(opt.get("code", "")) == current_code:
				picked_idx = i
		select.select(picked_idx)
		row.add_child(select)
		_setup_style_selects[key] = select


func _render_setup_step_confirm() -> void:
	_sync_setup_form_from_inputs()
	_new_game_setup_content.add_child(_make_setup_section_title("第四步：确认设定并创建地图局"))
	var summary := RichTextLabel.new()
	summary.scroll_active = true
	summary.fit_content = false
	summary.custom_minimum_size = Vector2(0, 310)
	summary.size_flags_vertical = Control.SIZE_EXPAND_FILL
	summary.append_text(_new_game_setup_confirm_text())
	_new_game_setup_content.add_child(summary)
	var check := CheckBox.new()
	check.text = "无敌（管理员测试）"
	check.button_pressed = _setup_form_god_mode
	check.toggled.connect(func(v: bool) -> void:
		_setup_form_god_mode = v
	)
	_new_game_setup_content.add_child(check)
	_setup_god_mode_check = check


func _sync_setup_form_from_inputs() -> void:
	if _setup_name_input != null:
		_setup_form_player_name = _setup_name_input.text
	if _setup_trait_selects.size() == TRAIT_OPTIONS.size():
		var out_traits: Array[String] = []
		for i in range(_setup_trait_selects.size()):
			var select: OptionButton = _setup_trait_selects[i]
			var idx: int = maxi(0, select.get_selected_id())
			out_traits.append(select.get_item_text(idx))
		_setup_form_traits = out_traits
	for key in _setup_style_selects.keys():
		var select_v: Variant = _setup_style_selects.get(key, null)
		if not (select_v is OptionButton):
			continue
		var select := select_v as OptionButton
		var idx: int = maxi(0, select.get_selected_id())
		var code_v: Variant = select.get_item_metadata(idx)
		_setup_form_style_answers[str(key)] = str(code_v)
	if _setup_god_mode_check != null:
		_setup_form_god_mode = _setup_god_mode_check.button_pressed


func _validate_current_setup_step() -> String:
	_sync_setup_form_from_inputs()
	if _setup_step == SETUP_STEP_NAME:
		var name := _setup_form_player_name.strip_edges()
		if name.is_empty():
			return "请输入玩家名称。"
		if name.length() > 20:
			return "玩家名称最长 20 字。"
	elif _setup_step == SETUP_STEP_TRAITS:
		if _setup_form_traits.size() != 4:
			return "请完成 4 组性格选择。"
	elif _setup_step == SETUP_STEP_STYLE:
		for q_v in STYLE_QUESTIONS:
			var q := q_v as Dictionary
			var key := str(q.get("key", ""))
			if str(_setup_form_style_answers.get(key, "")).is_empty():
				return "请完成操盘风格选择。"
	return ""


func _new_game_setup_confirm_text() -> String:
	var trait_text := "、".join(_setup_form_traits)
	var style_lines := [
		"交易周期：%s" % _style_label("horizon_preference", str(_setup_form_style_answers.get("horizon_preference", "long"))),
		"风险取向：%s" % _style_label("risk_preference", str(_setup_form_style_answers.get("risk_preference", "avoid_loss"))),
		"优先偏好：%s" % _style_label("priority_preference", str(_setup_form_style_answers.get("priority_preference", "skill"))),
	]
	var preview_counts := _preview_deck_summary_counts(_setup_form_style_answers, _setup_form_god_mode)
	var total_cards := 0
	for v in preview_counts.values():
		total_cards += int(v)
	var preview_lines := _preview_deck_summary_lines(_setup_form_style_answers, _setup_form_god_mode)
	return "姓名：%s\n\n性格：%s\n\n操盘风格：\n- %s\n- %s\n- %s\n\n初始上场卡组预览（%d张）：\n%s" % [
		_setup_form_player_name.strip_edges(),
		trait_text,
		style_lines[0], style_lines[1], style_lines[2],
		total_cards,
		"\n".join(preview_lines),
	]


func _style_label(key: String, code: String) -> String:
	match key:
		"horizon_preference":
			return "短线" if code == "short" else "长线"
		"risk_preference":
			return "追求获利" if code == "seek_profit" else "规避亏损"
		"priority_preference":
			return "心理素质" if code == "mindset" else "交易技巧"
		_:
			return code


func _preview_deck_summary_counts(style_answers: Dictionary, god_mode: bool) -> Dictionary:
	var counts: Dictionary = {}
	if god_mode:
		for cid in CARD_NAMES.keys():
			counts[str(cid)] = 1
	else:
		counts = {
			"short_short_novice": 3, "short_long_novice": 3, "trend_short_novice": 3, "trend_long_novice": 3,
			"tactic_quick_cancel": 1, "tactic_meditation": 1, "tactic_risk_control": 1
		}
		if str(style_answers.get("horizon_preference", "long")) == "short":
			counts["trend_short_novice"] = max(0, int(counts.get("trend_short_novice", 0)) - 1)
			counts["trend_long_novice"] = max(0, int(counts.get("trend_long_novice", 0)) - 1)
			counts["short_short_novice"] = int(counts.get("short_short_novice", 0)) + 1
			counts["short_long_novice"] = int(counts.get("short_long_novice", 0)) + 1
		if str(style_answers.get("risk_preference", "avoid_loss")) == "seek_profit":
			counts["tactic_risk_control"] = max(0, int(counts.get("tactic_risk_control", 0)) - 1)
			counts["tactic_quick_cancel"] = max(0, int(counts.get("tactic_quick_cancel", 0)) - 1)
			counts["trend_short_novice"] = int(counts.get("trend_short_novice", 0)) + 1
			counts["trend_long_novice"] = int(counts.get("trend_long_novice", 0)) + 1
		if str(style_answers.get("priority_preference", "skill")) == "mindset":
			counts["short_short_novice"] = max(0, int(counts.get("short_short_novice", 0)) - 1)
			counts["short_long_novice"] = max(0, int(counts.get("short_long_novice", 0)) - 1)
			counts["tactic_meditation"] = int(counts.get("tactic_meditation", 0)) + 2
	return counts


func _preview_deck_summary_lines(style_answers: Dictionary, god_mode: bool) -> PackedStringArray:
	var counts := _preview_deck_summary_counts(style_answers, god_mode)
	var lines: PackedStringArray = []
	var keys := counts.keys()
	keys.sort()
	for cid_v in keys:
		var cid := str(cid_v)
		var n := int(counts.get(cid, 0))
		if n <= 0:
			continue
		lines.append("- %s ×%d" % [_card_name(cid), n])
	return lines


func _build_new_game_setup_payload() -> Dictionary:
	_sync_setup_form_from_inputs()
	return {
		"player_name": _setup_form_player_name.strip_edges(),
		"traits": _setup_form_traits.duplicate(),
		"style_answers": _setup_form_style_answers.duplicate(true),
		"god_mode": _setup_form_god_mode,
	}


func _on_new_game_setup_cancel_pressed() -> void:
	if _setup_inflight:
		return
	_toggle_new_game_setup_overlay(false)


func _on_new_game_setup_prev_pressed() -> void:
	if _setup_inflight:
		return
	_set_setup_error("")
	_setup_step = maxi(SETUP_STEP_NAME, _setup_step - 1)
	_render_new_game_setup_step()


func _on_new_game_setup_next_pressed() -> void:
	if _setup_inflight:
		return
	var err := _validate_current_setup_step()
	if not err.is_empty():
		_set_setup_error(err)
		return
	_set_setup_error("")
	_setup_step = mini(SETUP_STEP_CONFIRM, _setup_step + 1)
	_render_new_game_setup_step()


func _on_new_game_setup_confirm_pressed() -> void:
	if _setup_inflight:
		return
	var err := _validate_current_setup_step()
	if not err.is_empty():
		_set_setup_error(err)
		return
	_setup_inflight = true
	_render_new_game_setup_step()
	battle_loading.emit(true, "正在创建角色与地图局")
	var ok := await _create_map_run(_build_new_game_setup_payload(), true)
	battle_loading.emit(false, "")
	_setup_inflight = false
	if ok:
		_toggle_new_game_setup_overlay(false)
	else:
		_render_new_game_setup_step()


func _style_action_btn_for_settings(btn: Button) -> void:
	if btn == null:
		return
	btn.custom_minimum_size = Vector2(0, 40)
	var normal := StyleBoxFlat.new()
	normal.set_corner_radius_all(9)
	normal.set_border_width_all(1)
	normal.bg_color = Color(0.22, 0.33, 0.50, 0.98)
	normal.border_color = Color(0.40, 0.58, 0.86, 0.98)
	var hover := normal.duplicate()
	hover.bg_color = Color(0.27, 0.39, 0.58, 0.98)
	hover.border_color = Color(0.49, 0.68, 0.97, 1.0)
	btn.add_theme_stylebox_override("normal", normal)
	btn.add_theme_stylebox_override("hover", hover)
	btn.add_theme_stylebox_override("pressed", hover)
	btn.add_theme_color_override("font_color", Color(0.95, 0.98, 1.0, 1.0))


func _toggle_settings(open: bool) -> void:
	if _settings_overlay == null:
		return
	_settings_overlay.visible = open
	if not open:
		return
	_refresh_settings_overlay_info()
	if _settings_modal != null:
		_settings_modal.scale = Vector2(0.94, 0.94)
		_settings_modal.modulate = Color(1, 1, 1, 0.0)
		var tw := create_tween()
		tw.tween_property(_settings_modal, "scale", Vector2(1.0, 1.0), 0.16).set_trans(Tween.TRANS_BACK).set_ease(Tween.EASE_OUT)
		tw.parallel().tween_property(_settings_modal, "modulate", Color(1, 1, 1, 1.0), 0.16).set_trans(Tween.TRANS_SINE).set_ease(Tween.EASE_OUT)


func _refresh_settings_overlay_info() -> void:
	if _settings_run_summary_label != null:
		if _state.is_empty():
			_settings_run_summary_label.text = "当前局：未开始"
		else:
			_settings_run_summary_label.text = "当前局：MapRun#%s | %s | 回合=%s/72 | 地点=%s" % [
				str(_state.get("map_run_id", 0)),
				str(_state.get("status", "-")),
				str(_state.get("turn_index", 1)),
				str(_state.get("location_name", _state.get("location", "-"))),
			]
	if _settings_version_label != null:
		_settings_version_label.text = "客户端版本：build=%s" % str(_api.client_build())


func _on_settings_home_pressed() -> void:
	_toggle_settings(false)
	logout_requested.emit(false)


func _on_settings_new_map_pressed() -> void:
	_toggle_settings(false)
	_open_new_game_setup_overlay("地图内新开")


func _on_settings_quit_pressed() -> void:
	_toggle_settings(false)
	get_tree().quit()


func _status(msg: String) -> void:
	if _status_label != null:
		_status_label.text = "状态：%s" % msg


func _append_log(line: String) -> void:
	if _log_box == null:
		return
	if _log_box.get_parsed_text().length() > 0:
		_log_box.append_text("\n")
	_log_box.append_text("[%s] %s" % [Time.get_datetime_string_from_system(false, true), line])
	_log_box.scroll_to_line(_log_box.get_line_count())


func _set_action_enabled(enabled: bool) -> void:
	_actions_enabled = enabled
	var disabled = not enabled
	for btn in [
		_create_btn, _resume_btn, _logout_btn,
		_home_attr_btn, _home_deck_btn, _home_out_btn, _rest_btn,
		_move_home_btn, _move_assoc_btn, _battle_btn,
		_add_card_btn, _remove_card_btn, _direction_sort_btn, _level_sort_btn,
		_assoc_back_home_btn, _assoc_battle_btn,
		]:
			if btn != null:
				btn.disabled = disabled
	for btn in _outing_map_buttons:
		if btn != null:
			btn.disabled = disabled
	_refresh_outing_view_state()


func _panel_for_home_mode(mode: String) -> PanelContainer:
	match mode:
		HOME_VIEW_ATTR:
			return _home_attr_panel
		HOME_VIEW_DECK:
			return _home_deck_panel
		HOME_VIEW_OUT:
			return _home_out_panel
		_:
			return _home_home_panel


func _set_home_view(mode: String) -> void:
	var prev_mode := _home_view_mode
	var changed := prev_mode != mode
	_home_view_mode = mode
	if _home_home_panel != null:
		_home_home_panel.visible = mode == HOME_VIEW_HOME
	if _home_attr_panel != null:
		_home_attr_panel.visible = mode == HOME_VIEW_ATTR
	if _home_deck_panel != null:
		_home_deck_panel.visible = mode == HOME_VIEW_DECK
	if _home_out_panel != null:
		_home_out_panel.visible = mode == HOME_VIEW_OUT
	var on_home := mode == HOME_VIEW_HOME
	var on_out := mode == HOME_VIEW_OUT
	if _home_menu_panel_ref != null:
		_home_menu_panel_ref.visible = on_home
	if _home_view_host != null:
		if on_out:
			_home_view_host.anchor_left = 0.0
			_home_view_host.anchor_top = 0.0
			_home_view_host.anchor_right = 1.0
			_home_view_host.anchor_bottom = 1.0
		else:
			_home_view_host.anchor_left = 0.03
			_home_view_host.anchor_top = 0.20
			_home_view_host.anchor_right = 0.70 if on_home else 0.97
			_home_view_host.anchor_bottom = 0.95
	if _home_scene_title_label != null:
		_home_scene_title_label.visible = not on_out
	if _home_scene_stats_panel != null:
		_home_scene_stats_panel.visible = not on_out
	if not changed:
		return
	var prev_panel := _panel_for_home_mode(prev_mode)
	var target_panel := _panel_for_home_mode(mode)
	if target_panel == null:
		return
	if prev_panel == null or prev_panel == target_panel:
		target_panel.visible = true
		target_panel.modulate = Color(1, 1, 1, 1)
		return
	prev_panel.visible = true
	target_panel.visible = true
	prev_panel.modulate = Color(1, 1, 1, 1)
	target_panel.modulate = Color(1, 1, 1, 0)
	var tw := create_tween()
	tw.set_parallel(true)
	tw.tween_property(prev_panel, "modulate", Color(1, 1, 1, 0), 0.12).set_trans(Tween.TRANS_SINE).set_ease(Tween.EASE_OUT)
	tw.tween_property(target_panel, "modulate", Color(1, 1, 1, 1), 0.12).set_trans(Tween.TRANS_SINE).set_ease(Tween.EASE_OUT)
	tw.finished.connect(func() -> void:
		if prev_panel != null and prev_panel != target_panel:
			prev_panel.visible = false
			prev_panel.modulate = Color(1, 1, 1, 1)
		if target_panel != null:
			target_panel.visible = true
			target_panel.modulate = Color(1, 1, 1, 1)
	)


func _init_card_pool() -> void:
	_pool_ids.clear()
	for cid in CARD_NAMES.keys():
		for _i in range(POOL_COPIES_PER_CARD):
			_pool_ids.append(cid)
	_pool_ids.sort()
	_pool_cards = _pool_ids.duplicate()


func _render_deck_lists() -> void:
	_pool_stack_entries = _sorted_stack_entries(_build_stack_entries(_pool_cards), _sort_mode)
	_deck_stack_entries = _sorted_stack_entries(_build_stack_entries(_editing_deck), _sort_mode)
	if _deck_list_title_label != null:
		_deck_list_title_label.text = "上场卡组（%d）" % _editing_deck.size()
	if _pool_list != null:
		_pool_list.clear()
		for e in _pool_stack_entries:
			var label := "%s  ×%s" % [str(e.get("name", "-")), str(e.get("count", 0))]
			_pool_list.add_item(label)
	if _deck_list != null:
		_deck_list.clear()
		for e in _deck_stack_entries:
			var label := "%s  ×%s" % [str(e.get("name", "-")), str(e.get("count", 0))]
			_deck_list.add_item(label)
	if not _try_restore_detail_selection():
		_update_deck_detail_for_empty()


func _build_stack_entries(cards: Array) -> Array[Dictionary]:
	var grouped: Dictionary = {}
	var order: Array[String] = []
	for raw in cards:
		var cid := str(raw).strip_edges()
		if cid.is_empty():
			continue
		var name := _card_name(cid)
		if not grouped.has(name):
			grouped[name] = {
				"name": name,
				"primary_id": cid,
				"ids": [cid],
				"count": 1,
			}
			order.append(name)
		else:
			var e: Dictionary = grouped[name]
			var ids_arr: Array = e.get("ids", [])
			ids_arr.append(cid)
			e["ids"] = ids_arr
			e["count"] = int(e.get("count", 0)) + 1
			grouped[name] = e
	var out: Array[Dictionary] = []
	for name in order:
		var one: Variant = grouped.get(name, {})
		if one is Dictionary:
			out.append(one)
	return out


func _sorted_stack_entries(entries: Array[Dictionary], mode: String) -> Array[Dictionary]:
	var sorted: Array[Dictionary] = entries.duplicate()
	var current_mode := mode.strip_edges().to_lower()
	sorted.sort_custom(func(a: Dictionary, b: Dictionary) -> bool:
		var a_name := _entry_sort_name(a)
		var b_name := _entry_sort_name(b)
		if current_mode == "direction":
			var a_bucket := _direction_bucket(str(a.get("primary_id", "")))
			var b_bucket := _direction_bucket(str(b.get("primary_id", "")))
			if a_bucket != b_bucket:
				return a_bucket < b_bucket
		elif current_mode == "level":
			var a_rank := _level_rank(str(a.get("primary_id", "")))
			var b_rank := _level_rank(str(b.get("primary_id", "")))
			if a_rank != b_rank:
				return a_rank < b_rank
		return a_name < b_name
	)
	return sorted


func _entry_sort_name(entry: Dictionary) -> String:
	return str(entry.get("name", "")).to_lower()


func _direction_bucket(card_id: String) -> int:
	var cid := card_id.strip_edges().to_lower()
	if cid.begins_with("short_long_") or cid.begins_with("trend_long_") or cid.begins_with("breakout_long_"):
		return 0
	if cid.begins_with("short_short_") or cid.begins_with("trend_short_") or cid.begins_with("breakout_short_"):
		return 1
	return 2


func _level_rank(card_id: String) -> int:
	var cid := card_id.strip_edges().to_lower()
	if cid.ends_with("_novice"):
		return 0
	if cid.ends_with("_skilled"):
		return 1
	if cid.ends_with("_veteran"):
		return 2
	if cid.ends_with("_master"):
		return 3
	return 9


func _update_deck_detail_for_empty() -> void:
	_detail_source_zone = ""
	_detail_card_id = ""
	if _deck_detail_title != null:
		_deck_detail_title.text = "请选择卡池或上场卡组中的卡牌"
	if _deck_detail_body != null:
		_deck_detail_body.clear()
		_deck_detail_body.append_text("这里会显示该卡牌的详细说明与数量。")


func _show_card_detail_from_entry(entry: Dictionary, source_zone: String) -> void:
	if entry.is_empty():
		_update_deck_detail_for_empty()
		return
	var cid := str(entry.get("primary_id", ""))
	var name := str(entry.get("name", _card_name(cid)))
	var count := int(entry.get("count", 0))
	_detail_card_id = cid
	_detail_source_zone = source_zone
	if _deck_detail_title != null:
		_deck_detail_title.text = "%s｜%s ×%s" % [name, source_zone, str(count)]
	if _deck_detail_body != null:
		_deck_detail_body.clear()
		_deck_detail_body.append_text("ID：%s\n\n%s" % [cid, _card_desc(cid)])


func _try_restore_detail_selection() -> bool:
	if _detail_card_id.is_empty():
		return false
	var source := _detail_source_zone
	if source == "卡池":
		var pool_idx := _find_entry_index_by_card_id(_pool_stack_entries, _detail_card_id)
		if pool_idx >= 0:
			if _pool_list != null:
				_pool_list.select(pool_idx)
			_show_card_detail_from_entry(_pool_stack_entries[pool_idx], "卡池")
			return true
	elif source == "上场卡组":
		var deck_idx := _find_entry_index_by_card_id(_deck_stack_entries, _detail_card_id)
		if deck_idx >= 0:
			if _deck_list != null:
				_deck_list.select(deck_idx)
			_show_card_detail_from_entry(_deck_stack_entries[deck_idx], "上场卡组")
			return true
	var fallback_pool := _find_entry_index_by_card_id(_pool_stack_entries, _detail_card_id)
	if fallback_pool >= 0:
		if _pool_list != null:
			_pool_list.select(fallback_pool)
		_show_card_detail_from_entry(_pool_stack_entries[fallback_pool], "卡池")
		return true
	var fallback_deck := _find_entry_index_by_card_id(_deck_stack_entries, _detail_card_id)
	if fallback_deck >= 0:
		if _deck_list != null:
			_deck_list.select(fallback_deck)
		_show_card_detail_from_entry(_deck_stack_entries[fallback_deck], "上场卡组")
		return true
	return false


func _find_entry_index_by_card_id(entries: Array[Dictionary], card_id: String) -> int:
	for i in range(entries.size()):
		var entry: Dictionary = entries[i]
		if _entry_contains_card(entry, card_id):
			return i
	return -1


func _entry_contains_card(entry: Dictionary, card_id: String) -> bool:
	if str(entry.get("primary_id", "")) == card_id:
		return true
	var ids: Array = entry.get("ids", [])
	for one in ids:
		if str(one) == card_id:
			return true
	return false


func _remove_one(arr: Array[String], cid: String) -> bool:
	for i in range(arr.size()):
		if str(arr[i]) == cid:
			arr.remove_at(i)
			return true
	return false


func _take_one_from_entry_in_source(entry: Dictionary, source_cards: Array[String]) -> String:
	var ids: Array = entry.get("ids", [])
	for one in ids:
		var candidate := str(one).strip_edges()
		if candidate.is_empty():
			continue
		if _remove_one(source_cards, candidate):
			return candidate
	var primary := str(entry.get("primary_id", "")).strip_edges()
	if primary.is_empty():
		return ""
	if _remove_one(source_cards, primary):
		return primary
	return ""


func _card_desc(card_id: String) -> String:
	var cid := card_id.strip_edges()
	match cid:
		"short_long_novice":
			return "短线多-新手：未来5根有上涨得1分，失败扣2分。"
		"short_long_skilled":
			return "短线多-熟练：未来5根有上涨得2分，失败扣1分。"
		"short_long_veteran":
			return "短线多-老手：未来5根有上涨得3分，失败扣1分。"
		"short_long_master":
			return "短线多-大师：未来5根有上涨得3分，失败扣1分。"
		"short_short_novice":
			return "短线空-新手：未来5根有下跌得1分，失败扣2分。"
		"short_short_skilled":
			return "短线空-熟练：未来5根有下跌得2分，失败扣1分。"
		"short_short_veteran":
			return "短线空-老手：未来5根有下跌得3分，失败扣1分。"
		"short_short_master":
			return "短线空-大师：未来5根有下跌得3分，失败扣1分。"
		"trend_long_novice":
			return "顺势做多-新手：末根高于首根得3+X，失败扣4+X。"
		"trend_long_skilled":
			return "顺势做多-熟练：末根高于首根得6+X，失败扣6+X。"
		"trend_long_veteran":
			return "顺势做多-老手：末根高于首根得10+X，失败扣6+X。"
		"trend_long_master":
			return "顺势做多-大师：末根高于首根得15+X，失败扣6+X。"
		"trend_short_novice":
			return "顺势做空-新手：末根低于首根得3+X，失败扣4+X。"
		"trend_short_skilled":
			return "顺势做空-熟练：末根低于首根得6+X，失败扣6+X。"
		"trend_short_veteran":
			return "顺势做空-老手：末根低于首根得10+X，失败扣6+X。"
		"trend_short_master":
			return "顺势做空-大师：末根低于首根得15+X，失败扣6+X。"
		"breakout_long_novice":
			return "突破追多-新手：未来5根任一高点突破最近15根历史高点得20分，有动量得40分。"
		"breakout_long_veteran":
			return "突破追多-老手：未来5根任一高点突破最近15根历史高点得30分，有动量得80分。"
		"breakout_short_novice":
			return "突破追空-新手：未来5根任一低点跌破最近15根历史低点得20分，有动量得40分。"
		"breakout_short_veteran":
			return "突破追空-老手：未来5根任一低点跌破最近15根历史低点得30分，有动量得80分。"
		"tactic_quick_cancel":
			return "快速撤单：下回合额外抽1张，仅生效1回合，可叠加。"
		"tactic_scalp_cycle":
			return "剥头皮循环：短线小计*1.5，未来涨跌幅>3%时作废。"
		"tactic_leverage":
			return "借钱加杠杆：有得分则总分*2，否则信心-40。"
		"tactic_risk_control":
			return "风险控制：得分时*0.6，失分时*0.5。"
		"tactic_meditation":
			return "冥想思考：恢复信心5~15。"
		_:
			return "卡牌说明待补充。"


func _render_location_panels(location: String) -> void:
	var loc = location.strip_edges().to_lower()
	var at_home = loc == "home"
	if _home_scene != null:
		_home_scene.visible = at_home
	if _assoc_scene != null:
		_assoc_scene.visible = not at_home


func _card_name(card_id: String) -> String:
	return str(CARD_NAMES.get(card_id, card_id))


func _connect_and_boot(prefer_cached_state: bool = false) -> void:
	_set_action_enabled(false)
	_status("连接地图服务中...")
	var health: Dictionary = await _api.get_json("/v1/map/health")
	if not health.get("ok", false):
		_status("连接失败：%s" % str(health.get("message", "unknown")))
		_append_log("地图服务连接失败：%s" % str(health))
		return
	_set_action_enabled(true)
	_status("连接成功。")
	_append_log("地图服务连接成功。")
	if _map_run_id > 0:
		var cache_ok := prefer_cached_state and (not _state.is_empty()) and int(_state.get("map_run_id", 0)) == _map_run_id
		if not cache_ok:
			await _refresh_state()
	else:
		if _entry_mode == "create":
			_open_new_game_setup_overlay("登录重开")
		else:
			var resumed := await _resume_map_run()
			if not resumed:
				_open_new_game_setup_overlay("首次创建")


func _refresh_state() -> void:
	if _map_run_id <= 0:
		_state = {}
		_render_state()
		map_state_updated.emit({})
		return
	var res: Dictionary = await _api.post_json("/v1/map/run/state", {"map_run_id": _map_run_id})
	if not res.get("ok", false):
		_status("读取地图状态失败：%s" % str(res.get("message", "unknown")))
		_append_log("读取地图状态失败：%s" % str(res))
		return
	if not _apply_map_run_from_payload(res):
		_state = {}
		_render_state()
		map_state_updated.emit({})


func _render_state() -> void:
	if _state.is_empty():
		_end_log_marker = ""
		if _home_scene_stats_label != null:
			_home_scene_stats_label.text = "日期：-\n体力：-   金钱：-"
		if _home_attr_label != null:
			_home_attr_label.text = "请先新开或继续地图局。"
		_render_location_panels("home")
		_refresh_settings_overlay_info()
		return

	var status := str(_state.get("status", "unknown"))
	if _home_scene_stats_label != null:
		_home_scene_stats_label.text = "日期：%s\n体力：%s   金钱：%s" % [
			str(_state.get("date_label", "-")),
			str(_state.get("stamina", 0)),
			str(_state.get("money", 0)),
		]

	if _home_attr_label != null:
		var traits_arr: Array = _state.get("traits", [])
		var traits_text := "、".join(PackedStringArray(traits_arr))
		var style_answers: Dictionary = _state.get("style_answers", {})
		var stress_value := int(_state.get("stress", 0))
		var pressure_tip := "压力机制：本轮仅展示，不影响结算（占位）"
		if stress_value >= 200:
			pressure_tip = "压力>=200：死亡机制待接入（占位）"
		elif stress_value >= 100:
			pressure_tip = "压力>=100：躁郁效果待接入（占位）"
		_home_attr_label.text = "[基础信息]\n名字：%s\n日期：%s\n回合：%s / 72\n当前地点：%s\n\n[资源与状态]\n金钱：%s\n管理规模(AUM)：%s\n体力：%s / 100\n行动点：%s / 20\n压力：%s / 200\n名气：%s\n经验：%s\n信心：%s / 100\n\n[性格与风格]\n性格：%s\n交易周期：%s\n风险取向：%s\n优先偏好：%s\n无敌（测试）：%s\n\n[状态提示]\n%s\n地图信心归零将结束游戏（已接入）" % [
			str(_state.get("player_name", "-")),
			str(_state.get("date_label", "-")),
			str(_state.get("turn_index", 1)),
			str(_state.get("location_name", _state.get("location", "-"))),
			str(_state.get("money", 0)),
			str(_state.get("management_aum", 0)),
			str(_state.get("stamina", 0)),
			str(_state.get("action_points", 0)),
			str(_state.get("stress", 0)),
			str(_state.get("fame", 0)),
			str(_state.get("exp", 0)),
			str(_state.get("confidence", 0)),
			traits_text if not traits_text.is_empty() else "（未设置）",
			_style_label("horizon_preference", str(style_answers.get("horizon_preference", "long"))),
			_style_label("risk_preference", str(style_answers.get("risk_preference", "avoid_loss"))),
			_style_label("priority_preference", str(style_answers.get("priority_preference", "skill"))),
			"开启" if bool(_state.get("god_mode", false)) else "关闭",
			pressure_tip,
		]

	if _assoc_desc_label != null:
		_assoc_desc_label.text = "本版仅提供场景占位，用于验证地点切换与体力消耗。\n\n当前资源：体力%s | 金钱%s | 名气%s | 经验%s" % [
			str(_state.get("stamina", 0)),
			str(_state.get("money", 0)),
			str(_state.get("fame", 0)),
			str(_state.get("exp", 0)),
		]

	_render_location_panels(str(_state.get("location", "home")))
	_set_home_view(_home_view_mode)
	_refresh_settings_overlay_info()
	_refresh_outing_view_state()

	var ended := status == "ended"
	var at_home := str(_state.get("location", "")) == "home"
	_rest_btn.disabled = (not _actions_enabled) or ended or (not at_home)
	_move_assoc_btn.disabled = (not _actions_enabled) or ended
	_move_home_btn.disabled = (not _actions_enabled) or ended
	_battle_btn.disabled = (not _actions_enabled) or ended
	if _assoc_back_home_btn != null:
		_assoc_back_home_btn.disabled = (not _actions_enabled) or ended
	if _assoc_battle_btn != null:
		_assoc_battle_btn.disabled = (not _actions_enabled) or ended

	if bool(_state.get("deck_pending_apply", false)):
		_deck_hint_label.text = "卡组长度需 10~15 张；已保存，下一次新战斗将生效。"
	else:
		_deck_hint_label.text = "卡组长度需 10~15 张；保存后下次新战斗生效。"

	if ended:
		_status("地图局已结束。")
		var marker := "%s|%s|%s" % [str(_state.get("map_run_id", 0)), str(_state.get("turn_index", 0)), str(_state.get("ended_reason", ""))]
		if marker != _end_log_marker:
			_end_log_marker = marker
			_append_log("地图局结束：%s" % str(_state.get("ended_reason", "unknown")))


func _refresh_outing_view_state() -> void:
	if _outing_info_label == null:
		return
	var location := str(_state.get("location", "home"))
	var location_name := str(_state.get("location_name", location))
	var stamina := float(_state.get("stamina", 0.0))
	var money := float(_state.get("money", 0.0))
	var turn_index := int(_state.get("turn_index", 1))
	var ended := str(_state.get("status", "")) == "ended"
	_outing_info_label.text = "地点：%s\n日期：%s｜回合 %s/72\n体力：%.1f   金钱：%.1f" % [
		location_name,
		str(_state.get("date_label", "-")),
		str(turn_index),
		stamina,
		money,
	]
	if _outing_hint_label != null:
		_outing_hint_label.text = "移动规则：前往非住宅地点体力 -10｜返回住宅免费"
	_outing_refresh_poi_buttons()
	if _battle_btn != null:
		_battle_btn.disabled = (not _actions_enabled) or ended


func _create_map_run(new_game_setup: Dictionary = {}, restart_existing_active: bool = false) -> bool:
	var req: Dictionary = {}
	if restart_existing_active:
		req["restart_existing_active"] = true
	if not new_game_setup.is_empty():
		req["new_game_setup"] = new_game_setup
	var res: Dictionary = await _api.post_json("/v1/map/run/create", req)
	if not res.get("ok", false):
		_status("创建地图局失败：%s" % str(res.get("message", "unknown")))
		_append_log("创建地图局失败：%s" % str(res))
		return false
	_map_run_id = int(res.get("map_run_id", 0))
	var summary_v: Variant = res.get("applied_setup_summary", {})
	if summary_v is Dictionary and not (summary_v as Dictionary).is_empty():
		var summary: Dictionary = summary_v
		_append_log("角色设定完成：%s｜新局 MapRun#%s" % [str(summary.get("player_name", "-")), str(_map_run_id)])
	else:
		_append_log("已新开地图局：MapRun#%s" % str(_map_run_id))
	await _load_home_deck()
	await _refresh_state()
	return _map_run_id > 0


func _resume_map_run() -> bool:
	var res: Dictionary = await _api.post_json("/v1/map/run/resume", {})
	if not res.get("ok", false):
		_status("恢复地图局失败：%s" % str(res.get("message", "unknown")))
		_append_log("恢复地图局失败：%s" % str(res))
		return false
	var run_v: Variant = res.get("run", {})
	var run: Dictionary = run_v if typeof(run_v) == TYPE_DICTIONARY else {}
	if run.is_empty():
		_status("没有可恢复地图局，自动新开。")
		_append_log("没有可恢复地图局。")
		return false
	_map_run_id = int(run.get("map_run_id", 0))
	_append_log("已恢复地图局：MapRun#%s" % str(_map_run_id))
	_apply_state_snapshot(run)
	await _load_home_deck()
	return _map_run_id > 0


func _load_home_deck() -> void:
	if _map_run_id <= 0:
		return
	var res: Dictionary = await _api.post_json("/v1/map/home/deck/get", {"map_run_id": _map_run_id})
	if not res.get("ok", false):
		return
	var deck: Array = res.get("deck_cards", [])
	_editing_deck.clear()
	_pool_cards = _pool_ids.duplicate()
	for one in deck:
		var cid = str(one).strip_edges()
		if not cid.is_empty():
			if not _remove_one(_pool_cards, cid):
				_append_log("卡组载入跳过异常卡：%s（不在卡池库存）" % cid)
				continue
			_editing_deck.append(cid)
	_deck_autosave_dirty = false
	_deck_autosave_inflight = false
	if _deck_autosave_timer != null:
		_deck_autosave_timer.stop()
	_render_deck_lists()


func _commit_returned_battle(battle_run_id: int) -> void:
	if _map_run_id <= 0:
		return
	var res: Dictionary = await _api.post_json(
		"/v1/map/battle/commit",
		{"map_run_id": _map_run_id, "battle_run_id": int(battle_run_id)}
	)
	if not res.get("ok", false):
		_append_log("战斗回写失败：%s" % str(res.get("message", "unknown")))
		return
	_append_log(str(res.get("log_line", "战斗回写已完成。")))
	_apply_map_run_from_payload(res)
	# Returning from battle creates a fresh map scene instance in app_root.
	# Reload home deck after commit so the deck editor doesn't show an empty local cache.
	await _load_home_deck()


func _unhandled_input(event: InputEvent) -> void:
	if not (event is InputEventKey) or not event.pressed or event.echo:
		return
	if event.keycode != KEY_ESCAPE:
		return
	if _new_game_setup_overlay != null and _new_game_setup_overlay.visible:
		if not _setup_inflight:
			_toggle_new_game_setup_overlay(false)
		accept_event()
		return
	if _settings_overlay == null:
		return
	_toggle_settings(not _settings_overlay.visible)
	accept_event()


func _on_create_pressed() -> void:
	_open_new_game_setup_overlay("地图内新开")


func _on_resume_pressed() -> void:
	await _resume_map_run()


func _on_logout_pressed() -> void:
	logout_requested.emit(false)


func _on_home_attr_view_pressed() -> void:
	_set_home_view(HOME_VIEW_ATTR)


func _on_home_deck_view_pressed() -> void:
	_set_home_view(HOME_VIEW_DECK)


func _on_home_out_view_pressed() -> void:
	_set_home_view(HOME_VIEW_OUT)


func _on_home_back_pressed() -> void:
	_set_home_view(HOME_VIEW_HOME)


func _on_pool_item_selected(index: int) -> void:
	if index < 0 or index >= _pool_stack_entries.size():
		_update_deck_detail_for_empty()
		return
	_show_card_detail_from_entry(_pool_stack_entries[index], "卡池")


func _on_deck_item_selected(index: int) -> void:
	if index < 0 or index >= _deck_stack_entries.size():
		_update_deck_detail_for_empty()
		return
	_show_card_detail_from_entry(_deck_stack_entries[index], "上场卡组")


func _make_drag_preview(text: String) -> Control:
	var panel := PanelContainer.new()
	panel.custom_minimum_size = Vector2(220, 30)
	_style_panel(panel, Color(0.12, 0.19, 0.30, 0.95), Color(0.84, 0.72, 0.42, 0.98), 8)
	var lb := Label.new()
	lb.text = text
	lb.add_theme_color_override("font_color", Color(0.95, 0.98, 1.0, 1.0))
	panel.add_child(lb)
	return panel


func _pool_get_drag_data(at_position: Vector2) -> Variant:
	if _pool_list == null:
		return null
	var idx := _pool_list.get_item_at_position(at_position, true)
	if idx < 0 or idx >= _pool_stack_entries.size():
		return null
	var entry := _pool_stack_entries[idx]
	var data := {"type": "deck_pool_entry", "source": "pool", "entry": entry}
	_pool_list.set_drag_preview(_make_drag_preview("拖拽到上场卡组：%s" % str(entry.get("name", "-"))))
	return data


func _pool_can_drop_data(_at_position: Vector2, data: Variant) -> bool:
	if not (data is Dictionary):
		return false
	var d: Dictionary = data
	return str(d.get("type", "")) == "deck_pool_entry" and str(d.get("source", "")) == "deck"


func _pool_drop_data(_at_position: Vector2, data: Variant) -> void:
	if not _pool_can_drop_data(Vector2.ZERO, data):
		return
	var d: Dictionary = data
	var entry_v: Variant = d.get("entry", {})
	if not (entry_v is Dictionary):
		return
	var entry: Dictionary = entry_v
	var moved_cid := _take_one_from_entry_in_source(entry, _editing_deck)
	if moved_cid.is_empty():
		return
	_pool_cards.append(moved_cid)
	_render_deck_lists()
	_mark_deck_changed()


func _deck_get_drag_data(at_position: Vector2) -> Variant:
	if _deck_list == null:
		return null
	var idx := _deck_list.get_item_at_position(at_position, true)
	if idx < 0 or idx >= _deck_stack_entries.size():
		return null
	var entry := _deck_stack_entries[idx]
	var data := {"type": "deck_pool_entry", "source": "deck", "entry": entry}
	_deck_list.set_drag_preview(_make_drag_preview("拖拽回卡池：%s" % str(entry.get("name", "-"))))
	return data


func _deck_can_drop_data(_at_position: Vector2, data: Variant) -> bool:
	if not (data is Dictionary):
		return false
	var d: Dictionary = data
	return str(d.get("type", "")) == "deck_pool_entry" and str(d.get("source", "")) == "pool"


func _deck_drop_data(_at_position: Vector2, data: Variant) -> void:
	if not _deck_can_drop_data(Vector2.ZERO, data):
		return
	if _editing_deck.size() >= 15:
		_status("上场卡组最多15张。")
		return
	var d: Dictionary = data
	var entry_v: Variant = d.get("entry", {})
	if not (entry_v is Dictionary):
		return
	var entry: Dictionary = entry_v
	var moved_cid := _take_one_from_entry_in_source(entry, _pool_cards)
	if moved_cid.is_empty():
		_status("卡池该卡数量不足。")
		return
	_editing_deck.append(moved_cid)
	_render_deck_lists()
	_mark_deck_changed()


func _on_move_home_pressed() -> void:
	await _move_to("home")


func _on_move_assoc_pressed() -> void:
	await _move_to("association")


func _on_locked_location_pressed(name: String) -> void:
	_status("%s 暂未开放。" % name)
	_append_log("地点未开放：%s（占位）" % name)


func _move_to(location: String) -> void:
	if _map_run_id <= 0:
		return
	var res: Dictionary = await _api.post_json("/v1/map/location/move", {"map_run_id": _map_run_id, "to_location": location})
	if not res.get("ok", false):
		_status("地点切换失败：%s" % str(res.get("message", "unknown")))
		return
	_append_log(str(res.get("log_line", "地点切换完成。")))
	_apply_map_run_from_payload(res)


func _on_rest_pressed() -> void:
	if _map_run_id <= 0:
		return
	var res: Dictionary = await _api.post_json("/v1/map/turn/rest", {"map_run_id": _map_run_id})
	if not res.get("ok", false):
		_status("休息失败：%s" % str(res.get("message", "unknown")))
		return
	_append_log(str(res.get("log_line", "休息完成。")))
	_apply_map_run_from_payload(res)


func _on_start_battle_pressed() -> void:
	if _map_run_id <= 0:
		return
	if _battle_start_inflight:
		return
	_battle_start_inflight = true
	_set_action_enabled(false)
	battle_loading.emit(true, "正在读取战斗数据")
	_status("正在读取战斗数据...")
	var res: Dictionary = await _api.post_json("/v1/map/battle/start", {"map_run_id": _map_run_id})
	if not res.get("ok", false):
		_status("启动战斗失败：%s" % str(res.get("message", "unknown")))
		_append_log("启动战斗失败：%s" % str(res))
		battle_loading.emit(false, "")
		_battle_start_inflight = false
		_set_action_enabled(true)
		return
	var battle_run_id := int(res.get("battle_run_id", 0))
	if battle_run_id <= 0:
		_status("启动战斗失败：battle_run_id 无效")
		battle_loading.emit(false, "")
		_battle_start_inflight = false
		_set_action_enabled(true)
		return
	_append_log("已进入卡牌战斗测试：BattleRun#%s" % str(battle_run_id))
	launch_battle.emit(_map_run_id, battle_run_id)


func _on_add_card_pressed() -> void:
	if _pool_list == null:
		return
	var changed := false
	for idx in _pool_list.get_selected_items():
		if _editing_deck.size() >= 15:
			break
		var i = int(idx)
		if i < 0 or i >= _pool_stack_entries.size():
			continue
		var entry := _pool_stack_entries[i]
		var moved_cid := _take_one_from_entry_in_source(entry, _pool_cards)
		if moved_cid.is_empty():
			continue
		_editing_deck.append(moved_cid)
		changed = true
	_render_deck_lists()
	if changed:
		_mark_deck_changed()


func _on_remove_card_pressed() -> void:
	if _deck_list == null:
		return
	var selected := _deck_list.get_selected_items()
	selected.reverse()
	var changed := false
	for idx in selected:
		var i = int(idx)
		if i < 0 or i >= _deck_stack_entries.size():
			continue
		var entry := _deck_stack_entries[i]
		var moved_cid := _take_one_from_entry_in_source(entry, _editing_deck)
		if moved_cid.is_empty():
			continue
		_pool_cards.append(moved_cid)
		changed = true
	_render_deck_lists()
	if changed:
		_mark_deck_changed()


func _on_sort_direction_pressed() -> void:
	if _sort_mode == "direction":
		_sort_mode = "text"
	else:
		_sort_mode = "direction"
	_update_sort_mode_buttons()
	_render_deck_lists()


func _on_sort_level_pressed() -> void:
	if _sort_mode == "level":
		_sort_mode = "text"
	else:
		_sort_mode = "level"
	_update_sort_mode_buttons()
	_render_deck_lists()


func _update_sort_mode_buttons() -> void:
	_set_sort_button_style(_direction_sort_btn, _sort_mode == "direction")
	_set_sort_button_style(_level_sort_btn, _sort_mode == "level")


func _set_sort_button_style(btn: Button, active: bool) -> void:
	if btn == null:
		return
	var normal := StyleBoxFlat.new()
	normal.bg_color = Color(0.16, 0.24, 0.36, 0.96) if active else Color(0.12, 0.18, 0.28, 0.96)
	normal.border_color = Color(0.90, 0.77, 0.42, 1.0) if active else Color(0.44, 0.62, 0.83, 0.92)
	normal.set_border_width_all(2 if active else 1)
	normal.set_corner_radius_all(7)
	btn.add_theme_stylebox_override("normal", normal)
	btn.add_theme_stylebox_override("pressed", normal)
	var hover := normal.duplicate()
	hover.bg_color = Color(0.20, 0.29, 0.43, 0.98) if active else Color(0.16, 0.24, 0.36, 0.98)
	btn.add_theme_stylebox_override("hover", hover)
	btn.add_theme_color_override("font_color", Color(0.97, 0.91, 0.73, 1.0) if active else Color(0.80, 0.90, 1.0, 1.0))


func _on_save_deck_pressed() -> void:
	await _save_deck_to_server(false)


func _mark_deck_changed() -> void:
	_deck_autosave_dirty = true
	if _deck_hint_label != null:
		_deck_hint_label.text = "卡组已变更，自动保存中..."
	if _deck_autosave_timer == null:
		return
	_deck_autosave_timer.start(DECK_AUTOSAVE_DELAY_SEC)


func _on_deck_autosave_timeout() -> void:
	if _deck_autosave_inflight:
		_deck_autosave_dirty = true
		if _deck_autosave_timer != null:
			_deck_autosave_timer.start(DECK_AUTOSAVE_DELAY_SEC)
		return
	if not _deck_autosave_dirty:
		return
	if _map_run_id <= 0:
		return
	if _editing_deck.size() < 10 or _editing_deck.size() > 15:
		if _deck_hint_label != null:
			_deck_hint_label.text = "卡组长度需 10~15 张；当前不满足，暂未自动保存。"
		return
	_deck_autosave_inflight = true
	_deck_autosave_dirty = false
	var ok := await _save_deck_to_server(true)
	_deck_autosave_inflight = false
	if _deck_autosave_dirty and _deck_autosave_timer != null:
		_deck_autosave_timer.start(DECK_AUTOSAVE_DELAY_SEC)
	elif (not ok) and _deck_autosave_timer != null:
		_deck_autosave_dirty = true
		_deck_autosave_timer.start(DECK_AUTOSAVE_DELAY_SEC)


func _save_deck_to_server(is_auto: bool) -> bool:
	if _map_run_id <= 0:
		return false
	if _editing_deck.size() < 10 or _editing_deck.size() > 15:
		if not is_auto:
			_status("卡组长度必须 10~15 张。")
		return false
	var res: Dictionary = await _api.post_json(
		"/v1/map/home/deck/save",
		{"map_run_id": _map_run_id, "deck_cards": _editing_deck}
	)
	if not res.get("ok", false):
		var msg := str(res.get("message", "unknown"))
		if is_auto:
			_status("自动保存失败：%s" % msg)
			if _deck_hint_label != null:
				_deck_hint_label.text = "自动保存失败，稍后重试。"
		else:
			_status("保存卡组失败：%s" % msg)
		return false
	_apply_map_run_from_payload(res)
	if is_auto:
		if _deck_hint_label != null:
			_deck_hint_label.text = "卡组已自动保存（本地图局生效）。"
	else:
		_append_log(str(res.get("log_line", "卡组保存成功。")))
	return true
