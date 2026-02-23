extends Control

signal logout_requested(clear_saved_session: bool)
signal battle_exit_requested(map_run_id: int, battle_run_id: int)

const API_CLIENT_SCRIPT := preload("res://scripts/api_client.gd")
const RUN_STATE_SCRIPT := preload("res://scripts/run_state.gd")
const KLINE_CHART_VIEW_SCRIPT := preload("res://scripts/kline_chart_view.gd")
const CARD_TILE_SCRIPT := preload("res://scripts/card_tile.gd")
const QUEUE_DROP_ZONE_SCRIPT := preload("res://scripts/queue_drop_zone.gd")
const HAND_DROP_ZONE_SCRIPT := preload("res://scripts/hand_drop_zone.gd")
const CARD_VISUALS_RESOURCE := preload("res://themes/card_visuals.tres")

@export var standalone_login_mode: bool = true

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
	"option_buy_put_novice": "买看跌做空-新手",
	"option_buy_put_skilled": "买看跌做空-熟练",
	"option_buy_put_veteran": "买看跌做空-老手",
	"option_buy_put_master": "买看跌做空-大师",
	"option_sell_call_novice": "卖看涨做空-新手",
	"option_sell_call_skilled": "卖看涨做空-熟练",
	"option_sell_call_veteran": "卖看涨做空-老手",
	"option_sell_call_master": "卖看涨做空-大师",
	"option_sell_put_novice": "卖看跌做多-新手",
	"option_sell_put_skilled": "卖看跌做多-熟练",
	"option_sell_put_veteran": "卖看跌做多-老手",
	"option_sell_put_master": "卖看跌做多-大师",
}

const CARD_TYPE_LABELS := {
	"short": "短线",
	"trend": "趋势",
	"breakout": "突破",
	"tactic": "战术",
	"arbitrage": "套利",
	"option": "期权",
}

const CARD_SHORT_DESC := {
	"short_long_novice": "未来5根需3根上涨得1分，失败扣2分。",
	"short_long_skilled": "未来5根需2根上涨得2分，失败扣1分。",
	"short_long_veteran": "未来5根需2根上涨得3分，失败扣1分。",
	"short_long_master": "未来5根需1根上涨得4分，失败扣1分。",
	"short_short_novice": "未来5根需3根下跌得1分，失败扣2分。",
	"short_short_skilled": "未来5根需2根下跌得2分，失败扣2分。",
	"short_short_veteran": "未来5根需2根下跌得3分，失败扣1分。",
	"short_short_master": "未来5根需1根下跌得4分，失败扣1分。",
	"trend_long_novice": "末根高于首根得3+X，失败扣4+X。",
	"trend_long_skilled": "末根高于首根得6+X，失败扣6+X。",
	"trend_long_veteran": "末根高于首根得10+X，失败扣6+X。",
	"trend_long_master": "末根高于首根得15+X，失败扣6+X。",
	"trend_short_novice": "末根低于首根得3+X，失败扣4+X。",
	"trend_short_skilled": "末根低于首根得6+X，失败扣6+X。",
	"trend_short_veteran": "末根低于首根得10+X，失败扣6+X。",
	"trend_short_master": "末根低于首根得15+X，失败扣6+X。",
	"breakout_long_novice": "未来5根任一收盘突破最近15根历史高点得20分，有动量得30分。",
	"breakout_long_veteran": "未来5根任一收盘突破最近15根历史高点得30分，有动量得60分。",
	"breakout_short_novice": "未来5根任一收盘跌破最近15根历史低点得20分，有动量得30分。",
	"breakout_short_veteran": "未来5根任一收盘跌破最近15根历史低点得30分，有动量得60分。",
	"tactic_quick_cancel": "下回合额外抽1张，可叠加。",
	"tactic_scalp_cycle": "短线小计*1.5，未来涨跌幅>3%作废。",
	"tactic_leverage": "有得分则总分*2，否则信心-40。",
	"tactic_risk_control": "得分时*0.6，失分时*0.5。",
	"tactic_meditation": "恢复信心5~15。",
	"tactic_dynamic_adjust": "下回合抽牌前先弃掉剩余手牌，再补抽。",
	"tactic_self_confidence": "需信心>=80；该牌结算点总分>0则总分*2，否则信心-20。",
	"tactic_fast_stop": "保护后两张牌最终负分；突破与买方期权不可保护。",
	"arb_east_novice": "套利成功+2，失败-2（需连段配对）。",
	"arb_east_veteran": "套利成功+3，失败-1（需连段配对）。",
	"arb_west_novice": "套利成功+2，失败-2（需连段配对）。",
	"arb_west_veteran": "套利成功+3，失败-1（需连段配对）。",
	"arb_south_novice": "套利成功+2，失败-2（需连段配对）。",
	"arb_south_veteran": "套利成功+3，失败-1（需连段配对）。",
	"arb_north_novice": "套利成功+2，失败-2（需连段配对）。",
	"arb_north_veteran": "套利成功+3，失败-1（需连段配对）。",
	"option_buy_call_novice": "先扣5；成功得(Y-2)*4，暴击再*2。",
	"option_buy_call_skilled": "先扣4；成功得(Y-2)*5，暴击再*2。",
	"option_buy_call_veteran": "先扣4；成功得(Y-2)*6，暴击再*2。",
	"option_buy_call_master": "先扣3；成功得(Y-2)*8，暴击再*2。",
	"option_buy_put_novice": "先扣5；成功得(Z-2)*4，暴击再*2。",
	"option_buy_put_skilled": "先扣4；成功得(Z-2)*5，暴击再*2。",
	"option_buy_put_veteran": "先扣4；成功得(Z-2)*6，暴击再*2。",
	"option_buy_put_master": "先扣3；成功得(Z-2)*8，暴击再*2。",
	"option_sell_call_novice": "成功+3；失败-16（严重失败倍率更高）。",
	"option_sell_call_skilled": "成功+2；失败-12（严重失败倍率更高）。",
	"option_sell_call_veteran": "成功+2；失败-8（严重失败倍率更高）。",
	"option_sell_call_master": "成功+2；失败-4（严重失败倍率更高）。",
	"option_sell_put_novice": "成功+3；失败-16（严重失败倍率更高）。",
	"option_sell_put_skilled": "成功+2；失败-10（严重失败倍率更高）。",
	"option_sell_put_veteran": "成功+2；失败-8（严重失败倍率更高）。",
	"option_sell_put_master": "成功+2；失败-4（严重失败倍率更高）。",
}

const HAND_CARD_WIDTH := 164.0
const HAND_CARD_HEIGHT := 236.0
const HAND_CARD_SPREAD_MIN := 42.0
const HAND_CARD_SPREAD_MAX := 120.0
const DEV_METRIC_WINDOW := 20

var _api: Node
var _state = RUN_STATE_SCRIPT.new()
var _queue_cards: Array[String] = []
var _hand_view_cards: Array[String] = []
var _pending_candidates: Array = []
var _last_meta: Dictionary = {}
var _actions_enabled: bool = false
var _selected_hand_idx: int = -1
var _selected_queue_idx: int = -1
var _queue_drop_insert_idx: int = -1
var _card_visuals: Resource = CARD_VISUALS_RESOURCE
var _sfx_players: Dictionary = {}
var _fallback_sfx_player: AudioStreamPlayer
var _fallback_sfx_stream: AudioStreamGenerator

var _status_label: Label
var _run_label: Label
var _meta_label: Label
var _stage_label: Label
var _log_box: RichTextLabel
var _chart_view: Control
var _fx_score_label: Label
var _fx_conf_label: Label
var _fx_stage_label: Label
var _turn_info_panel: PanelContainer
var _pile_label: Label
var _hand_stat_label: Label
var _turn_hint_label: Label
var _log_summary_title: Label
var _settle_panel: PanelContainer
var _settle_delta_label: Label
var _settle_detail_label: Label
var _settle_stage_bar: ProgressBar
var _settle_conf_bar: ProgressBar
var _settle_stage_value: Label
var _settle_conf_value: Label

var _base_url_input: LineEdit
var _username_input: LineEdit
var _token_input: LineEdit
var _login_panel: PanelContainer

var _connect_btn: Button
var _meta_btn: Button
var _create_run_btn: Button
var _resume_btn: Button
var _refresh_btn: Button
var _prepare_stage_btn: Button
var _finish_run_btn: Button
var _logout_btn: Button

var _hand_cards_stage: Control
var _hand_pile_layer: Control
var _queue_cards_box: VBoxContainer
var _hand_focus_label: Label
var _hand_drop_zone: PanelContainer
var _draw_pile_btn: Button
var _discard_pile_btn: Button
var _draw_pile_badge: Label
var _discard_pile_badge: Label
var _queue_drop_zone: PanelContainer
var _main_vsplit: VSplitContainer
var _battle_split: HSplitContainer
var _bottom_split: HSplitContainer
var _cand_container: VBoxContainer
var _cand_panel: PanelContainer
var _upgrade_container: VBoxContainer
var _floating_fx_layer: Control

var _add_queue_btn: Button
var _queue_execute_btn: Button
var _pass_btn: Button
var _settings_overlay: ColorRect
var _settings_modal: PanelContainer
var _settings_home_btn: Button
var _settings_exit_battle_btn: Button
var _settings_quit_btn: Button
var _settings_run_summary_label: Label
var _settings_version_label: Label
var _pile_view_dialog: AcceptDialog
var _pile_view_title: Label
var _pile_view_body: RichTextLabel
var _story_overlay: ColorRect
var _story_panel: PanelContainer
var _story_name_label: Label
var _story_text_label: RichTextLabel
var _story_next_btn: Button
var _story_skip_btn: Button
var _story_active: bool = false
var _story_lines: Array[Dictionary] = []
var _story_line_idx: int = 0
var _story_shown_stage_marker: String = ""
var _story_transitioning: bool = false
var _boot_entry_mode: String = "resume"
var _hand_layout_reflow_pending: bool = false
var _turn_action_inflight: bool = false
var _last_layout_viewport_size: Vector2 = Vector2.ZERO
var _dev_metrics_box: RichTextLabel
var _dev_turn_metrics: Array = []
var _dev_metrics_run_id: int = 0
var _map_mode: bool = false
var _map_run_id: int = 0
var _map_battle_run_id: int = 0
var _map_exit_emitted: bool = false


func _ready() -> void:
	randomize()
	_api = API_CLIENT_SCRIPT.new()
	add_child(_api)
	_build_ui()
	_init_sfx()
	if not get_viewport().size_changed.is_connected(_on_viewport_size_changed):
		get_viewport().size_changed.connect(_on_viewport_size_changed)
	if standalone_login_mode:
		_set_status("客户端就绪。先填写 URL + 用户名 + Token，然后连接。")
	else:
		_set_status("客户端就绪。等待登录会话初始化。")
	_update_action_enabled(false)


func _build_ui() -> void:
	var bg := TextureRect.new()
	bg.set_anchors_preset(Control.PRESET_FULL_RECT)
	bg.mouse_filter = Control.MOUSE_FILTER_IGNORE
	bg.z_index = -100
	bg.stretch_mode = TextureRect.STRETCH_SCALE
	var grad := Gradient.new()
	grad.offsets = PackedFloat32Array([0.0, 0.58, 1.0])
	grad.colors = PackedColorArray([
		Color(0.04, 0.06, 0.10, 1.0),
		Color(0.03, 0.12, 0.16, 1.0),
		Color(0.02, 0.07, 0.10, 1.0),
	])
	var grad_tex := GradientTexture2D.new()
	grad_tex.gradient = grad
	grad_tex.fill = GradientTexture2D.FILL_LINEAR
	grad_tex.fill_from = Vector2(0.5, 0.0)
	grad_tex.fill_to = Vector2(0.5, 1.0)
	bg.texture = grad_tex
	add_child(bg)

	var root := VBoxContainer.new()
	root.set_anchors_preset(Control.PRESET_FULL_RECT)
	root.size_flags_vertical = Control.SIZE_EXPAND_FILL
	root.add_theme_constant_override("separation", 7)
	add_child(root)

	_status_label = Label.new()

	_login_panel = PanelContainer.new()
	root.add_child(_login_panel)
	var login_box := VBoxContainer.new()
	login_box.add_theme_constant_override("separation", 6)
	_login_panel.add_child(login_box)

	_base_url_input = _line("API Base URL", "http://127.0.0.1:8787")
	_username_input = _line("Username", "")
	_token_input = _line("Token", "")
	_token_input.secret = true

	login_box.add_child(_row("API地址", _base_url_input))
	login_box.add_child(_row("用户名", _username_input))
	login_box.add_child(_row("Token", _token_input))

	var login_btn_row := HBoxContainer.new()
	login_box.add_child(login_btn_row)
	_connect_btn = Button.new()
	_connect_btn.text = "连接并鉴权"
	_connect_btn.pressed.connect(_on_connect_pressed)
	login_btn_row.add_child(_connect_btn)
	_meta_btn = Button.new()
	_meta_btn.text = "刷新独立经验"
	_meta_btn.pressed.connect(_on_refresh_meta_pressed)
	login_btn_row.add_child(_meta_btn)

	var hud_panel := PanelContainer.new()
	root.add_child(hud_panel)
	var hud_box := VBoxContainer.new()
	hud_box.add_theme_constant_override("separation", 4)
	hud_panel.add_child(hud_box)

	var lobby_btn_row := HBoxContainer.new()
	lobby_btn_row.add_theme_constant_override("separation", 6)
	hud_box.add_child(lobby_btn_row)
	lobby_btn_row.visible = false
	_create_run_btn = Button.new()
	_create_run_btn.text = "新开一局"
	_create_run_btn.pressed.connect(_on_create_run_pressed)
	lobby_btn_row.add_child(_create_run_btn)
	_resume_btn = Button.new()
	_resume_btn.text = "继续未完成局"
	_resume_btn.pressed.connect(_on_resume_pressed)
	lobby_btn_row.add_child(_resume_btn)
	_refresh_btn = Button.new()
	_refresh_btn.text = "刷新局状态"
	_refresh_btn.pressed.connect(_on_refresh_state_pressed)
	lobby_btn_row.add_child(_refresh_btn)
	_prepare_stage_btn = Button.new()
	_prepare_stage_btn.text = "准备当前关卡"
	_prepare_stage_btn.pressed.connect(_on_prepare_stage_pressed)
	lobby_btn_row.add_child(_prepare_stage_btn)
	_finish_run_btn = Button.new()
	_finish_run_btn.text = "领取整局结算"
	_finish_run_btn.pressed.connect(_on_finish_run_pressed)
	lobby_btn_row.add_child(_finish_run_btn)
	_logout_btn = Button.new()
	_logout_btn.text = "断开连接" if standalone_login_mode else "切换账号"
	_logout_btn.pressed.connect(_on_logout_pressed)
	lobby_btn_row.add_child(_logout_btn)

	var state_row := HBoxContainer.new()
	state_row.add_theme_constant_override("separation", 8)
	hud_box.add_child(state_row)
	state_row.visible = false
	_meta_label = Label.new()
	_meta_label.text = "Meta: -"
	_meta_label.size_flags_horizontal = Control.SIZE_EXPAND_FILL
	_meta_label.clip_text = true
	state_row.add_child(_meta_label)
	_run_label = Label.new()
	_run_label.text = "Run: -"
	_run_label.size_flags_horizontal = Control.SIZE_EXPAND_FILL
	_run_label.clip_text = true
	state_row.add_child(_run_label)
	_stage_label = Label.new()
	_stage_label.text = "Stage: -"
	_stage_label.size_flags_horizontal = Control.SIZE_EXPAND_FILL
	_stage_label.clip_text = true
	state_row.add_child(_stage_label)

	_build_settlement_panel(hud_box)

	var fx_panel := PanelContainer.new()
	fx_panel.size_flags_horizontal = Control.SIZE_EXPAND_FILL
	hud_box.add_child(fx_panel)
	var fx_row := HBoxContainer.new()
	fx_row.add_theme_constant_override("separation", 4)
	fx_panel.add_child(fx_row)
	_fx_score_label = _make_fx_label("回合得分 Δ 0")
	_fx_conf_label = _make_fx_label("信心变化 Δ 0")
	_fx_stage_label = _make_fx_label("关卡分变化 Δ 0")
	fx_row.add_child(_fx_score_label)
	fx_row.add_child(_fx_conf_label)
	fx_row.add_child(_fx_stage_label)

	_main_vsplit = VSplitContainer.new()
	_main_vsplit.size_flags_vertical = Control.SIZE_EXPAND_FILL
	root.add_child(_main_vsplit)

	_battle_split = HSplitContainer.new()
	_battle_split.size_flags_vertical = Control.SIZE_EXPAND_FILL
	_main_vsplit.add_child(_battle_split)

	var chart_panel := PanelContainer.new()
	chart_panel.size_flags_horizontal = Control.SIZE_EXPAND_FILL
	chart_panel.size_flags_vertical = Control.SIZE_EXPAND_FILL
	_battle_split.add_child(chart_panel)
	var chart_box := VBoxContainer.new()
	chart_panel.add_child(chart_box)
	_chart_view = KLINE_CHART_VIEW_SCRIPT.new()
	_chart_view.custom_minimum_size = Vector2(0, 190)
	_chart_view.size_flags_horizontal = Control.SIZE_EXPAND_FILL
	_chart_view.size_flags_vertical = Control.SIZE_EXPAND_FILL
	chart_box.add_child(_chart_view)

	var side_col := VBoxContainer.new()
	side_col.custom_minimum_size = Vector2(320, 0)
	side_col.size_flags_vertical = Control.SIZE_EXPAND_FILL
	side_col.add_theme_constant_override("separation", 8)
	_battle_split.add_child(side_col)

	var bottom_tabs := TabContainer.new()
	bottom_tabs.size_flags_vertical = Control.SIZE_EXPAND_FILL
	bottom_tabs.custom_minimum_size = Vector2(0, 100)
	bottom_tabs.use_hidden_tabs_for_min_size = true
	side_col.add_child(bottom_tabs)

	_cand_panel = PanelContainer.new()
	_cand_panel.name = "标的选择"
	bottom_tabs.add_child(_cand_panel)
	var cand_box := VBoxContainer.new()
	cand_box.add_theme_constant_override("separation", 6)
	_cand_panel.add_child(cand_box)
	var cand_title := Label.new()
	cand_title.text = "候选标的（仅在选关阶段显示）"
	cand_box.add_child(cand_title)
	_cand_container = VBoxContainer.new()
	cand_box.add_child(_cand_container)

	var upgrade_panel := PanelContainer.new()
	upgrade_panel.name = "关卡强化"
	bottom_tabs.add_child(upgrade_panel)
	var up_box := VBoxContainer.new()
	up_box.add_theme_constant_override("separation", 6)
	upgrade_panel.add_child(up_box)
	var up_title := Label.new()
	up_title.text = "关卡强化（二选一）"
	up_box.add_child(up_title)
	_upgrade_container = VBoxContainer.new()
	up_box.add_child(_upgrade_container)

	var log_panel := PanelContainer.new()
	log_panel.name = "操作日志"
	bottom_tabs.add_child(log_panel)
	var log_box := VBoxContainer.new()
	log_box.add_theme_constant_override("separation", 6)
	log_panel.add_child(log_box)
	_log_summary_title = Label.new()
	_log_summary_title.text = "牌局摘要"
	_log_summary_title.add_theme_color_override("font_color", Color(0.82, 0.90, 0.98, 1.0))
	log_box.add_child(_log_summary_title)
	_pile_label = Label.new()
	_pile_label.text = "牌堆 - | 手牌 -/-"
	_pile_label.add_theme_color_override("font_color", Color(0.90, 0.95, 1.0, 1.0))
	log_box.add_child(_pile_label)
	_hand_stat_label = Label.new()
	_hand_stat_label.text = "回合 - | 待执行 -"
	_hand_stat_label.add_theme_color_override("font_color", Color(0.78, 0.86, 0.98, 1.0))
	log_box.add_child(_hand_stat_label)
	_turn_hint_label = Label.new()
	_turn_hint_label.text = "提示：连接后可查看当前回合信息。"
	_turn_hint_label.add_theme_color_override("font_color", Color(0.66, 0.80, 0.96, 1.0))
	log_box.add_child(_turn_hint_label)
	var log_title := Label.new()
	log_title.text = "操作日志"
	log_box.add_child(log_title)
	_log_box = RichTextLabel.new()
	_log_box.fit_content = false
	_log_box.scroll_active = true
	_log_box.size_flags_vertical = Control.SIZE_EXPAND_FILL
	_log_box.custom_minimum_size = Vector2(0, 120)
	log_box.add_child(_log_box)

	var dev_panel := PanelContainer.new()
	dev_panel.name = "开发仪表"
	bottom_tabs.add_child(dev_panel)
	var dev_box := VBoxContainer.new()
	dev_box.add_theme_constant_override("separation", 6)
	dev_panel.add_child(dev_box)
	var dev_title := Label.new()
	dev_title.text = "数值仪表（近20回合）"
	dev_title.add_theme_color_override("font_color", Color(0.86, 0.94, 1.0, 1.0))
	dev_box.add_child(dev_title)
	_dev_metrics_box = RichTextLabel.new()
	_dev_metrics_box.fit_content = false
	_dev_metrics_box.scroll_active = true
	_dev_metrics_box.size_flags_vertical = Control.SIZE_EXPAND_FILL
	_dev_metrics_box.custom_minimum_size = Vector2(0, 120)
	dev_box.add_child(_dev_metrics_box)
	_render_dev_metrics()

	_bottom_split = HSplitContainer.new()
	_bottom_split.custom_minimum_size = Vector2(0, 160)
	_bottom_split.size_flags_vertical = Control.SIZE_EXPAND_FILL
	_main_vsplit.add_child(_bottom_split)

	var hand_panel := PanelContainer.new()
	hand_panel.size_flags_horizontal = Control.SIZE_EXPAND_FILL
	hand_panel.size_flags_vertical = Control.SIZE_EXPAND_FILL
	_bottom_split.add_child(hand_panel)
	var hand_outer := VBoxContainer.new()
	hand_outer.add_theme_constant_override("separation", 5)
	hand_panel.add_child(hand_outer)
	var hand_head_row := HBoxContainer.new()
	hand_outer.add_child(hand_head_row)
	var hand_title := Label.new()
	hand_title.text = "手牌（当前）"
	hand_title.add_theme_color_override("font_color", Color(0.90, 0.95, 1.0, 1.0))
	hand_head_row.add_child(hand_title)
	var hand_spacer := Control.new()
	hand_spacer.size_flags_horizontal = Control.SIZE_EXPAND_FILL
	hand_head_row.add_child(hand_spacer)
	_hand_focus_label = Label.new()
	_hand_focus_label.text = "选中卡牌：-"
	_hand_focus_label.add_theme_color_override("font_color", Color(0.62, 0.74, 0.89, 1.0))
	hand_head_row.add_child(_hand_focus_label)
	_hand_drop_zone = HAND_DROP_ZONE_SCRIPT.new()
	_hand_drop_zone.custom_minimum_size = Vector2(0, HAND_CARD_HEIGHT + 28.0)
	_hand_drop_zone.size_flags_vertical = Control.SIZE_EXPAND_FILL
	if _hand_drop_zone.has_signal("queue_card_returned"):
		_hand_drop_zone.queue_card_returned.connect(_on_hand_drop_from_queue)
	hand_outer.add_child(_hand_drop_zone)
	var hand_shell := VBoxContainer.new()
	hand_shell.size_flags_vertical = Control.SIZE_EXPAND_FILL
	hand_shell.add_theme_constant_override("separation", 4)
	_hand_drop_zone.add_child(hand_shell)
	var hand_hint := Label.new()
	hand_hint.text = "拖拽手牌到等待区；也可把等待区卡牌拖回这里撤销"
	hand_hint.add_theme_color_override("font_color", Color(0.60, 0.74, 0.92, 1.0))
	hand_hint.add_theme_font_size_override("font_size", 12)
	hand_shell.add_child(hand_hint)
	_hand_cards_stage = Control.new()
	_hand_cards_stage.size_flags_horizontal = Control.SIZE_EXPAND_FILL
	_hand_cards_stage.size_flags_vertical = Control.SIZE_EXPAND_FILL
	_hand_cards_stage.custom_minimum_size = Vector2(0, HAND_CARD_HEIGHT + 10.0)
	_hand_cards_stage.mouse_filter = Control.MOUSE_FILTER_PASS
	_hand_cards_stage.clip_contents = false
	hand_shell.add_child(_hand_cards_stage)
	_hand_pile_layer = Control.new()
	_hand_pile_layer.set_anchors_preset(Control.PRESET_FULL_RECT)
	_hand_pile_layer.mouse_filter = Control.MOUSE_FILTER_IGNORE
	_hand_pile_layer.z_index = 900
	_hand_pile_layer.set_meta("keep_on_clear", true)
	_hand_cards_stage.add_child(_hand_pile_layer)

	var pile_btn_layer := HBoxContainer.new()
	pile_btn_layer.mouse_filter = Control.MOUSE_FILTER_IGNORE
	pile_btn_layer.anchor_left = 1.0
	pile_btn_layer.anchor_top = 1.0
	pile_btn_layer.anchor_right = 1.0
	pile_btn_layer.anchor_bottom = 1.0
	pile_btn_layer.offset_left = -74.0
	pile_btn_layer.offset_top = -32.0
	pile_btn_layer.offset_right = -6.0
	pile_btn_layer.offset_bottom = -4.0
	pile_btn_layer.alignment = BoxContainer.ALIGNMENT_END
	pile_btn_layer.add_theme_constant_override("separation", 4)
	_hand_pile_layer.add_child(pile_btn_layer)
	var draw_btn_pack := _build_pile_button("抽牌堆", "抽", _on_open_draw_pile_pressed)
	_draw_pile_btn = draw_btn_pack.get("button")
	_draw_pile_badge = draw_btn_pack.get("badge")
	pile_btn_layer.add_child(_draw_pile_btn)
	var discard_btn_pack := _build_pile_button("弃牌堆", "弃", _on_open_discard_pile_pressed)
	_discard_pile_btn = discard_btn_pack.get("button")
	_discard_pile_badge = discard_btn_pack.get("badge")
	pile_btn_layer.add_child(_discard_pile_btn)

	var queue_panel := PanelContainer.new()
	queue_panel.custom_minimum_size = Vector2(280, 0)
	queue_panel.size_flags_vertical = Control.SIZE_EXPAND_FILL
	_bottom_split.add_child(queue_panel)
	var queue_shell_root := VBoxContainer.new()
	queue_shell_root.size_flags_vertical = Control.SIZE_EXPAND_FILL
	queue_shell_root.add_theme_constant_override("separation", 6)
	queue_panel.add_child(queue_shell_root)
	var queue_title := Label.new()
	queue_title.text = "等待区（执行顺序）"
	queue_title.add_theme_color_override("font_color", Color(0.90, 0.95, 1.0, 1.0))
	queue_shell_root.add_child(queue_title)
	_queue_drop_zone = QUEUE_DROP_ZONE_SCRIPT.new()
	_queue_drop_zone.size_flags_vertical = Control.SIZE_EXPAND_FILL
	_queue_drop_zone.custom_minimum_size = Vector2(0, 136)
	_queue_drop_zone.hand_card_dropped.connect(_on_queue_drop_from_hand)
	if _queue_drop_zone.has_signal("queue_card_dropped"):
		_queue_drop_zone.queue_card_dropped.connect(_on_queue_drop_reorder)
	if _queue_drop_zone.has_signal("drag_hover"):
		_queue_drop_zone.drag_hover.connect(_on_queue_drag_hover)
	if _queue_drop_zone.has_signal("drag_ended"):
		_queue_drop_zone.drag_ended.connect(_on_queue_drag_end)
	queue_shell_root.add_child(_queue_drop_zone)
	var queue_shell := VBoxContainer.new()
	queue_shell.size_flags_vertical = Control.SIZE_EXPAND_FILL
	_queue_drop_zone.add_child(queue_shell)
	var queue_scroll := ScrollContainer.new()
	queue_scroll.size_flags_horizontal = Control.SIZE_EXPAND_FILL
	queue_scroll.size_flags_vertical = Control.SIZE_EXPAND_FILL
	queue_scroll.horizontal_scroll_mode = ScrollContainer.SCROLL_MODE_SHOW_NEVER
	queue_scroll.vertical_scroll_mode = ScrollContainer.SCROLL_MODE_SHOW_ALWAYS
	queue_shell.add_child(queue_scroll)
	_queue_cards_box = VBoxContainer.new()
	_queue_cards_box.size_flags_horizontal = Control.SIZE_EXPAND_FILL
	_queue_cards_box.add_theme_constant_override("separation", 6)
	queue_scroll.add_child(_queue_cards_box)

	var queue_btn_row := HBoxContainer.new()
	queue_shell_root.add_child(queue_btn_row)
	_pass_btn = Button.new()
	_pass_btn.text = "PASS"
	_pass_btn.pressed.connect(_on_pass_pressed)
	queue_btn_row.add_child(_pass_btn)
	var queue_btn_spacer := Control.new()
	queue_btn_spacer.size_flags_horizontal = Control.SIZE_EXPAND_FILL
	queue_btn_row.add_child(queue_btn_spacer)
	_queue_execute_btn = Button.new()
	_queue_execute_btn.text = "执行"
	_queue_execute_btn.add_theme_font_size_override("font_size", 11)
	_queue_execute_btn.pressed.connect(_on_execute_queue_pressed)
	queue_btn_row.add_child(_queue_execute_btn)

	_style_panel(_login_panel, Color(0.06, 0.09, 0.14, 0.96), Color(0.31, 0.44, 0.61, 0.95))
	_style_panel(hud_panel, Color(0.07, 0.10, 0.15, 0.96), Color(0.34, 0.47, 0.66, 0.95))
	_style_panel(fx_panel, Color(0.08, 0.10, 0.16, 0.96), Color(0.58, 0.49, 0.25, 0.90))
	_style_panel(chart_panel, Color(0.03, 0.08, 0.12, 0.98), Color(0.28, 0.42, 0.60, 0.95))
	_style_panel(queue_panel, Color(0.08, 0.11, 0.16, 0.95), Color(0.56, 0.47, 0.24, 0.90))
	_style_panel(hand_panel, Color(0.07, 0.11, 0.18, 0.96), Color(0.36, 0.52, 0.72, 0.90))
	_style_panel(_hand_drop_zone, Color(0.07, 0.12, 0.19, 0.96), Color(0.36, 0.52, 0.72, 0.90))
	_style_panel(_queue_drop_zone, Color(0.08, 0.11, 0.17, 0.95), Color(0.58, 0.48, 0.25, 0.88))
	_style_panel(_cand_panel, Color(0.08, 0.10, 0.16, 0.95), Color(0.34, 0.47, 0.66, 0.90))
	_style_panel(upgrade_panel, Color(0.08, 0.10, 0.16, 0.95), Color(0.34, 0.47, 0.66, 0.90))
	_style_panel(log_panel, Color(0.08, 0.10, 0.16, 0.95), Color(0.34, 0.47, 0.66, 0.90))
	_style_panel(dev_panel, Color(0.08, 0.10, 0.16, 0.95), Color(0.34, 0.47, 0.66, 0.90))
	_style_panel(_settle_panel, Color(0.07, 0.10, 0.15, 0.95), Color(0.58, 0.49, 0.25, 0.92))
	_style_chart_frame()
	hud_panel.modulate = Color(1, 1, 1, 0.92)
	fx_panel.modulate = Color(1, 1, 1, 0.90)

	bottom_tabs.add_theme_color_override("font_selected_color", Color(0.94, 0.98, 1.0, 1.0))
	bottom_tabs.add_theme_color_override("font_unselected_color", Color(0.62, 0.74, 0.89, 1.0))
	for one in [
		_connect_btn, _meta_btn, _create_run_btn, _resume_btn, _refresh_btn, _prepare_stage_btn, _finish_run_btn,
		_add_queue_btn, _pass_btn, _queue_execute_btn, _logout_btn
	]:
		_style_action_button(one)
	_style_primary_execute_button(_queue_execute_btn)
	_style_pile_icon_button(_draw_pile_btn)
	_style_pile_icon_button(_discard_pile_btn)

	_floating_fx_layer = Control.new()
	_floating_fx_layer.set_anchors_preset(Control.PRESET_FULL_RECT)
	_floating_fx_layer.mouse_filter = Control.MOUSE_FILTER_IGNORE
	_floating_fx_layer.z_index = 200
	add_child(_floating_fx_layer)
	_build_pile_view_dialog()
	_build_settings_overlay()
	_build_story_overlay()
	_refresh_pile_controls()
	_refresh_queue_drop_hint()
	if not standalone_login_mode:
		_login_panel.visible = false
	call_deferred("_apply_responsive_layout")


func _on_viewport_size_changed() -> void:
	_apply_responsive_layout()


func _safe_split_offset(total: int, ratio: float, min_left: int, min_right: int) -> int:
	if total <= 0:
		return 0
	var max_left: int = total - max(0, min_right)
	if max_left <= 0:
		return 0
	var want: int = int(float(total) * ratio)
	if min_left > max_left:
		return max_left
	return clampi(want, max(0, min_left), max_left)


func _apply_responsive_layout(force: bool = false) -> void:
	var viewport_size := get_viewport_rect().size
	if not force and viewport_size.is_equal_approx(_last_layout_viewport_size):
		return
	_last_layout_viewport_size = viewport_size
	var viewport_w := int(viewport_size.x)
	var viewport_h := int(viewport_size.y)
	var shared_right_col := 320
	if viewport_w <= 1366:
		shared_right_col = 280
	elif viewport_w >= 1920:
		shared_right_col = 360
	if _main_vsplit != null:
		var split_h := int(_main_vsplit.size.y)
		if split_h > 0:
			var top_ratio := 0.48
			if viewport_h <= 860:
				top_ratio = 0.44
			elif viewport_h >= 1180:
				top_ratio = 0.50
			_main_vsplit.split_offset = _safe_split_offset(split_h, top_ratio, 104, 340)
	if _battle_split != null:
		var battle_w := int(_battle_split.size.x)
		if battle_w > 0:
			var right_col_min := 250
			var battle_left_min := 360
			var right_col_cap: int = battle_w - battle_left_min
			if right_col_cap < right_col_min:
				right_col_cap = right_col_min
			var right_col_target := clampi(shared_right_col, right_col_min, right_col_cap)
			var battle_ratio: float = 1.0 - (float(right_col_target) / maxf(1.0, float(battle_w)))
			_battle_split.split_offset = _safe_split_offset(battle_w, battle_ratio, battle_left_min, right_col_min)
	if _bottom_split != null:
		var bottom_w := int(_bottom_split.size.x)
		if bottom_w > 0:
			var queue_col_min := 250
			var hand_col_min := 560
			var queue_col_cap: int = bottom_w - hand_col_min
			if queue_col_cap < queue_col_min:
				queue_col_cap = queue_col_min
			var queue_col_target := clampi(shared_right_col, queue_col_min, queue_col_cap)
			var bottom_ratio: float = 1.0 - (float(queue_col_target) / maxf(1.0, float(bottom_w)))
			_bottom_split.split_offset = _safe_split_offset(bottom_w, bottom_ratio, hand_col_min, queue_col_min)
	_sync_queue_drop_zone_height()
	_layout_hand_fan_cards(false)


func _sync_queue_drop_zone_height() -> void:
	if _queue_drop_zone == null or _bottom_split == null:
		return
	var bottom_h := int(_bottom_split.size.y)
	if bottom_h <= 0:
		return
	var target_min_h := clampi(bottom_h - 88, 126, 260)
	var current_h := int(_queue_drop_zone.custom_minimum_size.y)
	if abs(target_min_h - current_h) < 2:
		return
	_queue_drop_zone.custom_minimum_size = Vector2(0, target_min_h)


func _line(_placeholder: String, value: String) -> LineEdit:
	var le := LineEdit.new()
	le.placeholder_text = _placeholder
	le.text = value
	return le


func _row(label_text: String, field: Control) -> HBoxContainer:
	var row := HBoxContainer.new()
	var lb := Label.new()
	lb.text = "%s:" % label_text
	lb.custom_minimum_size = Vector2(88, 0)
	row.add_child(lb)
	field.size_flags_horizontal = Control.SIZE_EXPAND_FILL
	row.add_child(field)
	return row


func _make_fx_label(text: String) -> Label:
	var lb := Label.new()
	lb.text = text
	lb.custom_minimum_size = Vector2(128, 0)
	lb.add_theme_font_size_override("font_size", 13)
	lb.add_theme_color_override("font_color", Color(0.84, 0.90, 0.98, 1.0))
	return lb


func _build_pile_button(title: String, icon_text: String, open_handler: Callable) -> Dictionary:
	var btn := Button.new()
	btn.text = icon_text
	btn.custom_minimum_size = Vector2(30, 24)
	btn.focus_mode = Control.FOCUS_NONE
	btn.alignment = HORIZONTAL_ALIGNMENT_CENTER
	btn.mouse_default_cursor_shape = Control.CURSOR_POINTING_HAND
	btn.tooltip_text = "%s：点击查看卡牌集合与数量。" % title
	if open_handler.is_valid():
		btn.pressed.connect(open_handler)

	var badge := Label.new()
	badge.mouse_filter = Control.MOUSE_FILTER_IGNORE
	badge.anchor_left = 1.0
	badge.anchor_top = 0.0
	badge.anchor_right = 1.0
	badge.anchor_bottom = 0.0
	badge.offset_left = -16.0
	badge.offset_top = -6.0
	badge.offset_right = -1.0
	badge.offset_bottom = 8.0
	badge.horizontal_alignment = HORIZONTAL_ALIGNMENT_CENTER
	badge.vertical_alignment = VERTICAL_ALIGNMENT_CENTER
	badge.add_theme_font_size_override("font_size", 8)
	badge.add_theme_color_override("font_color", Color(0.98, 0.98, 1.0, 1.0))
	var badge_bg := StyleBoxFlat.new()
	badge_bg.bg_color = Color(0.74, 0.22, 0.30, 0.95)
	badge_bg.border_color = Color(0.98, 0.82, 0.62, 0.92)
	badge_bg.set_border_width_all(1)
	badge_bg.set_corner_radius_all(5)
	badge.add_theme_stylebox_override("normal", badge_bg)
	btn.add_child(badge)
	return {"button": btn, "badge": badge}


func _style_panel(panel: PanelContainer, bg: Color, border: Color) -> void:
	if panel == null:
		return
	var sb := StyleBoxFlat.new()
	sb.set_corner_radius_all(12)
	sb.set_border_width_all(1)
	sb.bg_color = bg
	sb.border_color = border
	sb.shadow_color = Color(0.0, 0.0, 0.0, 0.38)
	sb.shadow_size = 5
	sb.shadow_offset = Vector2(0, 2)
	sb.content_margin_left = 10.0
	sb.content_margin_right = 10.0
	sb.content_margin_top = 8.0
	sb.content_margin_bottom = 8.0
	panel.add_theme_stylebox_override("panel", sb)


func _style_chart_frame() -> void:
	if _chart_view == null:
		return
	var chart_bg := StyleBoxFlat.new()
	chart_bg.bg_color = Color(0.02, 0.08, 0.12, 0.98)
	chart_bg.border_color = Color(0.26, 0.46, 0.70, 0.82)
	chart_bg.set_border_width_all(1)
	chart_bg.set_corner_radius_all(8)
	chart_bg.shadow_color = Color(0.0, 0.0, 0.0, 0.45)
	chart_bg.shadow_size = 6
	chart_bg.shadow_offset = Vector2(0, 2)
	_chart_view.add_theme_stylebox_override("panel", chart_bg)


func _style_action_button(btn: Button) -> void:
	if btn == null:
		return
	btn.custom_minimum_size = Vector2(0, 30)
	btn.add_theme_color_override("font_color", Color(0.96, 0.93, 0.82, 1.0))
	btn.add_theme_font_size_override("font_size", 13)
	var normal := StyleBoxFlat.new()
	normal.set_corner_radius_all(7)
	normal.bg_color = Color(0.14, 0.20, 0.30, 1.0)
	normal.border_color = Color(0.62, 0.52, 0.30, 0.92)
	normal.set_border_width_all(1)
	normal.shadow_color = Color(0.0, 0.0, 0.0, 0.32)
	normal.shadow_size = 4
	normal.shadow_offset = Vector2(0, 1)
	var hover := StyleBoxFlat.new()
	hover.set_corner_radius_all(7)
	hover.bg_color = Color(0.18, 0.26, 0.38, 1.0)
	hover.border_color = Color(0.82, 0.69, 0.36, 1.0)
	hover.set_border_width_all(1)
	hover.shadow_color = Color(0.0, 0.0, 0.0, 0.36)
	hover.shadow_size = 5
	hover.shadow_offset = Vector2(0, 1)
	var pressed := StyleBoxFlat.new()
	pressed.set_corner_radius_all(7)
	pressed.bg_color = Color(0.11, 0.16, 0.24, 1.0)
	pressed.border_color = Color(0.88, 0.73, 0.36, 1.0)
	pressed.set_border_width_all(1)
	pressed.shadow_color = Color(0.0, 0.0, 0.0, 0.26)
	pressed.shadow_size = 3
	pressed.shadow_offset = Vector2(0, 1)
	btn.add_theme_stylebox_override("normal", normal)
	btn.add_theme_stylebox_override("hover", hover)
	btn.add_theme_stylebox_override("pressed", pressed)


func _style_pile_icon_button(btn: Button) -> void:
	if btn == null:
		return
	btn.custom_minimum_size = Vector2(30, 24)
	btn.add_theme_font_size_override("font_size", 11)
	btn.add_theme_color_override("font_color", Color(0.96, 0.95, 0.84, 1.0))
	var normal := StyleBoxFlat.new()
	normal.set_corner_radius_all(5)
	normal.bg_color = Color(0.10, 0.16, 0.25, 0.92)
	normal.border_color = Color(0.68, 0.60, 0.36, 0.92)
	normal.set_border_width_all(1)
	var hover := StyleBoxFlat.new()
	hover.set_corner_radius_all(5)
	hover.bg_color = Color(0.16, 0.24, 0.36, 0.96)
	hover.border_color = Color(0.88, 0.76, 0.44, 1.0)
	hover.set_border_width_all(1)
	var pressed := StyleBoxFlat.new()
	pressed.set_corner_radius_all(5)
	pressed.bg_color = Color(0.08, 0.12, 0.20, 0.98)
	pressed.border_color = Color(0.92, 0.80, 0.46, 1.0)
	pressed.set_border_width_all(1)
	btn.add_theme_stylebox_override("normal", normal)
	btn.add_theme_stylebox_override("hover", hover)
	btn.add_theme_stylebox_override("pressed", pressed)


func _style_primary_execute_button(btn: Button) -> void:
	if btn == null:
		return
	btn.custom_minimum_size = Vector2(150, 42)
	btn.add_theme_font_size_override("font_size", 17)
	btn.add_theme_color_override("font_color", Color(0.10, 0.12, 0.16, 1.0))
	var normal := StyleBoxFlat.new()
	normal.set_corner_radius_all(9)
	normal.bg_color = Color(0.95, 0.78, 0.34, 1.0)
	normal.border_color = Color(0.98, 0.90, 0.60, 0.98)
	normal.set_border_width_all(1)
	normal.shadow_color = Color(0.0, 0.0, 0.0, 0.36)
	normal.shadow_size = 6
	normal.shadow_offset = Vector2(0, 2)
	var hover := StyleBoxFlat.new()
	hover.set_corner_radius_all(9)
	hover.bg_color = Color(0.98, 0.84, 0.45, 1.0)
	hover.border_color = Color(1.0, 0.94, 0.70, 1.0)
	hover.set_border_width_all(1)
	hover.shadow_color = Color(0.0, 0.0, 0.0, 0.40)
	hover.shadow_size = 7
	hover.shadow_offset = Vector2(0, 2)
	var pressed := StyleBoxFlat.new()
	pressed.set_corner_radius_all(9)
	pressed.bg_color = Color(0.86, 0.66, 0.24, 1.0)
	pressed.border_color = Color(1.0, 0.90, 0.58, 1.0)
	pressed.set_border_width_all(1)
	pressed.shadow_color = Color(0.0, 0.0, 0.0, 0.30)
	pressed.shadow_size = 4
	pressed.shadow_offset = Vector2(0, 1)
	btn.add_theme_stylebox_override("normal", normal)
	btn.add_theme_stylebox_override("hover", hover)
	btn.add_theme_stylebox_override("pressed", pressed)


func _style_warning_discard_button(btn: Button) -> void:
	if btn == null:
		return
	btn.custom_minimum_size = Vector2(72, 42)
	btn.add_theme_font_size_override("font_size", 16)
	btn.add_theme_color_override("font_color", Color(0.15, 0.08, 0.06, 1.0))
	var normal := StyleBoxFlat.new()
	normal.set_corner_radius_all(9)
	normal.bg_color = Color(0.96, 0.52, 0.22, 1.0)
	normal.border_color = Color(1.0, 0.78, 0.52, 1.0)
	normal.set_border_width_all(1)
	normal.shadow_color = Color(0.0, 0.0, 0.0, 0.35)
	normal.shadow_size = 6
	normal.shadow_offset = Vector2(0, 2)
	var hover := StyleBoxFlat.new()
	hover.set_corner_radius_all(9)
	hover.bg_color = Color(0.99, 0.60, 0.30, 1.0)
	hover.border_color = Color(1.0, 0.84, 0.60, 1.0)
	hover.set_border_width_all(1)
	hover.shadow_color = Color(0.0, 0.0, 0.0, 0.38)
	hover.shadow_size = 7
	hover.shadow_offset = Vector2(0, 2)
	var pressed := StyleBoxFlat.new()
	pressed.set_corner_radius_all(9)
	pressed.bg_color = Color(0.84, 0.42, 0.16, 1.0)
	pressed.border_color = Color(1.0, 0.76, 0.48, 1.0)
	pressed.set_border_width_all(1)
	pressed.shadow_color = Color(0.0, 0.0, 0.0, 0.28)
	pressed.shadow_size = 4
	pressed.shadow_offset = Vector2(0, 1)
	btn.add_theme_stylebox_override("normal", normal)
	btn.add_theme_stylebox_override("hover", hover)
	btn.add_theme_stylebox_override("pressed", pressed)


func _build_pile_view_dialog() -> void:
	_pile_view_dialog = AcceptDialog.new()
	_pile_view_dialog.visible = false
	_pile_view_dialog.title = "牌堆查看"
	_pile_view_dialog.min_size = Vector2i(450, 520)
	add_child(_pile_view_dialog)
	_pile_view_dialog.get_ok_button().text = "关闭"

	var box := VBoxContainer.new()
	box.custom_minimum_size = Vector2(420, 460)
	box.size_flags_horizontal = Control.SIZE_EXPAND_FILL
	box.size_flags_vertical = Control.SIZE_EXPAND_FILL
	box.add_theme_constant_override("separation", 8)
	_pile_view_dialog.add_child(box)

	_pile_view_title = Label.new()
	_pile_view_title.text = "卡牌集合"
	_pile_view_title.add_theme_font_size_override("font_size", 18)
	_pile_view_title.add_theme_color_override("font_color", Color(0.90, 0.95, 1.0, 1.0))
	box.add_child(_pile_view_title)

	_pile_view_body = RichTextLabel.new()
	_pile_view_body.fit_content = false
	_pile_view_body.scroll_active = true
	_pile_view_body.size_flags_vertical = Control.SIZE_EXPAND_FILL
	_pile_view_body.custom_minimum_size = Vector2(0, 400)
	_pile_view_body.add_theme_color_override("default_color", Color(0.85, 0.92, 1.0, 1.0))
	box.add_child(_pile_view_body)


func _on_open_draw_pile_pressed() -> void:
	_open_pile_view("deck")


func _on_open_discard_pile_pressed() -> void:
	_open_pile_view("discard")


func _open_pile_view(mode: String) -> void:
	if _pile_view_dialog == null:
		return
	_render_pile_view_dialog(mode)
	_pile_view_dialog.popup_centered_ratio(0.50)


func _render_pile_view_dialog(mode: String) -> void:
	if _pile_view_title == null or _pile_view_body == null:
		return
	var cards: Array[String] = _deck_cards()
	var title := "抽牌堆"
	if mode == "discard":
		cards = _discard_cards()
		title = "弃牌堆"
	_pile_view_title.text = "%s · 集合视图（不显示顺序）" % title
	_pile_view_body.clear()
	if cards.is_empty():
		_pile_view_body.append_text("当前为空。")
		return
	var counts := {}
	for cid in cards:
		counts[cid] = int(counts.get(cid, 0)) + 1
	var entries: Array[Dictionary] = []
	for key in counts.keys():
		var cid := str(key)
		entries.append({
			"card_id": cid,
			"name": _get_card_name(cid),
			"count": int(counts[key]),
		})
	entries.sort_custom(func(a: Dictionary, b: Dictionary) -> bool:
		var name_a := str(a.get("name", ""))
		var name_b := str(b.get("name", ""))
		if name_a == name_b:
			return str(a.get("card_id", "")) < str(b.get("card_id", ""))
		return name_a < name_b
	)
	_pile_view_body.append_text("总数：%s    卡种：%s\n\n" % [str(cards.size()), str(entries.size())])
	for one in entries:
		_pile_view_body.append_text("%s × %s\n" % [str(one.get("name", "")), str(one.get("count", 0))])


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
	_settings_modal.custom_minimum_size = Vector2(360, 0)
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

	_settings_home_btn = Button.new()
	_settings_home_btn.text = "回到主选单"
	_settings_home_btn.pressed.connect(_on_settings_home_pressed)
	box.add_child(_settings_home_btn)

	_settings_exit_battle_btn = Button.new()
	_settings_exit_battle_btn.text = "退出战斗（测试）"
	_settings_exit_battle_btn.pressed.connect(_on_settings_exit_battle_pressed)
	box.add_child(_settings_exit_battle_btn)

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

	_style_action_button(_settings_home_btn)
	_style_action_button(_settings_exit_battle_btn)
	_style_action_button(_settings_quit_btn)


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
		if _state.run_id <= 0:
			_settings_run_summary_label.text = "当前局：未开始（可在登录页选择继续或新开）"
		else:
			_settings_run_summary_label.text = "当前局：Run#%s | 关卡=%s | 回合=T%s | 关卡分=%s/%s | 信心=%s" % [
				str(_state.run_id),
				str(_state.stage_status()),
				str(_state.stage.get("current_turn", "-")),
				str(_state.stage.get("stage_score", "-")),
				str(_state.stage.get("target_score", "-")),
				str(_state.run.get("confidence", "-")),
			]
	if _settings_version_label != null:
		_settings_version_label.text = "客户端版本：build=%s" % str(_api.client_build())
	if _settings_exit_battle_btn != null:
		_settings_exit_battle_btn.visible = _map_mode
		_settings_exit_battle_btn.disabled = (not _map_mode) or _map_exit_emitted


func _build_story_overlay() -> void:
	_story_overlay = ColorRect.new()
	_story_overlay.set_anchors_preset(Control.PRESET_FULL_RECT)
	_story_overlay.color = Color(0.0, 0.0, 0.0, 0.66)
	_story_overlay.mouse_filter = Control.MOUSE_FILTER_STOP
	_story_overlay.visible = false
	_story_overlay.z_index = 520
	add_child(_story_overlay)

	var bg := TextureRect.new()
	bg.set_anchors_preset(Control.PRESET_FULL_RECT)
	bg.mouse_filter = Control.MOUSE_FILTER_IGNORE
	bg.stretch_mode = TextureRect.STRETCH_KEEP_ASPECT_COVERED
	bg.texture = _make_story_background_texture()
	_story_overlay.add_child(bg)

	var veil := ColorRect.new()
	veil.set_anchors_preset(Control.PRESET_FULL_RECT)
	veil.mouse_filter = Control.MOUSE_FILTER_IGNORE
	veil.color = Color(0.03, 0.05, 0.09, 0.32)
	_story_overlay.add_child(veil)

	_story_panel = PanelContainer.new()
	_story_panel.anchor_left = 0.07
	_story_panel.anchor_top = 0.63
	_story_panel.anchor_right = 0.93
	_story_panel.anchor_bottom = 0.95
	_story_overlay.add_child(_story_panel)
	_style_panel(_story_panel, Color(0.05, 0.08, 0.14, 0.94), Color(0.73, 0.66, 0.41, 0.98))

	var box := VBoxContainer.new()
	box.set_anchors_preset(Control.PRESET_FULL_RECT)
	box.add_theme_constant_override("separation", 6)
	_story_panel.add_child(box)

	_story_name_label = Label.new()
	_story_name_label.text = "旁白"
	_story_name_label.add_theme_font_size_override("font_size", 19)
	_story_name_label.add_theme_color_override("font_color", Color(0.96, 0.90, 0.70, 1.0))
	box.add_child(_story_name_label)

	_story_text_label = RichTextLabel.new()
	_story_text_label.bbcode_enabled = false
	_story_text_label.fit_content = false
	_story_text_label.scroll_active = false
	_story_text_label.custom_minimum_size = Vector2(0, 86)
	_story_text_label.size_flags_vertical = Control.SIZE_EXPAND_FILL
	_story_text_label.add_theme_font_size_override("normal_font_size", 20)
	_story_text_label.add_theme_color_override("default_color", Color(0.90, 0.96, 1.0, 1.0))
	box.add_child(_story_text_label)

	var action_row := HBoxContainer.new()
	action_row.add_theme_constant_override("separation", 10)
	box.add_child(action_row)
	var hint := Label.new()
	hint.text = "空格 / Enter：继续"
	hint.add_theme_color_override("font_color", Color(0.65, 0.78, 0.92, 1.0))
	action_row.add_child(hint)
	var spacer := Control.new()
	spacer.size_flags_horizontal = Control.SIZE_EXPAND_FILL
	action_row.add_child(spacer)
	_story_skip_btn = Button.new()
	_story_skip_btn.text = "跳过剧情"
	_story_skip_btn.pressed.connect(_on_story_skip_pressed)
	action_row.add_child(_story_skip_btn)
	_story_next_btn = Button.new()
	_story_next_btn.text = "继续 >"
	_story_next_btn.pressed.connect(_on_story_next_pressed)
	action_row.add_child(_story_next_btn)
	_style_action_button(_story_skip_btn)
	_style_action_button(_story_next_btn)


func _make_story_background_texture() -> Texture2D:
	var grad := Gradient.new()
	grad.offsets = PackedFloat32Array([0.0, 0.56, 1.0])
	grad.colors = PackedColorArray([
		Color(0.11, 0.13, 0.28, 1.0),
		Color(0.22, 0.12, 0.34, 1.0),
		Color(0.06, 0.08, 0.16, 1.0),
	])
	var tex := GradientTexture2D.new()
	tex.gradient = grad
	tex.fill = GradientTexture2D.FILL_LINEAR
	tex.fill_from = Vector2(0.5, 0.0)
	tex.fill_to = Vector2(0.5, 1.0)
	tex.width = 1920
	tex.height = 1080
	return tex


func _default_victory_dialog() -> Array[Dictionary]:
	return [
		{"name": "旁白", "text": "夜色落在图表尽头，你在最后一笔波动里完成了通关。"},
		{"name": "神秘交易员", "text": "看见了吗？价格从来不是敌人，它只是对意志的回声。"},
		{"name": "你", "text": "这只是序章。下一段旅程，我会在更深的浪潮里找到答案。"},
	]


func _open_victory_story() -> void:
	if _story_overlay == null:
		return
	_story_lines = _default_victory_dialog()
	_story_line_idx = 0
	_story_transitioning = false
	_story_active = true
	_story_overlay.visible = true
	_apply_turn_action_locks()
	_render_story_line()


func _render_story_line() -> void:
	if _story_name_label == null or _story_text_label == null:
		return
	if _story_line_idx < 0 or _story_line_idx >= _story_lines.size():
		_close_story_overlay()
		return
	var one := _story_lines[_story_line_idx]
	_story_name_label.text = str(one.get("name", "旁白"))
	_story_text_label.clear()
	_story_text_label.append_text(str(one.get("text", "")))
	if _story_next_btn != null:
		if _story_line_idx >= _story_lines.size() - 1:
			_story_next_btn.text = "进入下一章"
		else:
			_story_next_btn.text = "继续 >"


func _advance_story_line() -> void:
	if not _story_active:
		return
	_story_line_idx += 1
	if _story_line_idx >= _story_lines.size():
		_close_story_overlay()
		return
	_render_story_line()


func _close_story_overlay() -> void:
	if _story_overlay != null:
		_story_overlay.visible = false
	_story_active = false
	_story_lines = []
	_story_line_idx = 0
	_apply_turn_action_locks()


func _maybe_trigger_victory_story() -> void:
	if _map_mode:
		return
	if _story_active:
		return
	if _state.run_id <= 0:
		return
	var run_status := _state.run_status()
	var stage_status := _state.stage_status()
	var stage_cleared := stage_status == "cleared" or stage_status == "stage_cleared"
	var run_cleared := run_status == "stage_cleared" or run_status == "cleared"
	var stage_score := int(_state.stage.get("stage_score", 0))
	var target_score := int(_state.stage.get("target_score", 0))
	var score_reached := target_score > 0 and stage_score >= target_score
	if not stage_cleared and not run_cleared and not score_reached:
		return
	var stage_no := int(_state.run.get("current_stage", _state.current_stage_no()))
	var marker := "%s:%s" % [str(_state.run_id), str(stage_no)]
	if _story_shown_stage_marker == marker:
		return
	_story_shown_stage_marker = marker
	_open_victory_story()


func _on_story_next_pressed() -> void:
	if not _story_active:
		return
	if _story_line_idx >= _story_lines.size() - 1:
		_start_next_chapter_from_story()
		return
	_advance_story_line()


func _on_story_skip_pressed() -> void:
	_close_story_overlay()


func _start_next_chapter_from_story() -> void:
	if _story_transitioning:
		return
	_story_transitioning = true
	_close_story_overlay()
	if not _actions_enabled:
		_story_transitioning = false
		_set_status("剧情结束。当前未连接，未自动开启下一章。")
		_append_log("剧情结束：未连接状态，未自动开局。")
		return
	_set_status("剧情结束，正在进入下一章...")
	_append_log("剧情结束：自动开启下一章。")
	call_deferred("_story_create_run_after_dialog")


func _story_create_run_after_dialog() -> void:
	await _on_create_run_pressed()
	_story_transitioning = false


func _init_sfx() -> void:
	_register_sfx("card_pick", "res://assets/sfx/card_pick.wav", -8.0)
	_register_sfx("card_return", "res://assets/sfx/card_return.wav", -9.0)
	_register_sfx("queue_reorder", "res://assets/sfx/queue_reorder.wav", -10.0)
	_register_sfx("queue_execute", "res://assets/sfx/queue_execute.wav", -7.0)
	_register_sfx("turn_pass", "res://assets/sfx/turn_pass.wav", -10.0)
	_register_sfx("score_up", "res://assets/sfx/score_up.wav", -7.0)
	_register_sfx("score_down", "res://assets/sfx/score_down.wav", -7.0)
	_fallback_sfx_stream = AudioStreamGenerator.new()
	_fallback_sfx_stream.mix_rate = 44100.0
	_fallback_sfx_stream.buffer_length = 0.25
	_fallback_sfx_player = AudioStreamPlayer.new()
	_fallback_sfx_player.bus = "Master"
	_fallback_sfx_player.volume_db = -14.0
	_fallback_sfx_player.stream = _fallback_sfx_stream
	add_child(_fallback_sfx_player)


func _register_sfx(key: String, resource_path: String, volume_db: float) -> void:
	var player := AudioStreamPlayer.new()
	player.bus = "Master"
	player.volume_db = volume_db
	if ResourceLoader.exists(resource_path):
		var stream := load(resource_path)
		if stream is AudioStream:
			player.stream = stream
	add_child(player)
	_sfx_players[key] = player


func _play_ui_sfx(key: String, pitch_scale: float = 1.0) -> void:
	var player: Variant = _sfx_players.get(key, null)
	if player == null:
		_play_fallback_sfx(key, pitch_scale)
		return
	if not (player is AudioStreamPlayer):
		_play_fallback_sfx(key, pitch_scale)
		return
	var audio: AudioStreamPlayer = player
	if audio.stream == null:
		_play_fallback_sfx(key, pitch_scale)
		return
	audio.pitch_scale = pitch_scale
	audio.play()


func _play_fallback_sfx(key: String, pitch_scale: float = 1.0) -> void:
	if _fallback_sfx_player == null or _fallback_sfx_stream == null:
		return
	if not _fallback_sfx_player.playing:
		_fallback_sfx_player.play()
	var playback: Variant = _fallback_sfx_player.get_stream_playback()
	if not (playback is AudioStreamGeneratorPlayback):
		return
	var gen: AudioStreamGeneratorPlayback = playback
	var base_freq := 880.0
	var amp := 0.18
	var duration := 0.08
	match key:
		"card_pick":
			base_freq = 900.0
			amp = 0.16
		"card_return":
			base_freq = 740.0
			amp = 0.14
		"queue_reorder":
			base_freq = 780.0
			amp = 0.14
		"queue_execute":
			base_freq = 620.0
			amp = 0.20
			duration = 0.10
		"turn_pass":
			base_freq = 520.0
			amp = 0.14
		"score_up":
			base_freq = 1020.0
			amp = 0.20
			duration = 0.11
		"score_down":
			base_freq = 430.0
			amp = 0.18
			duration = 0.11
		_:
			base_freq = 820.0
	var freq: float = clampf(base_freq * pitch_scale, 220.0, 1800.0)
	var sample_count: int = int(_fallback_sfx_stream.mix_rate * duration)
	for i in range(sample_count):
		var t: float = float(i) / _fallback_sfx_stream.mix_rate
		var env: float = pow(1.0 - (float(i) / max(1.0, float(sample_count))), 1.8)
		var s: float = sin(TAU * freq * t) * env * amp
		gen.push_frame(Vector2(s, s))


func _build_settlement_panel(parent: VBoxContainer) -> void:
	_settle_panel = PanelContainer.new()
	_settle_panel.custom_minimum_size = Vector2(0, 34)
	parent.add_child(_settle_panel)

	var box := VBoxContainer.new()
	box.add_theme_constant_override("separation", 2)
	_settle_panel.add_child(box)

	var summary_row := HBoxContainer.new()
	summary_row.add_theme_constant_override("separation", 10)
	box.add_child(summary_row)
	_settle_delta_label = Label.new()
	_settle_delta_label.text = "等待本回合结算..."
	_settle_delta_label.add_theme_font_size_override("font_size", 15)
	_settle_delta_label.add_theme_color_override("font_color", Color(0.84, 0.90, 0.98, 1.0))
	_settle_delta_label.size_flags_horizontal = Control.SIZE_EXPAND_FILL
	summary_row.add_child(_settle_delta_label)
	_settle_detail_label = Label.new()
	_settle_detail_label.text = "信心 - | 关卡分 - | 总分 -"
	_settle_detail_label.add_theme_font_size_override("font_size", 11)
	_settle_detail_label.add_theme_color_override("font_color", Color(0.72, 0.82, 0.95, 1.0))
	_settle_detail_label.clip_text = true
	_settle_detail_label.size_flags_horizontal = Control.SIZE_EXPAND_FILL
	summary_row.add_child(_settle_detail_label)

	var bars_row := HBoxContainer.new()
	bars_row.add_theme_constant_override("separation", 6)
	box.add_child(bars_row)
	var stage_name := Label.new()
	stage_name.text = "关卡"
	stage_name.custom_minimum_size = Vector2(36, 0)
	bars_row.add_child(stage_name)
	_settle_stage_bar = ProgressBar.new()
	_settle_stage_bar.show_percentage = false
	_settle_stage_bar.size_flags_horizontal = Control.SIZE_EXPAND_FILL
	_settle_stage_bar.custom_minimum_size = Vector2(0, 7)
	_settle_stage_bar.min_value = 0.0
	_settle_stage_bar.max_value = 1.0
	_settle_stage_bar.value = 0.0
	bars_row.add_child(_settle_stage_bar)
	_settle_stage_value = Label.new()
	_settle_stage_value.text = "0/0"
	_settle_stage_value.custom_minimum_size = Vector2(54, 0)
	bars_row.add_child(_settle_stage_value)
	var conf_name := Label.new()
	conf_name.text = "信心"
	conf_name.custom_minimum_size = Vector2(36, 0)
	bars_row.add_child(conf_name)
	_settle_conf_bar = ProgressBar.new()
	_settle_conf_bar.show_percentage = false
	_settle_conf_bar.size_flags_horizontal = Control.SIZE_EXPAND_FILL
	_settle_conf_bar.custom_minimum_size = Vector2(0, 7)
	_settle_conf_bar.min_value = 0.0
	_settle_conf_bar.max_value = 100.0
	_settle_conf_bar.value = 80.0
	bars_row.add_child(_settle_conf_bar)
	_settle_conf_value = Label.new()
	_settle_conf_value.text = "80"
	_settle_conf_value.custom_minimum_size = Vector2(48, 0)
	bars_row.add_child(_settle_conf_value)

	_settle_panel.modulate = Color(1, 1, 1, 0.36)


func _set_status(text: String) -> void:
	if _status_label != null:
		_status_label.text = "状态：%s" % text


func _append_log(text: String) -> void:
	_log_box.append_text("[%s] %s\n" % [Time.get_datetime_string_from_system(), text])
	_log_box.scroll_to_line(_log_box.get_line_count())


func _api_error_text(payload: Dictionary) -> String:
	var msg := str(payload.get("message", "unknown"))
	var code := int(payload.get("status_code", 0))
	if code <= 0:
		code = int(payload.get("result_code", 0))
	if code > 0:
		return "%s (code=%s)" % [msg, str(code)]
	return msg


func _update_action_enabled(enabled: bool) -> void:
	_actions_enabled = enabled
	if _meta_btn != null:
		_meta_btn.disabled = not enabled
	if _create_run_btn != null:
		_create_run_btn.disabled = not enabled
	if _resume_btn != null:
		_resume_btn.disabled = not enabled
	if _refresh_btn != null:
		_refresh_btn.disabled = not enabled
	if _prepare_stage_btn != null:
		_prepare_stage_btn.disabled = not enabled
	if _finish_run_btn != null:
		_finish_run_btn.disabled = not enabled
	_apply_turn_action_locks()


func _is_turn_playable_stage() -> bool:
	return _state.stage_status() == "playing"


func _pending_discard_count() -> int:
	var run_effects: Dictionary = _state.run.get("run_effects", {})
	return int(run_effects.get("pending_discard", 0))


func _is_discard_mode() -> bool:
	return _pending_discard_count() > 0 and _is_turn_playable_stage()


func _apply_turn_action_locks() -> void:
	var has_hand_selection := _selected_hand_idx >= 0 and _selected_hand_idx < _hand_view_cards.size()
	var playable_stage := _is_turn_playable_stage()
	var story_locked := _story_active
	var discard_mode := _is_discard_mode()
	var request_locked := _turn_action_inflight
	if _add_queue_btn != null:
		_add_queue_btn.disabled = (not _actions_enabled) or (not has_hand_selection) or (not playable_stage) or story_locked or discard_mode or request_locked
		if discard_mode:
			_add_queue_btn.tooltip_text = "当前需要先弃牌，不能加入等待区。"
		elif request_locked:
			_add_queue_btn.tooltip_text = "回合结算中，请稍候。"
		else:
			_add_queue_btn.tooltip_text = "加入等待区。"
	if _queue_execute_btn != null:
		_queue_execute_btn.text = "执行中..." if request_locked else "执行"
		var can_execute := _actions_enabled and playable_stage and (not _queue_cards.is_empty()) and (not story_locked) and (not discard_mode) and (not request_locked)
		_queue_execute_btn.disabled = not can_execute
		if story_locked:
			_queue_execute_btn.tooltip_text = "剧情进行中：请先完成对话。"
		elif request_locked:
			_queue_execute_btn.tooltip_text = "回合结算中，请稍候。"
		elif discard_mode:
			_queue_execute_btn.tooltip_text = "当前需要弃牌，执行按钮已锁定。"
		elif not _actions_enabled:
			_queue_execute_btn.tooltip_text = "当前不可操作：请先连接并进入进行中的关卡。"
		elif not playable_stage:
			_queue_execute_btn.tooltip_text = "当前关卡不可出牌（状态：%s）。" % _state.stage_status()
		elif _queue_cards.is_empty():
			_queue_execute_btn.tooltip_text = "等待区为空：先拖拽至少 1 张手牌。"
		else:
			_queue_execute_btn.tooltip_text = "按等待区顺序执行卡牌并结束回合。"
	if _pass_btn != null:
		_pass_btn.text = "弃牌" if discard_mode else "PASS"
		if discard_mode:
			_style_warning_discard_button(_pass_btn)
		else:
			_style_action_button(_pass_btn)
		_pass_btn.disabled = (not _actions_enabled) or (not playable_stage) or story_locked or request_locked or (discard_mode and (not has_hand_selection))
		if story_locked:
			_pass_btn.tooltip_text = "剧情进行中：请先完成对话。"
		elif request_locked:
			_pass_btn.tooltip_text = "回合结算中，请稍候。"
		elif discard_mode and not has_hand_selection:
			_pass_btn.tooltip_text = "需要弃牌：先选择一张手牌。"
		elif discard_mode:
			_pass_btn.tooltip_text = "提交当前选中的手牌进行弃牌。"
		elif not playable_stage:
			_pass_btn.tooltip_text = "当前关卡不可 PASS（状态：%s）。" % _state.stage_status()
		else:
			_pass_btn.tooltip_text = "不出牌直接结束回合。"


func _safe_array_size(value: Variant) -> int:
	if value is Array:
		return int(value.size())
	return 0


func _to_string_array(value: Variant) -> Array[String]:
	var out: Array[String] = []
	if not (value is Array):
		return out
	for raw in value:
		var cid := str(raw).strip_edges()
		if cid.is_empty():
			continue
		out.append(cid)
	return out


func _remove_first_card(cards: Array[String], card_id: String) -> bool:
	var target := card_id.strip_edges()
	if target.is_empty():
		return false
	for i in range(cards.size()):
		if cards[i] == target:
			cards.remove_at(i)
			return true
	return false


func _is_exhaust_card(card_id: String) -> bool:
	var cid := card_id.strip_edges().to_lower()
	if cid.is_empty():
		return false
	if cid.contains("exhaust"):
		return true
	var desc := _card_desc(cid)
	return desc.find("消耗") >= 0


func _sync_local_piles_from_turn_result(run: Dictionary, res: Dictionary) -> void:
	var deck: Array[String] = _to_string_array(run.get("deck", []))
	var discard: Array[String] = _to_string_array(run.get("discard", []))
	var action_type := str(res.get("action_type", "")).strip_edges().to_lower()

	if action_type == "combo" or action_type == "play":
		var played_cards: Array[String] = _to_string_array(res.get("played_cards", []))
		if action_type == "play" and played_cards.is_empty():
			var played_one := str(res.get("played_card", "")).strip_edges()
			if not played_one.is_empty():
				played_cards.append(played_one)
		for cid in played_cards:
			if _is_exhaust_card(cid):
				continue
			discard.append(cid)

	var drawn_cards: Array[String] = _to_string_array(res.get("drawn_cards", []))
	for cid in drawn_cards:
		if _remove_first_card(deck, cid):
			continue
		# Draw flow rule: when deck is insufficient, recycle all discard into deck, shuffle, then continue drawing.
		if not discard.is_empty():
			deck.append_array(discard)
			discard.clear()
		_remove_first_card(deck, cid)

	run["deck"] = deck
	run["discard"] = discard


func _deck_cards() -> Array[String]:
	return _to_string_array(_state.run.get("deck", []))


func _discard_cards() -> Array[String]:
	return _to_string_array(_state.run.get("discard", []))


func _deck_count() -> int:
	var run_count := _deck_cards().size()
	if _state.run.has("deck"):
		return run_count
	return int(_state.stage.get("deck_count", run_count))


func _discard_count() -> int:
	var run_count := _discard_cards().size()
	if _state.run.has("discard"):
		return run_count
	return int(_state.stage.get("discard_count", run_count))


func _set_pile_badge_text(badge: Label, count: int) -> void:
	if badge == null:
		return
	badge.text = str(max(0, count))
	badge.visible = true


func _refresh_pile_controls() -> void:
	var deck_count := _deck_count()
	var discard_count := _discard_count()
	if _draw_pile_btn != null:
		_draw_pile_btn.tooltip_text = "抽牌堆：剩余 %s 张（点击查看集合与数量）" % str(deck_count)
	if _discard_pile_btn != null:
		_discard_pile_btn.tooltip_text = "弃牌堆：当前 %s 张（点击查看集合与数量）" % str(discard_count)
	_set_pile_badge_text(_draw_pile_badge, deck_count)
	_set_pile_badge_text(_discard_pile_badge, discard_count)
	if _pile_view_dialog != null and _pile_view_dialog.visible:
		if _pile_view_title != null and _pile_view_title.text.begins_with("弃牌堆"):
			_render_pile_view_dialog("discard")
		else:
			_render_pile_view_dialog("deck")


func _render_turn_info_panel() -> void:
	if _pile_label == null or _hand_stat_label == null or _turn_hint_label == null:
		return
	var hand_count := _safe_array_size(_state.run.get("hand", []))
	var hand_limit := int(_state.run.get("hand_limit", 10))
	var deck_count := _deck_count()
	var discard_count := _discard_count()
	var turn_no := int(_state.stage.get("current_turn", 1))
	var pending_discard := _pending_discard_count()
	var run_effects: Dictionary = _state.run.get("run_effects", {})
	var momentum := int(run_effects.get("momentum", 0))
	var streak := int(run_effects.get("score_streak", 0))
	_pile_label.text = "抽牌堆 %s | 弃牌堆 %s | 手牌 %s/%s" % [
		str(deck_count),
		str(discard_count),
		str(hand_count),
		str(hand_limit),
	]
	_refresh_pile_controls()
	_hand_stat_label.text = "回合 T%s | 待执行 %s 张 | 需弃牌 %s 张 | 动量 M%s | 连得分 %s" % [
		str(turn_no),
		str(_queue_cards.size()),
		str(pending_discard),
		str(momentum),
		str(streak),
	]
	var stage_status := _state.stage_status()
	if stage_status == "playing":
		if pending_discard > 0:
			_turn_hint_label.text = "提示：当前需弃牌 %s 张，弃牌完成前无法执行/PASS。" % str(pending_discard)
			_turn_hint_label.add_theme_color_override("font_color", Color(0.95, 0.80, 0.44, 1.0))
			return
		_turn_hint_label.text = "提示：拖拽手牌到等待区，按顺序后点击执行。"
		_turn_hint_label.add_theme_color_override("font_color", Color(0.66, 0.84, 0.98, 1.0))
		return
	if stage_status == "choose_symbol":
		_turn_hint_label.text = "提示：请先在下方“标的选择”里选标，再开始本关回合。"
		_turn_hint_label.add_theme_color_override("font_color", Color(0.66, 0.84, 0.98, 1.0))
		return
	_turn_hint_label.text = "提示：当前关卡状态 %s，等待下一步操作。" % stage_status
	_turn_hint_label.add_theme_color_override("font_color", Color(0.66, 0.80, 0.96, 1.0))


func _get_card_name(card_id: String) -> String:
	var cid := card_id.strip_edges()
	return CARD_NAMES.get(cid, cid)


func _card_desc(card_id: String) -> String:
	var cid := card_id.strip_edges()
	if CARD_SHORT_DESC.has(cid):
		return str(CARD_SHORT_DESC.get(cid, ""))
	if cid.begins_with("short_"):
		return "未来5根内判定涨跌，命中得分，失手扣分。"
	if cid.begins_with("trend_"):
		return "按未来首末收盘判定趋势，结算含X。"
	if cid.begins_with("breakout_"):
		return "未来5根突破最近15根历史极值则得分。"
	if cid.begins_with("tactic_"):
		return "改变本回合结算或信心。"
	if cid.begins_with("arb_"):
		return "连续套利段按配对链与波动条件结算。"
	if cid.begins_with("option_"):
		return "按未来5根波动与方向结算期权得分。"
	return "未知卡牌"


func _card_type_key(card_id: String) -> String:
	var cid := card_id.strip_edges()
	if cid.begins_with("short_"):
		return "short"
	if cid.begins_with("trend_"):
		return "trend"
	if cid.begins_with("breakout_"):
		return "breakout"
	if cid.begins_with("tactic_"):
		return "tactic"
	if cid.begins_with("arb_"):
		return "arbitrage"
	if cid.begins_with("option_"):
		return "option"
	return "unknown"


func _card_type_label(card_id: String) -> String:
	var key := _card_type_key(card_id)
	return str(CARD_TYPE_LABELS.get(key, "未知"))


func _card_tag(card_id: String) -> String:
	match _card_type_key(card_id):
		"short":
			return "SCALP"
		"trend":
			return "TREND"
		"breakout":
			return "BREAK"
		"tactic":
			return "TACTIC"
		"arbitrage":
			return "ARB"
		"option":
			return "OPT"
		_:
			return "CARD"


func _card_tier(card_id: String) -> int:
	var cid := card_id.strip_edges()
	if cid.find("_master") >= 0:
		return 4
	if cid.find("_veteran") >= 0:
		return 3
	if cid.find("_skilled") >= 0:
		return 2
	if cid.begins_with("option_sell_"):
		return 1
	if cid.begins_with("breakout_"):
		return 2
	if cid == "tactic_leverage":
		return 3
	if cid == "tactic_scalp_cycle" or cid == "tactic_risk_control":
		return 2
	return 1


func _card_accent(card_id: String) -> Color:
	var cid := card_id.strip_edges()
	if cid.begins_with("short_long_") or cid.begins_with("trend_long_") or cid.begins_with("breakout_long_"):
		return Color(0.92, 0.30, 0.42, 1.0)
	if cid.begins_with("short_short_") or cid.begins_with("trend_short_") or cid.begins_with("breakout_short_"):
		return Color(0.22, 0.76, 0.48, 1.0)
	if cid == "tactic_quick_cancel":
		return Color(0.45, 0.74, 0.96, 1.0)
	if cid == "tactic_scalp_cycle":
		return Color(0.78, 0.50, 0.95, 1.0)
	if cid == "tactic_leverage":
		return Color(0.95, 0.66, 0.30, 1.0)
	if cid == "tactic_risk_control":
		return Color(0.50, 0.88, 0.76, 1.0)
	if cid == "tactic_meditation":
		return Color(0.84, 0.80, 0.45, 1.0)
	if cid == "tactic_dynamic_adjust":
		return Color(0.36, 0.84, 0.88, 1.0)
	if cid == "tactic_self_confidence":
		return Color(0.98, 0.56, 0.24, 1.0)
	if cid == "tactic_fast_stop":
		return Color(0.94, 0.42, 0.24, 1.0)
	if cid.begins_with("arb_"):
		return Color(0.92, 0.74, 0.22, 1.0)
	if cid.begins_with("option_buy_"):
		return Color(0.96, 0.38, 0.56, 1.0)
	if cid.begins_with("option_sell_"):
		return Color(0.42, 0.68, 0.96, 1.0)
	return Color(0.70, 0.78, 0.90, 1.0)


func _rarity_border_color(tier: int) -> Color:
	if _card_visuals != null and _card_visuals.has_method("border_for_tier"):
		return _card_visuals.call("border_for_tier", tier)
	match int(tier):
		4:
			return Color(0.98, 0.76, 0.95, 1.0)
		3:
			return Color(0.96, 0.84, 0.42, 1.0)
		2:
			return Color(0.85, 0.62, 0.32, 1.0)
		_:
			return Color(0.38, 0.56, 0.79, 1.0)


func _rarity_glow_color(tier: int) -> Color:
	if _card_visuals != null and _card_visuals.has_method("glow_for_tier"):
		return _card_visuals.call("glow_for_tier", tier)
	match int(tier):
		4:
			return Color(0.36, 0.18, 0.35, 1.0)
		3:
			return Color(0.44, 0.36, 0.15, 1.0)
		2:
			return Color(0.35, 0.25, 0.14, 1.0)
		_:
			return Color(0.16, 0.26, 0.39, 1.0)


func _frame_texture_for_tier(tier: int) -> Texture2D:
	if _card_visuals != null and _card_visuals.has_method("frame_for_tier"):
		return _card_visuals.call("frame_for_tier", tier)
	return null


func _selected_overlay_texture() -> Texture2D:
	if _card_visuals != null and _card_visuals.has_method("selected_overlay_texture"):
		return _card_visuals.call("selected_overlay_texture")
	return null


func _name_bar_tint_for_tier(tier: int) -> Color:
	if _card_visuals != null and _card_visuals.has_method("name_bar_tint_for_tier"):
		return _card_visuals.call("name_bar_tint_for_tier", tier)
	var glow := _rarity_glow_color(tier)
	return Color(glow.r, glow.g, glow.b, 0.92)


func _type_badge_bg_for_tier(tier: int) -> Color:
	if _card_visuals != null and _card_visuals.has_method("type_badge_color_for_tier"):
		return _card_visuals.call("type_badge_color_for_tier", tier)
	var glow := _rarity_glow_color(tier)
	return Color(glow.r * 0.8, glow.g * 0.8, glow.b * 0.8, 0.92)


func _make_card_style(accent: Color, tier: int, selected: bool, hover: bool, queue_card: bool) -> StyleBoxFlat:
	var sb := StyleBoxFlat.new()
	sb.set_corner_radius_all(8 if queue_card else 10)
	sb.set_border_width_all(2 if selected else 1)
	var base_bg := Color(0.10, 0.14, 0.21, 1.0) if queue_card else Color(0.09, 0.12, 0.19, 1.0)
	if selected:
		base_bg = Color(
			clamp(base_bg.r + accent.r * 0.20, 0.0, 1.0),
			clamp(base_bg.g + accent.g * 0.20, 0.0, 1.0),
			clamp(base_bg.b + accent.b * 0.20, 0.0, 1.0),
			1.0
		)
	elif hover:
		base_bg = Color(
			clamp(base_bg.r + 0.03, 0.0, 1.0),
			clamp(base_bg.g + 0.03, 0.0, 1.0),
			clamp(base_bg.b + 0.03, 0.0, 1.0),
			1.0
		)
	var rarity_glow := _rarity_glow_color(tier)
	base_bg = Color(
		clamp(base_bg.r + rarity_glow.r * 0.10, 0.0, 1.0),
		clamp(base_bg.g + rarity_glow.g * 0.10, 0.0, 1.0),
			clamp(base_bg.b + rarity_glow.b * 0.10, 0.0, 1.0),
			1.0
		)
	if queue_card:
		base_bg = Color(
			clamp(base_bg.r * 0.84 + 0.08, 0.0, 1.0),
			clamp(base_bg.g * 0.84 + 0.08, 0.0, 1.0),
			clamp(base_bg.b * 0.84 + 0.08, 0.0, 1.0),
			1.0
		)
	sb.bg_color = base_bg
	var rarity_border := _rarity_border_color(tier)
	sb.border_color = rarity_border if selected else Color(rarity_border.r, rarity_border.g, rarity_border.b, 0.90)
	if queue_card and hover:
		sb.bg_color = Color(
			clamp(sb.bg_color.r + 0.06, 0.0, 1.0),
			clamp(sb.bg_color.g + 0.05, 0.0, 1.0),
			clamp(sb.bg_color.b + 0.03, 0.0, 1.0),
			1.0
		)
		sb.border_color = Color(0.95, 0.82, 0.46, 1.0)
		if not selected:
			sb.set_border_width_all(2)
	elif queue_card and selected:
		sb.bg_color = Color(
			clamp(sb.bg_color.r + 0.04, 0.0, 1.0),
			clamp(sb.bg_color.g + 0.04, 0.0, 1.0),
			clamp(sb.bg_color.b + 0.04, 0.0, 1.0),
			1.0
		)
	return sb


func _make_sts_card_shell_style(tier: int, selected: bool, hover: bool) -> StyleBoxFlat:
	var border := _rarity_border_color(tier)
	var glow := _rarity_glow_color(tier)
	var sb := StyleBoxFlat.new()
	sb.set_corner_radius_all(14)
	sb.set_border_width_all(3 if selected else 2)
	sb.bg_color = Color(0.09 + glow.r * 0.09, 0.11 + glow.g * 0.10, 0.17 + glow.b * 0.10, 0.98)
	if hover:
		sb.bg_color = Color(
			clamp(sb.bg_color.r + 0.03, 0.0, 1.0),
			clamp(sb.bg_color.g + 0.03, 0.0, 1.0),
			clamp(sb.bg_color.b + 0.03, 0.0, 1.0),
			1.0
		)
	sb.border_color = border if selected else Color(border.r, border.g, border.b, 0.92)
	sb.shadow_color = Color(glow.r * 0.30, glow.g * 0.30, glow.b * 0.30, 0.30) if selected else Color(0.0, 0.0, 0.0, 0.38)
	sb.shadow_size = 8 if selected else 6
	sb.shadow_offset = Vector2(0, 2)
	sb.content_margin_left = 2.0
	sb.content_margin_right = 2.0
	sb.content_margin_top = 2.0
	sb.content_margin_bottom = 2.0
	return sb


func _make_art_frame_style(tier: int) -> StyleBoxFlat:
	var sb := StyleBoxFlat.new()
	sb.set_corner_radius_all(10)
	sb.set_border_width_all(2)
	var border := _rarity_border_color(tier)
	sb.border_color = Color(border.r, border.g, border.b, 0.92)
	var glow := _rarity_glow_color(tier)
	sb.bg_color = Color(0.06 + glow.r * 0.18, 0.08 + glow.g * 0.18, 0.12 + glow.b * 0.18, 0.96)
	sb.shadow_color = Color(0.0, 0.0, 0.0, 0.24)
	sb.shadow_size = 3
	sb.shadow_offset = Vector2(0, 1)
	return sb


func _make_name_bar_style(tier: int) -> StyleBoxFlat:
	var sb := StyleBoxFlat.new()
	sb.set_corner_radius_all(8)
	sb.set_border_width_all(1)
	sb.border_color = Color(_rarity_border_color(tier).r, _rarity_border_color(tier).g, _rarity_border_color(tier).b, 0.86)
	sb.bg_color = _name_bar_tint_for_tier(tier)
	sb.shadow_color = Color(0.0, 0.0, 0.0, 0.22)
	sb.shadow_size = 2
	sb.shadow_offset = Vector2(0, 1)
	return sb


func _make_type_badge_style(tier: int) -> StyleBoxFlat:
	var sb := StyleBoxFlat.new()
	sb.set_corner_radius_all(7)
	sb.set_border_width_all(1)
	sb.border_color = Color(_rarity_border_color(tier).r, _rarity_border_color(tier).g, _rarity_border_color(tier).b, 0.82)
	sb.bg_color = _type_badge_bg_for_tier(tier)
	sb.shadow_color = Color(0.0, 0.0, 0.0, 0.18)
	sb.shadow_size = 2
	sb.shadow_offset = Vector2(0, 1)
	return sb


func _card_thumbnail_symbol(card_id: String) -> String:
	var cid := card_id.strip_edges()
	if cid.begins_with("short_long_") or cid.begins_with("trend_long_") or cid.begins_with("breakout_long_"):
		return "▲"
	if cid.begins_with("short_short_") or cid.begins_with("trend_short_") or cid.begins_with("breakout_short_"):
		return "▼"
	if cid.begins_with("tactic_"):
		return "◆"
	return "●"


func _type_art_palette(card_id: String) -> Dictionary:
	match _card_type_key(card_id):
		"short":
			return {
				"top": Color(0.15, 0.29, 0.33, 1.0),
				"mid": Color(0.08, 0.20, 0.26, 1.0),
				"bottom": Color(0.05, 0.13, 0.18, 1.0),
				"motif": "candles",
				"label": "INTRADAY",
			}
		"trend":
			return {
				"top": Color(0.22, 0.19, 0.30, 1.0),
				"mid": Color(0.15, 0.14, 0.24, 1.0),
				"bottom": Color(0.08, 0.09, 0.17, 1.0),
				"motif": "trendline",
				"label": "TREND",
			}
		"breakout":
			return {
				"top": Color(0.30, 0.19, 0.16, 1.0),
				"mid": Color(0.24, 0.14, 0.11, 1.0),
				"bottom": Color(0.14, 0.09, 0.08, 1.0),
				"motif": "breakout",
				"label": "BREAKOUT",
			}
		"tactic":
			return {
				"top": Color(0.14, 0.24, 0.30, 1.0),
				"mid": Color(0.10, 0.18, 0.24, 1.0),
				"bottom": Color(0.06, 0.12, 0.16, 1.0),
				"motif": "sigil",
				"label": "TACTIC",
			}
		_:
			return {
				"top": Color(0.18, 0.23, 0.29, 1.0),
				"mid": Color(0.12, 0.17, 0.23, 1.0),
				"bottom": Color(0.08, 0.12, 0.17, 1.0),
				"motif": "none",
				"label": "CARD",
			}


func _palette_color(palette: Dictionary, key: String, fallback: Color) -> Color:
	var v: Variant = palette.get(key, fallback)
	if typeof(v) == TYPE_COLOR:
		return v
	return fallback


func _palette_text(palette: Dictionary, key: String, fallback: String) -> String:
	var v: Variant = palette.get(key, fallback)
	if typeof(v) == TYPE_STRING:
		return str(v)
	return fallback


func _make_vertical_gradient_texture(top: Color, mid: Color, bottom: Color, w: int = 180, h: int = 112) -> Texture2D:
	var grad := Gradient.new()
	grad.offsets = PackedFloat32Array([0.0, 0.48, 1.0])
	grad.colors = PackedColorArray([top, mid, bottom])
	var tex := GradientTexture2D.new()
	tex.gradient = grad
	tex.fill = GradientTexture2D.FILL_LINEAR
	tex.fill_from = Vector2(0.5, 0.0)
	tex.fill_to = Vector2(0.5, 1.0)
	tex.width = w
	tex.height = h
	return tex


func _add_thumbnail_motif(wrap: Control, motif: String, accent: Color) -> void:
	if motif == "candles":
		for i in range(6):
			var bar := ColorRect.new()
			bar.custom_minimum_size = Vector2(8, 16 + int((i % 3) * 8))
			bar.anchor_left = 0.08 + float(i) * 0.14
			bar.anchor_right = bar.anchor_left + 0.06
			bar.anchor_bottom = 0.88
			bar.anchor_top = bar.anchor_bottom - (0.16 + float(i % 3) * 0.08)
			bar.color = Color(accent.r, accent.g, accent.b, 0.30 + float(i % 2) * 0.12)
			bar.mouse_filter = Control.MOUSE_FILTER_IGNORE
			wrap.add_child(bar)
	elif motif == "trendline":
		var beam_a := ColorRect.new()
		beam_a.anchor_left = 0.12
		beam_a.anchor_top = 0.64
		beam_a.anchor_right = 0.82
		beam_a.anchor_bottom = 0.70
		beam_a.color = Color(accent.r, accent.g, accent.b, 0.36)
		beam_a.rotation_degrees = -12
		beam_a.mouse_filter = Control.MOUSE_FILTER_IGNORE
		wrap.add_child(beam_a)
		var beam_b := ColorRect.new()
		beam_b.anchor_left = 0.18
		beam_b.anchor_top = 0.46
		beam_b.anchor_right = 0.88
		beam_b.anchor_bottom = 0.52
		beam_b.color = Color(accent.r * 0.8, accent.g * 0.8, accent.b * 0.8, 0.28)
		beam_b.rotation_degrees = -8
		beam_b.mouse_filter = Control.MOUSE_FILTER_IGNORE
		wrap.add_child(beam_b)
	elif motif == "breakout":
		var base_line := ColorRect.new()
		base_line.anchor_left = 0.10
		base_line.anchor_right = 0.90
		base_line.anchor_top = 0.62
		base_line.anchor_bottom = 0.66
		base_line.color = Color(0.92, 0.86, 0.60, 0.36)
		base_line.mouse_filter = Control.MOUSE_FILTER_IGNORE
		wrap.add_child(base_line)
		var burst := ColorRect.new()
		burst.anchor_left = 0.66
		burst.anchor_right = 0.76
		burst.anchor_top = 0.24
		burst.anchor_bottom = 0.66
		burst.color = Color(accent.r, accent.g, accent.b, 0.42)
		burst.mouse_filter = Control.MOUSE_FILTER_IGNORE
		wrap.add_child(burst)
	elif motif == "sigil":
		var ring := PanelContainer.new()
		ring.anchor_left = 0.24
		ring.anchor_top = 0.20
		ring.anchor_right = 0.76
		ring.anchor_bottom = 0.72
		ring.mouse_filter = Control.MOUSE_FILTER_IGNORE
		var ring_style := StyleBoxFlat.new()
		ring_style.set_corner_radius_all(64)
		ring_style.set_border_width_all(2)
		ring_style.bg_color = Color(0.08, 0.10, 0.15, 0.22)
		ring_style.border_color = Color(accent.r, accent.g, accent.b, 0.42)
		ring.add_theme_stylebox_override("panel", ring_style)
		wrap.add_child(ring)


func _build_card_thumbnail_icon(card_id: String, icon_size: Vector2) -> Control:
	var cid := card_id.strip_edges()
	var palette := _type_art_palette(cid)
	var wrap := Control.new()
	wrap.size_flags_horizontal = Control.SIZE_EXPAND_FILL
	wrap.size_flags_vertical = Control.SIZE_EXPAND_FILL
	wrap.custom_minimum_size = icon_size
	wrap.mouse_filter = Control.MOUSE_FILTER_IGNORE

	var accent := _card_accent(cid)
	var top: Color = _palette_color(palette, "top", Color(0.16, 0.20, 0.30, 1.0))
	var mid: Color = _palette_color(palette, "mid", Color(0.10, 0.14, 0.22, 1.0))
	var bottom: Color = _palette_color(palette, "bottom", Color(0.06, 0.10, 0.16, 1.0))
	var art_bg := TextureRect.new()
	art_bg.set_anchors_preset(Control.PRESET_FULL_RECT)
	art_bg.expand_mode = TextureRect.EXPAND_IGNORE_SIZE
	art_bg.stretch_mode = TextureRect.STRETCH_SCALE
	art_bg.texture = _make_vertical_gradient_texture(top, mid, bottom)
	art_bg.mouse_filter = Control.MOUSE_FILTER_IGNORE
	wrap.add_child(art_bg)

	var ring := PanelContainer.new()
	ring.set_anchors_preset(Control.PRESET_FULL_RECT)
	ring.mouse_filter = Control.MOUSE_FILTER_IGNORE
	var ring_style := StyleBoxFlat.new()
	ring_style.set_corner_radius_all(9)
	ring_style.set_border_width_all(2)
	ring_style.bg_color = Color(0.04, 0.06, 0.10, 0.22)
	ring_style.border_color = Color(accent.r, accent.g, accent.b, 0.90)
	ring.add_theme_stylebox_override("panel", ring_style)
	wrap.add_child(ring)

	_add_thumbnail_motif(wrap, _palette_text(palette, "motif", "none"), accent)

	var scan := ColorRect.new()
	scan.anchor_left = 0.0
	scan.anchor_right = 1.0
	scan.anchor_top = 0.56
	scan.anchor_bottom = 0.60
	scan.color = Color(0.95, 0.98, 1.0, 0.08)
	scan.mouse_filter = Control.MOUSE_FILTER_IGNORE
	wrap.add_child(scan)

	var symbol := Label.new()
	symbol.set_anchors_preset(Control.PRESET_CENTER)
	symbol.position = Vector2(-18, -19)
	symbol.custom_minimum_size = Vector2(36, 36)
	symbol.horizontal_alignment = HORIZONTAL_ALIGNMENT_CENTER
	symbol.vertical_alignment = VERTICAL_ALIGNMENT_CENTER
	symbol.text = _card_thumbnail_symbol(cid)
	symbol.add_theme_font_size_override("font_size", 28)
	symbol.add_theme_color_override("font_color", Color(0.95, 0.97, 1.0, 1.0))
	symbol.mouse_filter = Control.MOUSE_FILTER_IGNORE
	wrap.add_child(symbol)

	var type_chip := Label.new()
	type_chip.anchor_left = 0.06
	type_chip.anchor_right = 0.62
	type_chip.anchor_top = 0.04
	type_chip.anchor_bottom = 0.20
	type_chip.horizontal_alignment = HORIZONTAL_ALIGNMENT_CENTER
	type_chip.vertical_alignment = VERTICAL_ALIGNMENT_CENTER
	type_chip.text = _palette_text(palette, "label", "CARD")
	type_chip.add_theme_font_size_override("font_size", 9)
	type_chip.add_theme_color_override("font_color", Color(0.90, 0.96, 1.0, 0.92))
	type_chip.mouse_filter = Control.MOUSE_FILTER_IGNORE
	wrap.add_child(type_chip)

	return wrap


func _build_sts_hand_card(card_id: String, index: int, selected: bool) -> Button:
	var cid := card_id.strip_edges()
	var tier := _card_tier(cid)
	var btn: Button = CARD_TILE_SCRIPT.new() as Button
	btn.setup(index, cid, "hand", true)
	btn.custom_minimum_size = Vector2(HAND_CARD_WIDTH, HAND_CARD_HEIGHT)
	btn.size_flags_horizontal = Control.SIZE_SHRINK_CENTER
	btn.size_flags_vertical = Control.SIZE_SHRINK_CENTER
	btn.text = ""
	btn.alignment = HORIZONTAL_ALIGNMENT_LEFT
	btn.clip_contents = false
	btn.mouse_default_cursor_shape = Control.CURSOR_POINTING_HAND
	btn.pivot_offset = Vector2(HAND_CARD_WIDTH * 0.5, HAND_CARD_HEIGHT * 0.82)
	btn.add_theme_stylebox_override("normal", _make_sts_card_shell_style(tier, selected, false))
	btn.add_theme_stylebox_override("hover", _make_sts_card_shell_style(tier, selected, true))
	btn.add_theme_stylebox_override("pressed", _make_sts_card_shell_style(tier, true, true))
	btn.tooltip_text = "%s\n%s\n[%s]" % [_get_card_name(cid), _card_desc(cid), cid]

	var frame_tex := _frame_texture_for_tier(tier)
	if frame_tex != null:
		var frame_layer := TextureRect.new()
		frame_layer.set_anchors_preset(Control.PRESET_FULL_RECT)
		frame_layer.texture = frame_tex
		frame_layer.expand_mode = TextureRect.EXPAND_IGNORE_SIZE
		frame_layer.stretch_mode = TextureRect.STRETCH_SCALE
		frame_layer.modulate = Color(1, 1, 1, 0.22)
		frame_layer.mouse_filter = Control.MOUSE_FILTER_IGNORE
		btn.add_child(frame_layer)
	var selected_overlay := _selected_overlay_texture()
	if selected and selected_overlay != null:
		var over := TextureRect.new()
		over.set_anchors_preset(Control.PRESET_FULL_RECT)
		over.texture = selected_overlay
		over.expand_mode = TextureRect.EXPAND_IGNORE_SIZE
		over.stretch_mode = TextureRect.STRETCH_SCALE
		over.modulate = Color(1, 1, 1, 0.20)
		over.mouse_filter = Control.MOUSE_FILTER_IGNORE
		btn.add_child(over)

	var edge_glow := ColorRect.new()
	edge_glow.set_anchors_preset(Control.PRESET_FULL_RECT)
	edge_glow.mouse_filter = Control.MOUSE_FILTER_IGNORE
	var border := _rarity_border_color(tier)
	edge_glow.color = Color(border.r, border.g, border.b, 0.10 if selected else 0.0)
	btn.add_child(edge_glow)

	var root := MarginContainer.new()
	root.set_anchors_preset(Control.PRESET_FULL_RECT)
	root.add_theme_constant_override("margin_left", 8)
	root.add_theme_constant_override("margin_right", 8)
	root.add_theme_constant_override("margin_top", 8)
	root.add_theme_constant_override("margin_bottom", 8)
	root.mouse_filter = Control.MOUSE_FILTER_IGNORE
	btn.add_child(root)

	var v := VBoxContainer.new()
	v.size_flags_vertical = Control.SIZE_EXPAND_FILL
	v.add_theme_constant_override("separation", 6)
	v.mouse_filter = Control.MOUSE_FILTER_IGNORE
	root.add_child(v)

	var name_bar := PanelContainer.new()
	name_bar.custom_minimum_size = Vector2(0, 26)
	name_bar.mouse_filter = Control.MOUSE_FILTER_IGNORE
	name_bar.add_theme_stylebox_override("panel", _make_name_bar_style(tier))
	v.add_child(name_bar)
	var name_lb := Label.new()
	name_lb.set_anchors_preset(Control.PRESET_FULL_RECT)
	name_lb.horizontal_alignment = HORIZONTAL_ALIGNMENT_CENTER
	name_lb.vertical_alignment = VERTICAL_ALIGNMENT_CENTER
	name_lb.text = _get_card_name(cid)
	name_lb.add_theme_font_size_override("font_size", 12)
	name_lb.add_theme_color_override("font_color", Color(0.95, 0.98, 1.0, 1.0))
	name_lb.mouse_filter = Control.MOUSE_FILTER_IGNORE
	name_bar.add_child(name_lb)

	var art_frame := PanelContainer.new()
	art_frame.size_flags_vertical = Control.SIZE_EXPAND_FILL
	art_frame.custom_minimum_size = Vector2(0, 78)
	art_frame.add_theme_stylebox_override("panel", _make_art_frame_style(tier))
	art_frame.mouse_filter = Control.MOUSE_FILTER_IGNORE
	v.add_child(art_frame)
	var art_margin := MarginContainer.new()
	art_margin.set_anchors_preset(Control.PRESET_FULL_RECT)
	art_margin.add_theme_constant_override("margin_left", 5)
	art_margin.add_theme_constant_override("margin_right", 5)
	art_margin.add_theme_constant_override("margin_top", 5)
	art_margin.add_theme_constant_override("margin_bottom", 5)
	art_margin.mouse_filter = Control.MOUSE_FILTER_IGNORE
	art_frame.add_child(art_margin)
	art_margin.add_child(_build_card_thumbnail_icon(cid, Vector2(0, 0)))

	var type_badge := PanelContainer.new()
	type_badge.custom_minimum_size = Vector2(0, 20)
	type_badge.add_theme_stylebox_override("panel", _make_type_badge_style(tier))
	type_badge.mouse_filter = Control.MOUSE_FILTER_IGNORE
	v.add_child(type_badge)
	var type_lb := Label.new()
	type_lb.set_anchors_preset(Control.PRESET_FULL_RECT)
	type_lb.horizontal_alignment = HORIZONTAL_ALIGNMENT_CENTER
	type_lb.vertical_alignment = VERTICAL_ALIGNMENT_CENTER
	type_lb.text = _card_type_label(cid)
	type_lb.add_theme_font_size_override("font_size", 11)
	type_lb.add_theme_color_override("font_color", Color(0.92, 0.96, 1.0, 1.0))
	type_lb.mouse_filter = Control.MOUSE_FILTER_IGNORE
	type_badge.add_child(type_lb)

	var desc_lb := Label.new()
	desc_lb.size_flags_vertical = Control.SIZE_EXPAND_FILL
	desc_lb.autowrap_mode = TextServer.AUTOWRAP_WORD_SMART
	desc_lb.text = _card_desc(cid)
	desc_lb.add_theme_font_size_override("font_size", 11)
	desc_lb.add_theme_color_override("font_color", Color(0.89, 0.94, 1.0, 1.0))
	desc_lb.mouse_filter = Control.MOUSE_FILTER_IGNORE
	v.add_child(desc_lb)

	btn.mouse_entered.connect(func() -> void:
		if not is_instance_valid(btn):
			return
		var tw := create_tween()
		tw.tween_property(btn, "scale", Vector2(1.05, 1.05), 0.10).set_trans(Tween.TRANS_SINE).set_ease(Tween.EASE_OUT)
		tw.parallel().tween_property(edge_glow, "color:a", 0.22, 0.10).set_trans(Tween.TRANS_SINE).set_ease(Tween.EASE_OUT)
	)
	btn.mouse_exited.connect(func() -> void:
		if not is_instance_valid(btn):
			return
		var tw := create_tween()
		tw.tween_property(btn, "scale", Vector2.ONE, 0.10).set_trans(Tween.TRANS_SINE).set_ease(Tween.EASE_OUT)
		tw.parallel().tween_property(edge_glow, "color:a", 0.10 if selected else 0.0, 0.10).set_trans(Tween.TRANS_SINE).set_ease(Tween.EASE_OUT)
	)
	return btn


func _build_card_button(card_id: String, index: int, queue_card: bool, selected: bool) -> Button:
	var cid := card_id.strip_edges()
	var name := _get_card_name(cid)
	if name.is_empty():
		name = "未知卡牌"
	if cid.is_empty():
		cid = "unknown"
	if not queue_card:
		return _build_sts_hand_card(cid, index, selected)
	var tier := _card_tier(cid)
	var accent := _card_accent(cid)
	var btn: Button = CARD_TILE_SCRIPT.new() as Button
	if btn.has_method("setup"):
		btn.call("setup", index, cid, "queue" if queue_card else "hand", true)
	btn.custom_minimum_size = Vector2(0, 46)
	btn.size_flags_horizontal = Control.SIZE_EXPAND_FILL
	btn.alignment = HORIZONTAL_ALIGNMENT_LEFT
	btn.add_theme_font_size_override("font_size", 13)
	btn.add_theme_color_override("font_color", Color(0.93, 0.97, 1.0, 1.0))
	btn.add_theme_stylebox_override("normal", _make_card_style(accent, tier, selected, false, queue_card))
	btn.add_theme_stylebox_override("hover", _make_card_style(accent, tier, selected, true, queue_card))
	btn.add_theme_stylebox_override("pressed", _make_card_style(accent, tier, true, false, queue_card))
	btn.mouse_default_cursor_shape = Control.CURSOR_POINTING_HAND
	btn.pivot_offset = btn.custom_minimum_size * 0.5
	btn.add_theme_font_size_override("font_size", 12)
	btn.text = "顺序 %02d  %s | %s" % [index + 1, name, _card_type_label(cid)]
	btn.tooltip_text = "%s\n%s\n[%s]" % [name, _card_desc(cid), cid]
	btn.modulate = Color(0.92, 0.95, 1.0, 0.94)

	btn.mouse_entered.connect(func() -> void:
		if not is_instance_valid(btn):
			return
		var tw := create_tween()
		var target := Vector2(1.05, 1.05) if not queue_card else Vector2(1.04, 1.04)
		tw.tween_property(btn, "scale", target, 0.10).set_trans(Tween.TRANS_SINE).set_ease(Tween.EASE_OUT)
		if queue_card:
			tw.parallel().tween_property(btn, "modulate", Color(1, 1, 1, 1), 0.10).set_trans(Tween.TRANS_SINE).set_ease(Tween.EASE_OUT)
	)
	btn.mouse_exited.connect(func() -> void:
		if not is_instance_valid(btn):
			return
		var tw := create_tween()
		var target := Vector2(1.03, 1.03) if (selected and not queue_card) else Vector2(1.0, 1.0)
		tw.tween_property(btn, "scale", target, 0.10).set_trans(Tween.TRANS_SINE).set_ease(Tween.EASE_OUT)
		if queue_card:
			tw.parallel().tween_property(btn, "modulate", Color(0.92, 0.95, 1.0, 0.94), 0.10).set_trans(Tween.TRANS_SINE).set_ease(Tween.EASE_OUT)
	)
	return btn


func _normalize_queue_against_hand() -> void:
	var hand_cards: Array = _state.hand_cards()
	var counts := {}
	for c in hand_cards:
		var key := str(c)
		counts[key] = int(counts.get(key, 0)) + 1
	var used := {}
	var next_queue: Array[String] = []
	for c in _queue_cards:
		var key := str(c)
		var n := int(used.get(key, 0))
		if n < int(counts.get(key, 0)):
			next_queue.append(key)
			used[key] = n + 1
	_queue_cards = next_queue


func _rebuild_hand_view_cards() -> void:
	var hand_cards: Array = _state.hand_cards()
	var queued_counts := {}
	for c in _queue_cards:
		var key := str(c)
		queued_counts[key] = int(queued_counts.get(key, 0)) + 1

	var consumed := {}
	var view: Array[String] = []
	for c in hand_cards:
		var key := str(c)
		var used := int(consumed.get(key, 0))
		var queued := int(queued_counts.get(key, 0))
		if used < queued:
			consumed[key] = used + 1
			continue
		view.append(key)
	_hand_view_cards = view


func _render_state() -> void:
	var current_run_id := int(_state.run.get("run_id", 0))
	if current_run_id > 0 and _dev_metrics_run_id > 0 and _dev_metrics_run_id != current_run_id:
		_dev_turn_metrics.clear()
		_dev_metrics_run_id = current_run_id
		_render_dev_metrics()
	var run_effects: Dictionary = _state.run.get("run_effects", {})
	var momentum := int(run_effects.get("momentum", 0))
	var streak := int(run_effects.get("score_streak", 0))
	_run_label.text = "Run#%s  %s  Σ%s  C%s" % [
		str(_state.run.get("run_id", "-")),
		str(_state.run_status()),
		str(_state.run.get("total_score", "-")),
		str(_state.run.get("confidence", "-")),
	]
	_run_label.text += "  M%s  S%s" % [str(momentum), str(streak)]
	_stage_label.text = "Stage %s · T%s · %s/%s" % [
		str(_state.stage_status()),
		str(_state.stage.get("current_turn", "-")),
		str(_state.stage.get("stage_score", "-")),
		str(_state.stage.get("target_score", "-")),
	]
	_render_turn_info_panel()
	_render_hand()
	_render_queue()
	_render_candidates()
	_render_upgrades()
	_render_chart()
	_maybe_trigger_victory_story()
	_apply_turn_action_locks()
	_apply_responsive_layout()
	_refresh_settings_overlay_info()


func _render_meta() -> void:
	if _last_meta.is_empty():
		_meta_label.text = "Meta: -"
		return
	_meta_label.text = "Meta: Lv.%s | EXP=%s | SP=%s | Cleared=%s" % [
		str(_last_meta.get("level", "-")),
		str(_last_meta.get("exp", "-")),
		str(_last_meta.get("skill_points", "-")),
		str(_last_meta.get("games_cleared", "-")),
	]


func _render_hand() -> void:
	_normalize_queue_against_hand()
	_rebuild_hand_view_cards()
	_clear_dynamic_children(_hand_cards_stage)
	if _hand_view_cards.is_empty():
		var empty := Label.new()
		empty.text = "当前无可用手牌"
		empty.add_theme_color_override("font_color", Color(0.56, 0.66, 0.80, 1.0))
		empty.position = Vector2(16, 16)
		_hand_cards_stage.add_child(empty)
		_selected_hand_idx = -1
		_hand_focus_label.text = "选中卡牌：-"
		_apply_turn_action_locks()
		return
	_selected_hand_idx = int(clamp(_selected_hand_idx, 0, _hand_view_cards.size() - 1))
	for i in range(_hand_view_cards.size()):
		var card_id := str(_hand_view_cards[i]).strip_edges()
		var selected := i == _selected_hand_idx
		var btn := _build_card_button(card_id, i, false, selected)
		if btn == null:
			continue
		btn.pressed.connect(_on_hand_card_pressed.bind(i))
		_hand_cards_stage.add_child(btn)
	var focus_id := str(_hand_view_cards[_selected_hand_idx]).strip_edges()
	_hand_focus_label.text = "选中卡牌：%s | %s" % [_get_card_name(focus_id), _card_desc(focus_id)]
	_layout_hand_fan_cards()
	_schedule_hand_layout_reflow()
	_apply_turn_action_locks()


func _schedule_hand_layout_reflow() -> void:
	if _hand_layout_reflow_pending:
		return
	_hand_layout_reflow_pending = true
	call_deferred("_flush_hand_layout_reflow")


func _flush_hand_layout_reflow() -> void:
	_hand_layout_reflow_pending = false
	_layout_hand_fan_cards(false)


func _layout_hand_fan_cards(animate: bool = false) -> void:
	if _hand_cards_stage == null:
		return
	var card_nodes: Array = []
	for c in _hand_cards_stage.get_children():
		if c is Button and not c.is_queued_for_deletion():
			card_nodes.append(c)
	if card_nodes.is_empty():
		return
	var avail_w := maxf(float(_hand_cards_stage.size.x), HAND_CARD_WIDTH + 24.0)
	var avail_h := maxf(float(_hand_cards_stage.size.y), HAND_CARD_HEIGHT + 10.0)
	var n := card_nodes.size()
	var spread := HAND_CARD_SPREAD_MAX
	if n > 1:
		spread = clampf((avail_w - HAND_CARD_WIDTH) / maxf(1.0, float(n - 1)), HAND_CARD_SPREAD_MIN, HAND_CARD_SPREAD_MAX)
	else:
		spread = 0.0
	var fan_angle_max := clampf(2.0 + float(n) * 0.7, 6.0, 14.0)
	var total_w := HAND_CARD_WIDTH + float(max(0, n - 1)) * spread
	var free_w := maxf(0.0, avail_w - total_w)
	# Stable anchor: fixed-left baseline with capped adaptive padding (prevents right drift when hand shrinks).
	var start_x := 12.0 + minf(36.0, free_w * 0.06)
	var base_y := clampf(avail_h - HAND_CARD_HEIGHT - 2.0, 0.0, 20.0)
	for i in range(n):
		var btn: Button = card_nodes[i]
		btn.scale = Vector2.ONE
		var t := 0.0
		if n > 1:
			t = (float(i) / float(n - 1)) * 2.0 - 1.0
		var angle := t * fan_angle_max
		var y_offset: float = absf(t) * 22.0
		btn.position = Vector2(start_x + float(i) * spread, base_y + y_offset)
		btn.rotation_degrees = angle
		btn.z_index = 200 + i
		if i == _selected_hand_idx:
			btn.z_index = 500
			btn.position.y -= 8.0
		if animate:
			_animate_card_spawn(btn, i, true)


func _make_queue_insert_marker() -> Control:
	var marker := HBoxContainer.new()
	marker.custom_minimum_size = Vector2(0, 12)
	var line := ColorRect.new()
	line.size_flags_horizontal = Control.SIZE_EXPAND_FILL
	line.custom_minimum_size = Vector2(0, 3)
	line.color = Color(0.96, 0.77, 0.28, 0.95)
	marker.add_child(line)
	return marker


func _render_queue() -> void:
	_clear_dynamic_children(_queue_cards_box)
	var marker_idx := _queue_drop_insert_idx
	var animate_cards := marker_idx < 0
	if marker_idx >= 0:
		marker_idx = int(clamp(marker_idx, 0, _queue_cards.size()))
	if _queue_cards.is_empty():
		if marker_idx == 0:
			_queue_cards_box.add_child(_make_queue_insert_marker())
		var empty := Label.new()
		empty.text = "等待区为空"
		empty.add_theme_color_override("font_color", Color(0.58, 0.70, 0.84, 1.0))
		_queue_cards_box.add_child(empty)
		_selected_queue_idx = -1
		_render_turn_info_panel()
		_apply_turn_action_locks()
		return
	_selected_queue_idx = int(clamp(_selected_queue_idx, 0, _queue_cards.size() - 1))
	for i in range(_queue_cards.size()):
		if marker_idx == i:
			_queue_cards_box.add_child(_make_queue_insert_marker())
		var card_id := str(_queue_cards[i]).strip_edges()
		var selected := i == _selected_queue_idx
		var btn := _build_card_button(card_id, i, true, selected)
		if btn == null:
			continue
		btn.pressed.connect(_on_queue_card_pressed.bind(i))
		_queue_cards_box.add_child(btn)
		if animate_cards:
			_animate_card_spawn(btn, i, false)
	if marker_idx == _queue_cards.size():
		_queue_cards_box.add_child(_make_queue_insert_marker())
	_render_turn_info_panel()
	_apply_turn_action_locks()


func _clear_dynamic_children(box: Node) -> void:
	for c in box.get_children():
		if c.has_meta("keep_on_clear") and bool(c.get_meta("keep_on_clear")):
			continue
		box.remove_child(c)
		c.queue_free()


func _animate_card_spawn(ctrl: Control, idx: int, big_card: bool) -> void:
	if ctrl == null:
		return
	var target_scale := Vector2.ONE
	ctrl.modulate = Color(1, 1, 1, 0.0)
	ctrl.scale = target_scale * (0.92 if big_card else 0.96)
	var tw := create_tween()
	tw.tween_interval(float(min(idx, 8)) * 0.018)
	tw.tween_property(ctrl, "modulate", Color(1, 1, 1, 1.0), 0.14).set_trans(Tween.TRANS_SINE).set_ease(Tween.EASE_OUT)
	tw.parallel().tween_property(ctrl, "scale", target_scale, 0.18).set_trans(Tween.TRANS_BACK).set_ease(Tween.EASE_OUT)


func _play_queue_cast_animation() -> void:
	var cards: Array = []
	for c in _queue_cards_box.get_children():
		if c is Control and c is Button:
			cards.append(c)
	if cards.is_empty():
		return
	for i in range(cards.size()):
		var card: Control = cards[i]
		var tw := create_tween()
		tw.tween_interval(float(i) * 0.045)
		tw.tween_property(card, "scale", Vector2(0.92, 0.92), 0.10).set_trans(Tween.TRANS_SINE).set_ease(Tween.EASE_IN)
		tw.parallel().tween_property(card, "modulate", Color(1, 1, 1, 0.20), 0.10).set_trans(Tween.TRANS_SINE).set_ease(Tween.EASE_IN)


func _play_queue_sweep_flash() -> void:
	if _queue_drop_zone == null or not is_instance_valid(_queue_drop_zone):
		return
	var beam := ColorRect.new()
	beam.mouse_filter = Control.MOUSE_FILTER_IGNORE
	beam.color = Color(0.98, 0.86, 0.45, 0.0)
	var beam_w: float = max(120.0, _queue_drop_zone.size.x * 0.20)
	var beam_h: float = max(36.0, _queue_drop_zone.size.y)
	beam.size = Vector2(beam_w, beam_h)
	beam.position = Vector2(-beam_w, 0)
	_queue_drop_zone.add_child(beam)
	var tw := create_tween()
	tw.tween_property(beam, "color:a", 0.28, 0.08).set_trans(Tween.TRANS_SINE).set_ease(Tween.EASE_OUT)
	tw.parallel().tween_property(beam, "position:x", _queue_drop_zone.size.x + beam_w, 0.30).set_trans(Tween.TRANS_QUAD).set_ease(Tween.EASE_OUT)
	tw.tween_property(beam, "color:a", 0.0, 0.14).set_trans(Tween.TRANS_SINE).set_ease(Tween.EASE_IN)
	tw.tween_callback(func() -> void:
		if is_instance_valid(beam):
			beam.queue_free()
	)


func _on_hand_card_pressed(index: int) -> void:
	_selected_hand_idx = index
	_render_hand()
	_apply_turn_action_locks()


func _on_queue_card_pressed(index: int) -> void:
	_selected_queue_idx = index
	_render_queue()
	_apply_turn_action_locks()


func _enqueue_hand_card_by_index(hand_index: int, expected_card_id: String = "", insert_idx: int = -1) -> void:
	if _is_discard_mode():
		_set_status("当前必须先弃牌，不能加入等待区。")
		return
	if not _is_turn_playable_stage():
		_set_status("当前关卡不可排队：%s" % _state.stage_status())
		_append_log("入队已阻止：关卡状态 %s 非 playing。" % _state.stage_status())
		return
	if hand_index < 0 or hand_index >= _hand_view_cards.size():
		return
	var idx := hand_index
	var card_id := str(_hand_view_cards[idx]).strip_edges()
	var expected := expected_card_id.strip_edges()
	if not expected.is_empty() and expected != card_id:
		for i in range(_hand_view_cards.size()):
			if str(_hand_view_cards[i]).strip_edges() == expected:
				idx = i
				card_id = expected
				break
	if card_id.is_empty():
		_append_log("加入等待区失败：卡牌ID为空。")
		return
	var use_insert := insert_idx
	if use_insert < 0 or use_insert > _queue_cards.size():
		use_insert = _queue_cards.size()
	_queue_cards.insert(use_insert, card_id)
	_selected_queue_idx = use_insert
	_selected_hand_idx = -1
	_clear_queue_drop_marker()
	_play_ui_sfx("card_pick", randf_range(0.97, 1.03))
	_render_hand()
	_render_queue()


func _on_queue_drop_from_hand(hand_index: int, card_id: String) -> void:
	if _is_discard_mode():
		return
	_enqueue_hand_card_by_index(hand_index, card_id, _queue_drop_insert_idx)


func _resolve_queue_index(idx: int, card_id: String) -> int:
	if idx >= 0 and idx < _queue_cards.size():
		if card_id.strip_edges().is_empty() or str(_queue_cards[idx]).strip_edges() == card_id.strip_edges():
			return idx
	var target := card_id.strip_edges()
	if target.is_empty():
		return -1
	for i in range(_queue_cards.size()):
		if str(_queue_cards[i]).strip_edges() == target:
			return i
	return -1


func _queue_insert_index_from_global_y(global_y: float) -> int:
	if _queue_cards.is_empty():
		return 0
	var btn_nodes: Array = []
	for c in _queue_cards_box.get_children():
		if c is Button:
			btn_nodes.append(c)
	if btn_nodes.is_empty():
		return _queue_cards.size()
	for i in range(btn_nodes.size()):
		var one: Control = btn_nodes[i]
		var mid := one.global_position.y + one.size.y * 0.5
		if global_y <= mid:
			return i
	return btn_nodes.size()


func _on_queue_drop_reorder(queue_index: int, card_id: String, global_y: float) -> void:
	var from_idx := _resolve_queue_index(queue_index, card_id)
	if from_idx < 0 or from_idx >= _queue_cards.size():
		return
	var to_idx := _queue_insert_index_from_global_y(global_y)
	to_idx = int(clamp(to_idx, 0, _queue_cards.size()))
	if to_idx > from_idx:
		to_idx -= 1
	if to_idx < 0:
		to_idx = 0
	if to_idx == from_idx:
		return
	var moved := str(_queue_cards[from_idx]).strip_edges()
	_queue_cards.remove_at(from_idx)
	_queue_cards.insert(to_idx, moved)
	_selected_queue_idx = to_idx
	_clear_queue_drop_marker()
	_play_ui_sfx("queue_reorder", randf_range(0.98, 1.02))
	_render_queue()


func _on_hand_drop_from_queue(queue_index: int, card_id: String) -> void:
	var idx := _resolve_queue_index(queue_index, card_id)
	if idx < 0 or idx >= _queue_cards.size():
		return
	_queue_cards.remove_at(idx)
	_selected_queue_idx = int(min(idx, _queue_cards.size() - 1))
	_clear_queue_drop_marker()
	_play_ui_sfx("card_return", randf_range(0.98, 1.02))
	_render_hand()
	_render_queue()


func _refresh_queue_drop_hint() -> void:
	if _queue_drop_zone == null:
		return
	if _queue_drop_insert_idx < 0:
		_queue_drop_zone.modulate = Color(1, 1, 1, 0.92)
		return
	_queue_drop_zone.modulate = Color(1.0, 1.0, 1.0, 1.0)


func _clear_queue_drop_marker() -> void:
	if _queue_drop_insert_idx < 0:
		return
	_queue_drop_insert_idx = -1
	_refresh_queue_drop_hint()


func _on_queue_drag_hover(global_y: float, drag_type: String) -> void:
	if drag_type != "hand_card" and drag_type != "queue_card":
		return
	var next_idx := _queue_insert_index_from_global_y(global_y)
	next_idx = int(clamp(next_idx, 0, _queue_cards.size()))
	if next_idx == _queue_drop_insert_idx:
		return
	_queue_drop_insert_idx = next_idx
	_refresh_queue_drop_hint()
	_render_queue()


func _on_queue_drag_end() -> void:
	if _queue_drop_insert_idx < 0:
		return
	_clear_queue_drop_marker()
	_render_queue()


func _render_chart() -> void:
	if _chart_view == null:
		return
	if _state.stage.is_empty():
		_chart_view.clear_data()
		return
	_chart_view.set_stage_data(_state.stage)


func _clear_container(box: VBoxContainer) -> void:
	for c in box.get_children():
		c.queue_free()


func _render_candidates() -> void:
	var stage_status := _state.stage_status()
	var should_show := false
	if not _pending_candidates.is_empty():
		should_show = true
	elif stage_status == "choose_symbol" and not _state.candidate_pool().is_empty():
		should_show = true
	if _cand_panel != null:
		_cand_panel.visible = should_show
	if not should_show:
		return

	_clear_container(_cand_container)
	var pool: Array = []
	if not _pending_candidates.is_empty():
		pool = _pending_candidates
	else:
		pool = _state.candidate_pool()
	if pool.is_empty():
		var lb := Label.new()
		lb.text = "当前无候选标的。"
		_cand_container.add_child(lb)
		return

	for c in pool:
		var one: Dictionary = c
		var sym := str(one.get("symbol", "N/A"))
		var sym_name := str(one.get("symbol_name", sym))
		var typ := str(one.get("symbol_type", "unknown"))
		var btn := Button.new()
		btn.text = "选择 %s (%s / %s)" % [sym_name, sym, typ]
		btn.pressed.connect(_on_candidate_selected.bind(sym))
		_cand_container.add_child(btn)


func _render_upgrades() -> void:
	_clear_container(_upgrade_container)
	var lb := Label.new()
	lb.text = "Card V2 已关闭关卡强化系统。"
	_upgrade_container.add_child(lb)


func boot_with_session(base_url: String, username: String, session_token: String, entry_mode: String = "resume") -> void:
	_map_mode = false
	_map_run_id = 0
	_map_battle_run_id = 0
	_map_exit_emitted = false
	if _base_url_input != null:
		_base_url_input.text = base_url.strip_edges()
	if _username_input != null:
		_username_input.text = username.strip_edges()
	if _token_input != null:
		_token_input.text = session_token.strip_edges()
	_boot_entry_mode = entry_mode.strip_edges()
	await _connect_and_boot(true, true, true)
	if _boot_entry_mode == "create":
		await _on_create_run_pressed()
	else:
		await _on_resume_pressed()


func boot_from_map_battle(base_url: String, username: String, session_token: String, battle_run_id: int, map_run_id: int) -> void:
	_map_mode = true
	_map_run_id = int(map_run_id)
	_map_battle_run_id = int(battle_run_id)
	_map_exit_emitted = false
	if _base_url_input != null:
		_base_url_input.text = base_url.strip_edges()
	if _username_input != null:
		_username_input.text = username.strip_edges()
	if _token_input != null:
		_token_input.text = session_token.strip_edges()
	await _connect_and_boot(false, false, false)
	if _map_battle_run_id <= 0:
		_set_status("地图战斗启动失败：battle run 无效。")
		return
	_state.run_id = _map_battle_run_id
	_append_log("地图战斗接入：MapRun#%s -> BattleRun#%s" % [str(_map_run_id), str(_map_battle_run_id)])
	await _refresh_state(true)


func _connect_and_boot(do_health_check: bool = true, refresh_meta: bool = true, refresh_state: bool = true) -> void:
	_api.base_url = _base_url_input.text.strip_edges()
	_api.set_auth(_username_input.text, _token_input.text)
	_update_action_enabled(false)
	_set_status("连接中...")
	if do_health_check:
		var health: Dictionary = await _api.get_json("/v1/card/health")
		if not health.get("ok", false):
			_set_status("连接失败：%s" % str(health.get("message", "unknown")))
			_append_log("连接健康检查失败：%s" % _api_error_text(health))
			return
	_update_action_enabled(true)
	_set_status("连接成功。")
	if do_health_check:
		_append_log("连接健康检查通过。")
	if refresh_meta:
		await _refresh_meta()
	if refresh_state:
		await _refresh_state(false)


func _on_connect_pressed() -> void:
	await _connect_and_boot(true, true, true)


func _on_refresh_meta_pressed() -> void:
	await _refresh_meta()


func _refresh_meta() -> void:
	var meta: Dictionary = await _api.post_json("/v1/card/meta/get", {})
	if not meta.get("ok", false):
		_append_log("读取独立经验失败：%s" % _api_error_text(meta))
		return
	_last_meta = meta
	_render_meta()
	_append_log("独立经验已刷新。")


func _on_create_run_pressed() -> void:
	var res: Dictionary = await _api.post_json("/v1/card/run/create", {})
	if not res.get("ok", false):
		_set_status("创建对局失败：%s" % str(res.get("message", "unknown")))
		_append_log("新开局失败：%s" % _api_error_text(res))
		return
	_state.run_id = int(res.get("run_id", 0))
	_pending_candidates = []
	_dev_turn_metrics.clear()
	_dev_metrics_run_id = _state.run_id
	_render_dev_metrics()
	_queue_cards.clear()
	_selected_hand_idx = -1
	_selected_queue_idx = -1
	if _story_active:
		_close_story_overlay()
	_append_log("已新开局：Run#%s" % str(_state.run_id))
	await _refresh_state(true)


func _on_resume_pressed() -> void:
	var res_v: Variant = await _api.post_json("/v1/card/run/resume", {})
	var res: Dictionary = res_v if typeof(res_v) == TYPE_DICTIONARY else {}
	if not res.get("ok", false):
		_append_log("恢复对局失败：%s" % _api_error_text(res))
		return
	var run_raw: Variant = res.get("run", {})
	var run: Dictionary = run_raw if typeof(run_raw) == TYPE_DICTIONARY else {}
	if run.is_empty():
		_set_status("没有可恢复的未完成局。")
		_append_log("没有可恢复的未完成局。")
		return
	_state.run_id = int(run.get("run_id", 0))
	_selected_hand_idx = -1
	_selected_queue_idx = -1
	_append_log("已恢复对局：Run#%s" % str(_state.run_id))
	_dev_metrics_run_id = _state.run_id
	await _refresh_state(true)


func _on_refresh_state_pressed() -> void:
	await _refresh_state(false)


func _refresh_state(auto_prepare: bool) -> void:
	if _state.run_id <= 0:
		return
	var state_res: Dictionary = await _api.post_json("/v1/card/run/state", {"run_id": _state.run_id})
	if not state_res.get("ok", false):
		_set_status("读取状态失败：%s" % str(state_res.get("message", "unknown")))
		_append_log("读取对局状态失败：%s" % _api_error_text(state_res))
		return
	_state.apply_state_payload(state_res)
	if auto_prepare and _state.run_status() == "await_stage_start":
		await _prepare_current_stage()
		return
	_render_state()


func _on_prepare_stage_pressed() -> void:
	await _prepare_current_stage()


func _prepare_current_stage() -> void:
	if _state.run_id <= 0:
		return
	var payload := {
		"run_id": _state.run_id,
		"stage_no": _state.current_stage_no(),
	}
	var res: Dictionary = await _api.post_json("/v1/card/stage/start", payload)
	if not res.get("ok", false):
		if int(res.get("result_code", -1)) == HTTPRequest.RESULT_TIMEOUT:
			_set_status("准备关卡超时，后台仍在处理中，正在同步状态...")
			_append_log("准备关卡超时，正在自动同步状态。")
			await _refresh_state_after_timeout()
			return
		_set_status("准备关卡失败：%s" % str(res.get("message", "unknown")))
		_append_log("准备关卡失败：%s" % _api_error_text(res))
		return
	if bool(res.get("need_choice", false)):
		_pending_candidates = res.get("candidates", [])
		if _pending_candidates.is_empty():
			_set_status("关卡候选为空，无法自动选标。")
			_append_log("关卡准备失败：候选标的为空。")
			return
		var pick_idx := int(randi_range(0, _pending_candidates.size() - 1))
		var picked_v: Variant = _pending_candidates[pick_idx]
		var picked: Dictionary = picked_v if typeof(picked_v) == TYPE_DICTIONARY else {}
		var symbol := str(picked.get("symbol", "")).strip_edges()
		if symbol.is_empty():
			for one in _pending_candidates:
				var cand: Dictionary = one if typeof(one) == TYPE_DICTIONARY else {}
				var cand_symbol := str(cand.get("symbol", "")).strip_edges()
				if not cand_symbol.is_empty():
					symbol = cand_symbol
					break
		if symbol.is_empty():
			_set_status("候选标的无效，无法自动选标。")
			_append_log("关卡准备失败：候选标的缺少 symbol。")
			return
		_append_log("关卡已准备：自动随机标的 %s。" % symbol)
		await _on_candidate_selected(symbol)
		return
	else:
		_pending_candidates = []
	_append_log("关卡准备完成。")
	await _refresh_state(false)


func _on_candidate_selected(symbol: String) -> void:
	if _state.run_id <= 0:
		return
	var payload := {
		"run_id": _state.run_id,
		"stage_no": _state.current_stage_no(),
		"symbol_choice": symbol,
	}
	var res: Dictionary = await _api.post_json("/v1/card/stage/start", payload)
	if not res.get("ok", false):
		if int(res.get("result_code", -1)) == HTTPRequest.RESULT_TIMEOUT:
			_set_status("选标请求超时，正在同步状态...")
			_append_log("选标请求超时，正在自动同步状态。")
			await _refresh_state_after_timeout()
			return
		_set_status("选标失败：%s" % str(res.get("message", "unknown")))
		_append_log("选标失败：%s" % _api_error_text(res))
		return
	_pending_candidates = []
	_append_log("已选择标的：%s" % symbol)
	await _refresh_state(false)


func _refresh_state_after_timeout() -> void:
	await get_tree().create_timer(0.45).timeout
	await _refresh_state(false)
	if _state.stage_status() == "playing" or _state.stage_status() == "choose_symbol":
		_set_status("关卡状态已同步。")
		_append_log("超时后状态同步成功。")


func _on_add_to_queue_pressed() -> void:
	if _is_discard_mode():
		_set_status("当前必须先弃牌。")
		return
	if _selected_hand_idx < 0 or _selected_hand_idx >= _hand_view_cards.size():
		return
	_enqueue_hand_card_by_index(_selected_hand_idx)


func _has_trend_direction_conflict(cards: Array[String]) -> bool:
	var has_trend_long := false
	var has_trend_short := false
	var has_breakout_long := false
	var has_breakout_short := false
	for cid in cards:
		if cid.begins_with("trend_long_"):
			has_trend_long = true
		elif cid.begins_with("trend_short_"):
			has_trend_short = true
		elif cid.begins_with("breakout_long_"):
			has_breakout_long = true
		elif cid.begins_with("breakout_short_"):
			has_breakout_short = true
		if has_trend_long and has_trend_short:
			return true
		if has_breakout_long and has_breakout_short:
			return true
		if (has_trend_long and has_breakout_short) or (has_trend_short and has_breakout_long):
			return true
	return false


func _local_combo_conflict_message(cards: Array[String]) -> String:
	if _has_trend_direction_conflict(cards):
		return "方向冲突：顺势多/空不能同回合；突破追多/追空不能同回合；突破追多不能配顺势做空，突破追空不能配顺势做多。"
	var has_buy_call := false
	var has_sell_call := false
	var has_buy_put := false
	var has_sell_put := false
	var arb_regions := {}
	for cid in cards:
		if cid.begins_with("option_buy_call_"):
			has_buy_call = true
		elif cid.begins_with("option_sell_call_"):
			has_sell_call = true
		elif cid.begins_with("option_buy_put_"):
			has_buy_put = true
		elif cid.begins_with("option_sell_put_"):
			has_sell_put = true
		elif cid.begins_with("arb_"):
			var region := ""
			if cid.begins_with("arb_east_"):
				region = "east"
			elif cid.begins_with("arb_west_"):
				region = "west"
			elif cid.begins_with("arb_south_"):
				region = "south"
			elif cid.begins_with("arb_north_"):
				region = "north"
			if region != "":
				if arb_regions.has(region):
					return "套利冲突：同一区域（东/西/南/北）的套利牌不能同回合重复执行。"
				arb_regions[region] = true
	if has_buy_call and has_sell_call:
		return "期权冲突：买看涨与卖看涨不能同回合同时执行。"
	if has_buy_put and has_sell_put:
		return "期权冲突：买看跌与卖看跌不能同回合同时执行。"
	return ""


func _local_combo_warning_message(cards: Array[String]) -> String:
	for i in range(cards.size()):
		var cid := str(cards[i]).strip_edges()
		if cid != "tactic_fast_stop":
			continue
		for offset in [1, 2]:
			var target_idx: int = i + int(offset)
			if target_idx >= cards.size():
				continue
			var target := str(cards[target_idx]).strip_edges()
			if target.begins_with("breakout_") or target.begins_with("option_buy_"):
				return "提示：快速止损后两张牌中若包含突破牌或买方期权，该位置会占位但不会被保护。"
	return ""


func _selected_hand_card_id() -> String:
	if _selected_hand_idx < 0 or _selected_hand_idx >= _hand_view_cards.size():
		return ""
	return str(_hand_view_cards[_selected_hand_idx]).strip_edges()


func _on_execute_queue_pressed() -> void:
	if not _is_turn_playable_stage():
		_set_status("当前关卡不可执行：%s" % _state.stage_status())
		_append_log("执行已阻止：关卡状态 %s 非 playing。" % _state.stage_status())
		return
	if _turn_action_inflight:
		return
	if _is_discard_mode():
		_set_status("当前需先弃牌，无法执行。")
		return
	if _queue_cards.is_empty():
		return
	var local_conflict := _local_combo_conflict_message(_queue_cards)
	if not local_conflict.is_empty():
		_set_status(local_conflict)
		_append_log("执行已阻止：%s" % local_conflict)
		return
	var local_warning := _local_combo_warning_message(_queue_cards)
	if not local_warning.is_empty():
		_append_log(local_warning)
	_clear_queue_drop_marker()
	_turn_action_inflight = true
	_apply_turn_action_locks()
	_set_status("正在执行回合...")
	_play_queue_sweep_flash()
	_play_ui_sfx("queue_execute", randf_range(0.96, 1.03))
	_play_queue_cast_animation()
	var res: Dictionary = await _api.post_json(
		"/v1/card/turn/play",
		{
			"run_id": _state.run_id,
			"type": "combo",
			"cards": _queue_cards,
		}
	)
	_turn_action_inflight = false
	_apply_turn_action_locks()
	if not res.get("ok", false):
		_set_status("回合执行失败：%s" % str(res.get("message", "unknown")))
		_append_log("回合执行失败：%s" % _api_error_text(res))
		_render_queue()
		return
	_queue_cards.clear()
	_selected_queue_idx = -1
	_record_turn_metric(res)
	_queue_turn_log_async(res)
	_play_settlement_fx(res)
	await _post_turn_settlement(res)


func _on_pass_pressed() -> void:
	if _turn_action_inflight:
		return
	if _is_discard_mode():
		var discard_card := _selected_hand_card_id()
		if discard_card.is_empty():
			_set_status("请先选择一张手牌用于弃牌。")
			return
		_turn_action_inflight = true
		_apply_turn_action_locks()
		var discard_res: Dictionary = await _api.post_json(
			"/v1/card/turn/play",
			{
				"run_id": _state.run_id,
				"type": "discard",
				"cards": [discard_card],
			}
		)
		_turn_action_inflight = false
		_apply_turn_action_locks()
		if not discard_res.get("ok", false):
			_set_status("弃牌失败：%s" % str(discard_res.get("message", "unknown")))
			_append_log("弃牌失败：%s" % _api_error_text(discard_res))
			return
		_append_log("已弃牌：%s" % _get_card_name(discard_card))
		await _refresh_state(false)
		return
	if not _is_turn_playable_stage():
		_set_status("当前关卡不可 PASS：%s" % _state.stage_status())
		_append_log("PASS 已阻止：关卡状态 %s 非 playing。" % _state.stage_status())
		return
	_clear_queue_drop_marker()
	_turn_action_inflight = true
	_apply_turn_action_locks()
	_set_status("正在PASS结算...")
	_play_ui_sfx("turn_pass", randf_range(0.98, 1.02))
	var res: Dictionary = await _api.post_json(
		"/v1/card/turn/play",
		{
			"run_id": _state.run_id,
			"type": "pass",
		}
	)
	_turn_action_inflight = false
	_apply_turn_action_locks()
	if not res.get("ok", false):
		_set_status("PASS失败：%s" % str(res.get("message", "unknown")))
		_append_log("PASS失败：%s" % _api_error_text(res))
		return
	_record_turn_metric(res)
	_queue_turn_log_async(res)
	_play_settlement_fx(res)
	await _post_turn_settlement(res)


func _append_turn_log(res: Dictionary) -> void:
	var score := int(res.get("turn_score", 0))
	var conf_delta := int(res.get("confidence_delta", 0))
	var action_type := str(res.get("action_type", ""))
	var lines: Array[String] = []
	lines.append("结算[%s] 回合得分%s | 信心%s" % [action_type, _fmt_delta(score), _fmt_delta(conf_delta)])
	if action_type == "combo":
		var cards: Array = res.get("played_cards", [])
		var names: Array[String] = []
		for c in cards:
			names.append(_get_card_name(str(c)))
		lines.append("出牌顺序：%s" % " -> ".join(names))
	else:
		lines.append("动作：%s" % action_type)
	var card_results: Array = res.get("card_results", [])
	if not card_results.is_empty():
		lines.append("逐卡结算：")
		for one in card_results:
			if one is Dictionary:
				lines.append("  " + _fmt_turn_card_result_line(one))
	var mechanics: Dictionary = res.get("mechanics", {})
	if not mechanics.is_empty():
		if bool(mechanics.get("short_breakout_misfire_applied", false)):
			var misfire_dir := str(mechanics.get("short_breakout_direction", "none"))
			var misfire_label := "突破"
			match misfire_dir:
				"long_breakout":
					misfire_label = "多头突破"
				"short_breakout":
					misfire_label = "空头突破"
				"both":
					misfire_label = "双向突破"
				_:
					misfire_label = "突破"
			lines.append("!!! 短线突破严重失误：检测到%s，短线总计覆盖为-8（短线基础分/配对/连排信心作废）" % misfire_label)
		var subtotal_short := int(mechanics.get("subtotal_short", 0))
		var subtotal_trend_raw := int(mechanics.get("subtotal_trend_raw", 0))
		var subtotal_trend_final := int(mechanics.get("subtotal_trend_final", subtotal_trend_raw))
		var subtotal_breakout := int(mechanics.get("subtotal_breakout", 0))
		var trend_mult := float(mechanics.get("trend_multiplier", 1.0))
		lines.append(
			"小计：短线%s | 趋势原始%s | 趋势结算%s(x%s) | 突破%s"
			% [
				_fmt_delta(subtotal_short),
				_fmt_delta(subtotal_trend_raw),
				_fmt_delta(subtotal_trend_final),
				str(trend_mult),
				_fmt_delta(subtotal_breakout),
			]
		)
		var pair_bonus := int(mechanics.get("short_pair_bonus", 0))
		var streak_bonus := int(mechanics.get("short_streak_conf_bonus", 0))
		if pair_bonus != 0 or streak_bonus != 0:
			lines.append("短线机制：配对奖励%s | 同向连击信心+%s" % [_fmt_delta(pair_bonus), str(streak_bonus)])
		var tactic_chain: Array = mechanics.get("tactic_chain", [])
		if not tactic_chain.is_empty():
			lines.append("战术链：")
			for node in tactic_chain:
				if node is Dictionary:
					lines.append("  " + _fmt_tactic_chain_line(node))
		var confidence_events: Array = mechanics.get("confidence_events", [])
		if not confidence_events.is_empty():
			var event_txt: Array[String] = []
			for e in confidence_events:
				if e is Dictionary:
					var code := str(e.get("code", ""))
					var delta := int(e.get("delta", 0))
					event_txt.append("%s(%s)" % [_fmt_conf_event_label(code), _fmt_delta(delta)])
			if not event_txt.is_empty():
				lines.append("信心事件：%s" % "，".join(event_txt))
	var momentum_before := int(mechanics.get("momentum_before", 0))
	var momentum_after := int(mechanics.get("momentum_after", momentum_before))
	var trend_gain := int(mechanics.get("trend_gain", 0))
	var trend_loss := int(mechanics.get("trend_loss", 0))
	var momentum_delta := int(mechanics.get("momentum_delta", momentum_after - momentum_before))
	lines.append(
		"动量来源：趋势命中 +%s | 趋势失手 -%s | 净变化 %s | M%s -> M%s"
		% [str(trend_gain), str(trend_loss), _fmt_delta(momentum_delta), str(momentum_before), str(momentum_after)]
	)
	var drawn: Array = res.get("drawn_cards", [])
	if not drawn.is_empty():
		var names: Array[String] = []
		for c in drawn:
			names.append(_get_card_name(str(c)))
		lines.append("回合抽牌：%s" % ", ".join(names))
	_append_log("\n".join(lines))


func _queue_turn_log_async(res: Dictionary) -> void:
	var snap: Dictionary = res.duplicate(true)
	call_deferred("_append_turn_log_deferred", snap)


func _append_turn_log_deferred(res: Dictionary) -> void:
	_append_turn_log(res)


func _play_settlement_fx(res: Dictionary) -> void:
	var score_delta := int(res.get("turn_score", 0))
	var conf_delta := int(res.get("confidence_delta", 0))
	var stage_delta := int(res.get("stage_score_delta", score_delta))
	_apply_fx_label(_fx_score_label, "回合得分 Δ %s" % _fmt_delta(score_delta), score_delta)
	_apply_fx_label(_fx_conf_label, "信心变化 Δ %s" % _fmt_delta(conf_delta), conf_delta)
	_apply_fx_label(_fx_stage_label, "关卡分变化 Δ %s" % _fmt_delta(stage_delta), stage_delta)
	_spawn_turn_floating_fx(score_delta, conf_delta, stage_delta)
	if score_delta >= 0:
		_play_ui_sfx("score_up", randf_range(0.98, 1.04))
	else:
		_play_ui_sfx("score_down", randf_range(0.96, 1.02))
	_show_turn_settlement(res)


func _spawn_turn_floating_fx(score_delta: int, conf_delta: int, stage_delta: int) -> void:
	if _floating_fx_layer == null:
		return
	var anchor := Vector2(size.x * 0.50 - 110.0, 210.0)
	if _settle_panel != null:
		anchor = Vector2(max(20.0, _settle_panel.global_position.x + 34.0), _settle_panel.global_position.y + 16.0)
	_spawn_floating_delta("回合 %s" % _fmt_delta(score_delta), _delta_color(score_delta), anchor + Vector2(0, 0), 0.00)
	_spawn_floating_delta("信心 %s" % _fmt_delta(conf_delta), _delta_color(conf_delta), anchor + Vector2(12, 22), 0.06)
	_spawn_floating_delta("关卡 %s" % _fmt_delta(stage_delta), _delta_color(stage_delta), anchor + Vector2(26, 44), 0.12)


func _spawn_floating_delta(text: String, color: Color, start_pos: Vector2, delay_sec: float) -> void:
	if _floating_fx_layer == null:
		return
	var lb := Label.new()
	lb.text = text
	lb.visible = false
	lb.position = start_pos
	lb.scale = Vector2(0.92, 0.92)
	lb.modulate = Color(color.r, color.g, color.b, 0.0)
	lb.add_theme_font_size_override("font_size", 19)
	lb.add_theme_color_override("font_color", color)
	_floating_fx_layer.add_child(lb)
	var tw := create_tween()
	if delay_sec > 0.0:
		tw.tween_interval(delay_sec)
	tw.tween_callback(func() -> void:
		if is_instance_valid(lb):
			lb.visible = true
	)
	tw.tween_property(lb, "scale", Vector2(1.04, 1.04), 0.16).set_trans(Tween.TRANS_BACK).set_ease(Tween.EASE_OUT)
	tw.parallel().tween_property(lb, "modulate", Color(color.r, color.g, color.b, 1.0), 0.14).set_trans(Tween.TRANS_SINE).set_ease(Tween.EASE_OUT)
	tw.parallel().tween_property(lb, "position:y", start_pos.y - 36.0, 0.54).set_trans(Tween.TRANS_QUAD).set_ease(Tween.EASE_OUT)
	tw.tween_property(lb, "modulate", Color(color.r, color.g, color.b, 0.0), 0.22).set_trans(Tween.TRANS_SINE).set_ease(Tween.EASE_IN)
	tw.tween_callback(func() -> void:
		if is_instance_valid(lb):
			lb.queue_free()
	)


func _apply_fx_label(lb: Label, text: String, delta_value: int) -> void:
	if lb == null:
		return
	lb.text = text
	lb.scale = Vector2(0.88, 0.88)
	lb.modulate = Color(1, 1, 1, 0.35)
	lb.add_theme_color_override("font_color", _delta_color(delta_value))
	var tw := create_tween()
	tw.set_parallel(true)
	tw.tween_property(lb, "scale", Vector2(1.0, 1.0), 0.28).set_trans(Tween.TRANS_BACK).set_ease(Tween.EASE_OUT)
	tw.tween_property(lb, "modulate", Color(1, 1, 1, 1.0), 0.32).set_trans(Tween.TRANS_SINE).set_ease(Tween.EASE_OUT)


func _delta_color(v: int) -> Color:
	if v > 0:
		return Color(0.96, 0.35, 0.45, 1.0)  # red
	if v < 0:
		return Color(0.20, 0.82, 0.50, 1.0)  # green
	return Color(0.84, 0.90, 0.98, 1.0)


func _fmt_delta(v: int) -> String:
	return "+%s" % str(v) if v > 0 else str(v)


func _fmt_conf_event_label(code: String) -> String:
	match code:
		"pass_penalty":
			return "PASS惩罚"
		"turn_negative_penalty":
			return "负分惩罚"
		"turn_gt_50_bonus":
			return "高分奖励(>50)"
		"card_mechanics":
			return "卡牌机制"
		"three_turn_score_streak":
			return "连得分奖励(3回合)"
		"total_score_negative_end":
			return "总分<0惩罚"
		"empty_hand_penalty":
			return "空手惩罚"
		_:
			return code


func _fmt_turn_card_result_line(item: Dictionary) -> String:
	var order := int(item.get("order", 0))
	var cid := str(item.get("card_id", ""))
	var name := _get_card_name(cid)
	var score := int(item.get("raw_score", 0))
	var hit := bool(item.get("hit", false))
	var ctype := str(item.get("card_type", ""))
	var status := "命中" if hit else "失手"
	var extra_parts: Array[String] = []
	if item.has("x"):
		extra_parts.append("X=%s" % str(item.get("x", 0)))
	if ctype == "trend":
		if item.has("first_close") and item.has("last_close"):
			extra_parts.append(
				"首/末收 %.4f/%.4f"
				% [float(item.get("first_close", 0.0)), float(item.get("last_close", 0.0))]
			)
		if item.has("trend_delta_pct"):
			extra_parts.append("Δ%.4f%%" % float(item.get("trend_delta_pct", 0.0)))
	if ctype == "breakout":
		if item.has("history_high") and item.has("history_low"):
			extra_parts.append(
				"历史高/低 %.4f/%.4f"
				% [float(item.get("history_high", 0.0)), float(item.get("history_low", 0.0))]
			)
		if item.has("future_high") and item.has("future_low"):
			extra_parts.append(
				"未来高/低 %.4f/%.4f"
				% [float(item.get("future_high", 0.0)), float(item.get("future_low", 0.0))]
			)
	if ctype == "option":
		var option_style := str(item.get("option_style", ""))
		var option_side := str(item.get("option_side", ""))
		if option_style == "buy":
			var entry_cost := int(item.get("option_entry_cost", 0))
			var yz := int(item.get("option_metric_yz", 0))
			var win_mult := int(item.get("option_win_mult", 0))
			var reward_offset := int(item.get("option_reward_offset", 0))
			var reward_units := int(item.get("option_reward_units", max(yz - reward_offset, 0)))
			var reward_before_crit := int(item.get("option_reward_before_crit", reward_units * win_mult))
			var crit := bool(item.get("option_crit", false))
			var symbol := "Y" if option_side == "call" else "Z"
			extra_parts.append("先扣%s" % _fmt_delta(-entry_cost))
			if bool(item.get("option_success", false)):
				extra_parts.append("%s=%s" % [symbol, str(yz)])
				extra_parts.append("max(%s-%s,0)=%s" % [symbol, str(reward_offset), str(reward_units)])
				extra_parts.append("%s*%s=%s" % [str(reward_units), str(win_mult), _fmt_delta(reward_before_crit)])
				if crit:
					extra_parts.append("暴击x2(按净值)")
				extra_parts.append("净值=%s" % _fmt_delta(score))
			else:
				extra_parts.append("%s未触发" % symbol)
				extra_parts.append("奖励+0")
				extra_parts.append("净值=%s" % _fmt_delta(score))
		elif option_style == "sell":
			if item.has("option_seller_fail_pct") and item.has("option_seller_severe_fail_pct"):
				extra_parts.append(
					"阈值>%s%%/严重>%s%%"
					% [
						str(item.get("option_seller_fail_pct", 0.0)),
						str(item.get("option_seller_severe_fail_pct", 0.0)),
					]
				)
			if bool(item.get("option_severe_fail", false)):
				extra_parts.append("严重失败")
	if ctype == "short" and bool(item.get("short_breakout_misfire", false)):
		extra_parts.append("短线严重失误覆盖(本回合短线总计=-8)")
	if item.has("final_score"):
		var final_score := int(item.get("final_score", score))
		if final_score != score:
			extra_parts.append("最终%s" % _fmt_delta(final_score))
	var extra := ""
	if not extra_parts.is_empty():
		extra = " | %s" % " | ".join(extra_parts)
	return "%02d) %s [%s] %s %s%s" % [order, name, _card_type_label(cid), status, _fmt_delta(score), extra]


func _fmt_tactic_chain_line(node: Dictionary) -> String:
	var cid := str(node.get("card_id", ""))
	var effect := str(node.get("effect", ""))
	var name := _get_card_name(cid)
	var details: Array[String] = []
	if node.has("score_multiplier"):
		details.append("倍率x%s" % str(node.get("score_multiplier", 1.0)))
	if node.has("score_delta"):
		details.append("分数%s" % _fmt_delta(int(round(float(node.get("score_delta", 0.0))))))
	if node.has("confidence_delta"):
		details.append("信心%s" % _fmt_delta(int(node.get("confidence_delta", 0))))
	if node.has("extra_draw_gain"):
		details.append("下回合额外抽牌+%s" % str(node.get("extra_draw_gain", 0)))
	if bool(node.get("voided", false)):
		details.append("作废(%s)" % str(node.get("reason", "")))
	if details.is_empty():
		details.append(effect)
	return "%s：%s" % [name, " | ".join(details)]


func _show_turn_settlement(res: Dictionary) -> void:
	if _settle_panel == null:
		return
	var turn_score := int(res.get("turn_score", 0))
	var conf_before := int(res.get("confidence_before", int(_state.run.get("confidence", 80))))
	var conf_after := int(res.get("confidence", conf_before + int(res.get("confidence_delta", 0))))
	var stage_before := int(res.get("stage_score_before", int(_state.stage.get("stage_score", 0))))
	var stage_after := int(res.get("stage_score", stage_before + int(res.get("stage_score_delta", 0))))
	var target: int = max(1, int(res.get("target_score", int(_state.stage.get("target_score", 1)))))
	var total_before := int(res.get("total_score_before", int(_state.run.get("total_score", 0))))
	var total_after := int(res.get("total_score", total_before + int(res.get("total_score_delta", 0))))
	var stage_from: int = clampi(stage_before, 0, target)
	var stage_to: int = clampi(stage_after, 0, target)
	var conf_from: int = clampi(conf_before, 0, 100)
	var conf_to: int = clampi(conf_after, 0, 100)
	var conf_delta: int = conf_after - conf_before
	var stage_delta: int = stage_after - stage_before

	_settle_delta_label.text = "本回合得分 %s" % _fmt_delta(0)
	_settle_delta_label.add_theme_color_override("font_color", _delta_color(turn_score))
	_settle_detail_label.text = "信心 %s -> %s (%s) | 关卡分 %s -> %s | 总分 %s -> %s" % [
		str(conf_before),
		str(conf_after),
		_fmt_delta(conf_delta),
		str(stage_before),
		str(stage_after),
		str(total_before),
		str(total_after),
	]

	_settle_stage_bar.min_value = 0.0
	_settle_stage_bar.max_value = float(target)
	_settle_stage_bar.value = float(stage_from)
	_settle_stage_value.text = "%s / %s" % [str(stage_from), str(target)]
	_settle_stage_value.add_theme_color_override("font_color", _delta_color(stage_delta))

	_settle_conf_bar.min_value = 0.0
	_settle_conf_bar.max_value = 100.0
	_settle_conf_bar.value = float(conf_from)
	_settle_conf_value.text = "%s" % str(conf_from)
	_settle_conf_value.add_theme_color_override("font_color", _delta_color(conf_delta))
	_settle_detail_label.modulate = Color(1, 1, 1, 0.68)

	_settle_panel.scale = Vector2(0.98, 0.98)
	_settle_panel.modulate = Color(1, 1, 1, 0.30)
	_settle_conf_bar.modulate = Color(1, 1, 1, 0.88)
	_settle_conf_value.modulate = Color(1, 1, 1, 0.92)
	_settle_stage_value.modulate = Color(1, 1, 1, 0.92)

	var settle_dur := 0.52

	var tw := create_tween()
	tw.set_parallel(true)
	tw.tween_property(_settle_panel, "scale", Vector2(1.0, 1.0), settle_dur).set_trans(Tween.TRANS_BACK).set_ease(Tween.EASE_OUT)
	tw.tween_property(_settle_panel, "modulate", Color(1, 1, 1, 1.0), settle_dur).set_trans(Tween.TRANS_SINE).set_ease(Tween.EASE_OUT)
	tw.tween_property(_settle_stage_bar, "value", float(stage_to), settle_dur).set_trans(Tween.TRANS_QUAD).set_ease(Tween.EASE_OUT)
	tw.tween_property(_settle_conf_bar, "value", float(conf_to), settle_dur).set_trans(Tween.TRANS_QUAD).set_ease(Tween.EASE_OUT)
	tw.tween_property(_settle_detail_label, "modulate", Color(1, 1, 1, 1.0), settle_dur).set_trans(Tween.TRANS_SINE).set_ease(Tween.EASE_OUT)

	var tw_num := create_tween()
	tw_num.set_parallel(true)
	tw_num.tween_method(
		func(v: float) -> void:
			var value_int := int(round(v))
			_settle_delta_label.text = "本回合得分 %s" % _fmt_delta(value_int),
		0.0,
		float(turn_score),
		settle_dur * 0.86
	).set_trans(Tween.TRANS_CUBIC).set_ease(Tween.EASE_OUT)
	tw_num.tween_method(
		func(v: float) -> void:
			_settle_stage_value.text = "%s / %s" % [str(int(round(v))), str(target)],
		float(stage_from),
		float(stage_to),
		settle_dur
	).set_trans(Tween.TRANS_QUAD).set_ease(Tween.EASE_OUT)
	tw_num.tween_method(
		func(v: float) -> void:
			_settle_conf_value.text = "%s" % str(int(round(v))),
		float(conf_from),
		float(conf_to),
		settle_dur
	).set_trans(Tween.TRANS_QUAD).set_ease(Tween.EASE_OUT)

	var conf_flash := _delta_color(conf_delta)
	_settle_conf_bar.modulate = Color(conf_flash.r, conf_flash.g, conf_flash.b, 0.82)
	_settle_conf_value.modulate = Color(conf_flash.r, conf_flash.g, conf_flash.b, 0.96)
	var tw_flash := create_tween()
	tw_flash.set_parallel(true)
	tw_flash.tween_property(_settle_conf_bar, "modulate", Color(1, 1, 1, 1.0), settle_dur).set_trans(Tween.TRANS_SINE).set_ease(Tween.EASE_OUT)
	tw_flash.tween_property(_settle_conf_value, "modulate", Color(1, 1, 1, 1.0), settle_dur).set_trans(Tween.TRANS_SINE).set_ease(Tween.EASE_OUT)
	tw_flash.tween_property(_settle_stage_value, "modulate", Color(1, 1, 1, 1.0), settle_dur).set_trans(Tween.TRANS_SINE).set_ease(Tween.EASE_OUT)


func _apply_turn_result_to_local_state(res: Dictionary) -> void:
	if _state.run.is_empty() or _state.stage.is_empty():
		return
	var run: Dictionary = _state.run.duplicate(true)
	var stage: Dictionary = _state.stage.duplicate(true)
	var mechanics: Dictionary = res.get("mechanics", {})
	var turn_no := int(res.get("turn_no", stage.get("current_turn", 1)))
	var stage_complete := bool(res.get("stage_complete", false))
	var next_turn := turn_no if stage_complete else (turn_no + 1)

	run["run_id"] = int(res.get("run_id", run.get("run_id", _state.run_id)))
	run["status"] = str(res.get("run_status", run.get("status", "")))
	run["current_stage"] = int(res.get("stage_no", run.get("current_stage", 1)))
	run["current_turn"] = next_turn
	run["current_stage_score"] = int(res.get("stage_score", run.get("current_stage_score", 0)))
	run["total_score"] = int(res.get("total_score", run.get("total_score", 0)))
	run["confidence"] = int(res.get("confidence", run.get("confidence", 80)))
	if res.has("hand"):
		run["hand"] = res.get("hand", [])
	run["hand_limit"] = int(res.get("hand_limit", run.get("hand_limit", 10)))
	_sync_local_piles_from_turn_result(run, res)
	run["pending_upgrades"] = res.get("pending_upgrades", run.get("pending_upgrades", []))

	var effects_var: Variant = run.get("run_effects", {})
	var run_effects: Dictionary = effects_var if typeof(effects_var) == TYPE_DICTIONARY else {}
	run_effects["rules_version"] = str(res.get("rule_version", run_effects.get("rules_version", "")))
	run_effects["momentum"] = int(mechanics.get("momentum_after", run_effects.get("momentum", 0)))
	run_effects["score_streak"] = int(mechanics.get("score_streak_after", run_effects.get("score_streak", 0)))
	var extra_draw_gain := int(mechanics.get("extra_draw_next_turn_gain", 0))
	if extra_draw_gain > 0:
		run_effects["extra_draw_next_turn"] = extra_draw_gain
		run_effects["extra_draw_pending_turn"] = turn_no + 1
	else:
		run_effects["extra_draw_next_turn"] = 0
		run_effects["extra_draw_pending_turn"] = 0
	var pending_discard := int(res.get("pending_discard", 0))
	if pending_discard > 0:
		run_effects["pending_discard"] = pending_discard
	else:
		run_effects.erase("pending_discard")
	run["run_effects"] = run_effects

	stage["status"] = str(res.get("stage_status", stage.get("status", "")))
	stage["stage_score"] = int(res.get("stage_score", stage.get("stage_score", 0)))
	stage["target_score"] = int(res.get("target_score", stage.get("target_score", 0)))
	stage["current_turn"] = next_turn
	var current_visible := int(stage.get("visible_end", 20))
	var revealed := _safe_array_size(res.get("reveal_bars", []))
	var bars: Array = stage.get("bars", [])
	if revealed > 0:
		if bars.is_empty():
			stage["visible_end"] = current_visible + revealed
		else:
			stage["visible_end"] = min(current_visible + revealed, bars.size())

	_state.run = run
	_state.stage = stage
	_state.run_id = int(run.get("run_id", _state.run_id))
	_render_dev_metrics()


func _record_turn_metric(res: Dictionary) -> void:
	if not bool(res.get("ok", true)):
		return
	var run_id := int(res.get("run_id", _state.run_id))
	if run_id <= 0:
		return
	if _dev_metrics_run_id != run_id:
		_dev_turn_metrics.clear()
		_dev_metrics_run_id = run_id
	var card_results: Array = res.get("card_results", [])
	var total_cards := 0
	var total_hits := 0
	var trend_total := 0
	var trend_hits := 0
	var breakout_total := 0
	var breakout_hits := 0
	for one in card_results:
		if not (one is Dictionary):
			continue
		total_cards += 1
		var item: Dictionary = one
		var hit := bool(item.get("hit", false))
		if hit:
			total_hits += 1
		var ctype := str(item.get("card_type", ""))
		if ctype == "trend":
			trend_total += 1
			if hit:
				trend_hits += 1
		elif ctype == "breakout":
			breakout_total += 1
			if hit:
				breakout_hits += 1
	var mechanics: Dictionary = res.get("mechanics", {})
	var momentum_before := int(mechanics.get("momentum_before", 0))
	var momentum_after := int(mechanics.get("momentum_after", momentum_before))
	var row := {
		"score": int(res.get("turn_score", 0)),
		"cards": total_cards,
		"hits": total_hits,
		"trend_total": trend_total,
		"trend_hits": trend_hits,
		"breakout_total": breakout_total,
		"breakout_hits": breakout_hits,
		"momentum_before": momentum_before,
		"momentum_after": momentum_after,
	}
	_dev_turn_metrics.append(row)
	if _dev_turn_metrics.size() > DEV_METRIC_WINDOW:
		_dev_turn_metrics.remove_at(0)
	_render_dev_metrics()


func _render_dev_metrics() -> void:
	if _dev_metrics_box == null:
		return
	if _dev_turn_metrics.is_empty():
		_dev_metrics_box.clear()
		_dev_metrics_box.append_text("暂无回合数据。\n执行回合后会显示：均分、命中率、动量变化。")
		return
	var n := _dev_turn_metrics.size()
	var score_sum := 0
	var pos_turns := 0
	var total_cards := 0
	var total_hits := 0
	var trend_total := 0
	var trend_hits := 0
	var breakout_total := 0
	var breakout_hits := 0
	var momentum_delta_sum := 0
	var momentum_up := 0
	var momentum_down := 0
	var score_tail: Array[String] = []
	for row_v in _dev_turn_metrics:
		if not (row_v is Dictionary):
			continue
		var row: Dictionary = row_v
		var sc := int(row.get("score", 0))
		score_sum += sc
		if sc > 0:
			pos_turns += 1
		total_cards += int(row.get("cards", 0))
		total_hits += int(row.get("hits", 0))
		trend_total += int(row.get("trend_total", 0))
		trend_hits += int(row.get("trend_hits", 0))
		breakout_total += int(row.get("breakout_total", 0))
		breakout_hits += int(row.get("breakout_hits", 0))
		var mb := int(row.get("momentum_before", 0))
		var ma := int(row.get("momentum_after", mb))
		var md := ma - mb
		momentum_delta_sum += md
		if md > 0:
			momentum_up += 1
		elif md < 0:
			momentum_down += 1
		score_tail.append(str(sc))
	var avg_score := float(score_sum) / maxf(1.0, float(n))
	var pos_rate := float(pos_turns) / maxf(1.0, float(n)) * 100.0
	var hit_rate := float(total_hits) / maxf(1.0, float(max(1, total_cards))) * 100.0
	var trend_hit_rate := float(trend_hits) / maxf(1.0, float(max(1, trend_total))) * 100.0
	var breakout_hit_rate := float(breakout_hits) / maxf(1.0, float(max(1, breakout_total))) * 100.0
	var avg_momentum_delta := float(momentum_delta_sum) / maxf(1.0, float(n))
	_dev_metrics_box.clear()
	_dev_metrics_box.append_text("样本回合：%d\n" % n)
	_dev_metrics_box.append_text("平均回合分：%.2f | 正分回合率：%.1f%%\n" % [avg_score, pos_rate])
	_dev_metrics_box.append_text("逐卡命中率：%.1f%% (%d/%d)\n" % [hit_rate, total_hits, total_cards])
	_dev_metrics_box.append_text("趋势命中率：%.1f%% (%d/%d) | 突破命中率：%.1f%% (%d/%d)\n" % [
		trend_hit_rate, trend_hits, trend_total, breakout_hit_rate, breakout_hits, breakout_total
	])
	_dev_metrics_box.append_text("动量均值变化：%.2f | 上升回合：%d | 下降回合：%d\n" % [avg_momentum_delta, momentum_up, momentum_down])
	_dev_metrics_box.append_text("近回合得分序列：%s" % ", ".join(score_tail))


func _post_turn_settlement(res: Dictionary) -> void:
	_apply_turn_result_to_local_state(res)
	_render_state()
	if not bool(res.get("stage_complete", false)):
		_set_status("回合结算完成。")
		return
	if bool(res.get("stage_complete", false)):
		_set_status("回合结算完成，正在后台处理关卡结算...")
		var stage_fin: Dictionary = await _api.post_json("/v1/card/stage/finish", {"run_id": _state.run_id})
		if stage_fin.get("ok", false):
			_append_log(
				"关卡结算：%s | 关卡分=%s/%s"
				% [
					str(stage_fin.get("stage_result", stage_fin.get("run_status", "unknown"))),
					str(stage_fin.get("stage_score", "-")),
					str(stage_fin.get("target_score", "-")),
				]
			)
			var run_status := str(stage_fin.get("run_status", ""))
			if run_status == "failed" or run_status == "cleared":
				var run_fin: Dictionary = await _api.post_json("/v1/card/run/finish", {"run_id": _state.run_id})
				if run_fin.get("ok", false):
					_append_log(
						"整局结算：%s | 奖励EXP=%s | 总分=%s"
						% [
							str(run_fin.get("status", "unknown")),
							str(run_fin.get("reward_exp", 0)),
							str(run_fin.get("total_score", "-")),
						]
					)
				else:
					_append_log("整局结算失败：%s" % _api_error_text(run_fin))
				await _refresh_meta()
		else:
			_append_log("关卡结算失败：%s" % _api_error_text(stage_fin))
	await _refresh_state(false)
	if _map_mode:
		var map_run_status := _state.run_status()
		if map_run_status == "failed" or map_run_status == "cleared":
			_append_log("地图战斗已结束，返回大地图并回写资源。")
			_emit_map_battle_exit()


func _on_upgrade_selected(upgrade_code: String) -> void:
	if not upgrade_code.is_empty():
		pass
	_set_status("Card V2 已关闭关卡强化。")


func _on_finish_run_pressed() -> void:
	if _state.run_id <= 0:
		return
	var res: Dictionary = await _api.post_json("/v1/card/run/finish", {"run_id": _state.run_id})
	if not res.get("ok", false):
		_append_log("整局结算失败：%s" % _api_error_text(res))
		return
	_append_log("已领取整局奖励：+%s EXP" % str(res.get("reward_exp", 0)))
	await _refresh_meta()
	await _refresh_state(false)


func _emit_map_battle_exit() -> void:
	if not _map_mode or _map_exit_emitted:
		return
	var battle_run_id := int(_state.run_id if _state.run_id > 0 else _map_battle_run_id)
	if battle_run_id <= 0:
		return
	_map_exit_emitted = true
	battle_exit_requested.emit(int(_map_run_id), battle_run_id)


func _on_settings_home_pressed() -> void:
	_toggle_settings(false)
	_reset_local_game_state()
	_set_status("已返回主选单。")
	if standalone_login_mode:
		if _login_panel != null:
			_login_panel.visible = true
		return
	logout_requested.emit(false)


func _on_settings_exit_battle_pressed() -> void:
	_toggle_settings(false)
	if not _map_mode:
		_set_status("当前不在地图战斗模式。")
		return
	if _map_exit_emitted:
		return
	var run_id := int(_state.run_id if _state.run_id > 0 else _map_battle_run_id)
	if run_id <= 0:
		_emit_map_battle_exit()
		return
	_set_status("正在结束战斗并返回住宅...")
	var res: Dictionary = await _api.post_json("/v1/card/run/abort", {"run_id": run_id})
	if not res.get("ok", false):
		_set_status("结束战斗失败：%s" % str(res.get("message", "unknown")))
		_append_log("结束战斗失败：%s" % _api_error_text(res))
		return
	_append_log("已结束本场战斗（测试退出）。")
	await _refresh_state(false)
	_emit_map_battle_exit()


func _on_settings_quit_pressed() -> void:
	_toggle_settings(false)
	get_tree().quit()


func _reset_local_game_state() -> void:
	_update_action_enabled(false)
	_state.clear()
	_dev_turn_metrics.clear()
	_dev_metrics_run_id = 0
	_queue_cards.clear()
	_pending_candidates.clear()
	_last_meta = {}
	_selected_hand_idx = -1
	_selected_queue_idx = -1
	_story_shown_stage_marker = ""
	_story_transitioning = false
	_map_mode = false
	_map_run_id = 0
	_map_battle_run_id = 0
	_map_exit_emitted = false
	_close_story_overlay()
	_clear_queue_drop_marker()
	_render_meta()
	_render_state()
	_render_dev_metrics()


func _unhandled_input(event: InputEvent) -> void:
	if not (event is InputEventKey) or not event.pressed or event.echo:
		return
	if _story_active:
		if event.keycode == KEY_SPACE or event.keycode == KEY_ENTER or event.keycode == KEY_KP_ENTER:
			_on_story_next_pressed()
			accept_event()
			return
		if event.keycode == KEY_ESCAPE:
			_close_story_overlay()
			accept_event()
			return
	if event.keycode != KEY_ESCAPE:
		return
	if _settings_overlay == null:
		return
	_toggle_settings(not _settings_overlay.visible)
	accept_event()


func _on_logout_pressed() -> void:
	_reset_local_game_state()
	_set_status("已退出当前会话。")
	if standalone_login_mode:
		if _login_panel != null:
			_login_panel.visible = true
		return
	logout_requested.emit(true)
