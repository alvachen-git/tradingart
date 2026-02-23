extends Control
class_name KlineChartView

const RENDER_WINDOW_LIMIT := 60
const RANGE_EPS := 0.0001
const PRICE_ZOOM_MIN := 1.0
const PRICE_ZOOM_MAX := 10.0
const PRICE_ZOOM_STEP := 1.16

var _bars: Array = []
var _visible_count: int = 20

var _bars_layer: Control
var _hover_ui_layer: Control
var _hover_info_panel: PanelContainer
var _hover_info_labels: Array[Label] = []
var _wick_nodes: Array[ColorRect] = []
var _body_nodes: Array[ColorRect] = []
var _vol_nodes: Array[ColorRect] = []

var _display_bars: Array = []
var _revealed_count: int = 0

var _axis_w: float = 58.0
var _plot_rect: Rect2 = Rect2()
var _kline_rect: Rect2 = Rect2()
var _vol_rect: Rect2 = Rect2()
var _slot_w: float = 0.0
var _body_w: float = 0.0
var _wick_w: float = 1.0

var _has_range: bool = false
var _min_low: float = 0.0
var _max_high: float = 0.0
var _view_min_low: float = 0.0
var _view_max_high: float = 0.0
var _max_vol: float = 0.0
var _price_zoom: float = 1.0

var _cached_size: Vector2 = Vector2.ZERO
var _stage_key: String = ""
var _hover_active: bool = false
var _hover_pos: Vector2 = Vector2.ZERO
var _hover_bar_idx: int = -1


func _ready() -> void:
	_ensure_bars_layer()
	_ensure_hover_ui()
	if not resized.is_connected(_on_resized):
		resized.connect(_on_resized)
	if not mouse_exited.is_connected(_on_mouse_exited):
		mouse_exited.connect(_on_mouse_exited)


func _on_resized() -> void:
	_cached_size = Vector2.ZERO
	if _display_bars.is_empty():
		queue_redraw()
		return
	# Resize changes slot width and y mapping; rebuild once to keep geometry stable.
	var range_info := _compute_range(_display_bars)
	if bool(range_info.get("ok", false)):
		_min_low = float(range_info.get("min_low", _min_low))
		_max_high = float(range_info.get("max_high", _max_high))
		_max_vol = float(range_info.get("max_vol", _max_vol))
		_has_range = true
		_refresh_view_range()
	_apply_layout(_display_bars.size())
	_rebuild_all_bar_nodes(_display_bars)
	_cached_size = size
	_refresh_hover_state()
	queue_redraw()


func set_stage_data(stage: Dictionary) -> void:
	_ensure_bars_layer()
	_ensure_hover_ui()
	_bars = stage.get("bars", [])
	_visible_count = max(0, int(stage.get("visible_end", 20)))
	var next_stage_key := _build_stage_key(stage, _bars)
	var stage_changed := next_stage_key != _stage_key
	if stage_changed:
		_stage_key = next_stage_key
		_has_range = false
		_min_low = 0.0
		_max_high = 0.0
		_view_min_low = 0.0
		_view_max_high = 0.0
		_max_vol = 0.0
		_price_zoom = 1.0
		_revealed_count = 0
		_display_bars = []

	var prev_revealed: int = _revealed_count
	var prev_display_size: int = _display_bars.size()
	var new_revealed: int = min(_bars.size(), max(1, _visible_count)) if not _bars.is_empty() else 0
	var visible: Array = _visible_bars()
	_display_bars = visible
	_revealed_count = new_revealed

	if visible.is_empty():
		_clear_bar_nodes()
		_has_range = false
		_clear_hover()
		queue_redraw()
		return

	var size_changed: bool = not size.is_equal_approx(_cached_size)
	var range_info: Dictionary = _compute_range(visible)
	if not bool(range_info.get("ok", false)):
		_clear_bar_nodes()
		_has_range = false
		queue_redraw()
		return
	var prev_min: float = _min_low
	var prev_max: float = _max_high
	var prev_max_vol: float = _max_vol
	var had_range: bool = _has_range
	_min_low = float(range_info.get("min_low", 0.0))
	_max_high = float(range_info.get("max_high", 0.0))
	_max_vol = float(range_info.get("max_vol", 0.0))
	_has_range = true
	_refresh_view_range()
	var range_changed: bool = (not had_range) \
		or (not is_equal_approx(prev_min, _min_low)) \
		or (not is_equal_approx(prev_max, _max_high)) \
		or (not is_equal_approx(prev_max_vol, _max_vol))
	_apply_layout(visible.size())

	var delta_revealed: int = max(0, new_revealed - prev_revealed)
	var can_incremental: bool = (not size_changed) \
		and (not range_changed) \
		and prev_display_size == visible.size() \
		and visible.size() >= RENDER_WINDOW_LIMIT \
		and delta_revealed > 0 \
		and delta_revealed < visible.size()

	if can_incremental:
		_incremental_shift_and_append(visible, delta_revealed)
	else:
		_rebuild_all_bar_nodes(visible)

	_cached_size = size
	_refresh_hover_state()
	if size_changed or range_changed or stage_changed:
		queue_redraw()


func clear_data() -> void:
	_bars = []
	_visible_count = 0
	_display_bars = []
	_revealed_count = 0
	_has_range = false
	_stage_key = ""
	_min_low = 0.0
	_max_high = 0.0
	_view_min_low = 0.0
	_view_max_high = 0.0
	_max_vol = 0.0
	_price_zoom = 1.0
	_clear_hover()
	_clear_bar_nodes()
	queue_redraw()


func _gui_input(event: InputEvent) -> void:
	if event is InputEventMouseButton:
		if not _has_range:
			return
		var mouse_event := event as InputEventMouseButton
		if not mouse_event.pressed:
			return
		if mouse_event.button_index == MOUSE_BUTTON_WHEEL_UP:
			_adjust_price_zoom(true)
			accept_event()
		elif mouse_event.button_index == MOUSE_BUTTON_WHEEL_DOWN:
			_adjust_price_zoom(false)
			accept_event()
	elif event is InputEventMouseMotion:
		var motion := event as InputEventMouseMotion
		_update_hover_from_pos(motion.position)


func _draw() -> void:
	var bg := Color(0.03, 0.06, 0.12, 1.0)
	draw_rect(Rect2(Vector2.ZERO, size), bg, true)

	var count: int = max(1, _display_bars.size())
	_apply_layout(count)

	draw_rect(_plot_rect, Color(0.07, 0.12, 0.20, 1.0), true)
	draw_rect(_plot_rect, Color(0.18, 0.26, 0.40, 1.0), false, 1.0)
	draw_rect(
		Rect2(_plot_rect.position, Vector2(_axis_w, _plot_rect.size.y)),
		Color(0.05, 0.10, 0.17, 1.0),
		true
	)
	draw_line(
		Vector2(_kline_rect.position.x, _plot_rect.position.y),
		Vector2(_kline_rect.position.x, _plot_rect.end.y),
		Color(0.20, 0.30, 0.45, 0.8),
		1.0
	)

	draw_rect(_kline_rect, Color(0.02, 0.08, 0.15, 1.0), true)
	draw_rect(_kline_rect, Color(0.20, 0.32, 0.45, 1.0), false, 1.0)
	draw_rect(_vol_rect, Color(0.02, 0.07, 0.13, 1.0), true)
	draw_rect(_vol_rect, Color(0.20, 0.30, 0.42, 1.0), false, 1.0)

	var sep_y := _kline_rect.end.y + 7.0
	draw_line(
		Vector2(_kline_rect.position.x, sep_y),
		Vector2(_kline_rect.end.x, sep_y),
		Color(0.22, 0.32, 0.48, 0.55),
		1.0
	)

	_draw_price_grid(_kline_rect)
	_draw_volume_grid(_vol_rect)
	_draw_section_labels(_kline_rect, _vol_rect)
	if _has_range:
		_draw_price_axis_labels(_plot_rect.position.x, _axis_w, _kline_rect, _view_min_low, _view_max_high)
	_draw_hover_overlay()


func _apply_layout(count: int) -> void:
	var pad: float = 10.0
	_plot_rect = Rect2(
		pad,
		pad,
		max(24.0, size.x - pad * 2.0),
		max(24.0, size.y - pad * 2.0)
	)
	var chart_rect: Rect2 = Rect2(
		_plot_rect.position + Vector2(_axis_w, 0.0),
		Vector2(max(24.0, _plot_rect.size.x - _axis_w), _plot_rect.size.y)
	)
	var section_gap: float = 14.0
	var kline_h: float = chart_rect.size.y * 0.70
	var vol_h: float = chart_rect.size.y - kline_h - section_gap
	if vol_h < 52.0:
		vol_h = 52.0
		kline_h = chart_rect.size.y - vol_h - section_gap
	_kline_rect = Rect2(chart_rect.position, Vector2(chart_rect.size.x, kline_h))
	_vol_rect = Rect2(
		chart_rect.position + Vector2(0.0, kline_h + section_gap),
		Vector2(chart_rect.size.x, vol_h)
	)
	var n: int = max(1, count)
	_slot_w = _kline_rect.size.x / float(n)
	_body_w = clampf(_slot_w * 0.62, 3.0, 12.0)
	_wick_w = maxf(1.0, _body_w * 0.18)


func _compute_range(visible: Array) -> Dictionary:
	if visible.is_empty():
		return {"ok": false}
	var min_low: float = INF
	var max_high: float = -INF
	var max_vol: float = 0.0
	for b in visible:
		var low: float = _f(b.get("low", 0.0))
		var high: float = _f(b.get("high", 0.0))
		var vol: float = _f(b.get("volume", 0.0))
		min_low = min(min_low, low)
		max_high = max(max_high, high)
		max_vol = max(max_vol, vol)
	if not is_finite(min_low) or not is_finite(max_high) or max_high <= min_low:
		return {"ok": false}
	var price_padding: float = (max_high - min_low) * 0.05
	return {
		"ok": true,
		"min_low": min_low - price_padding,
		"max_high": max_high + price_padding,
		"max_vol": max_vol,
	}


func _ensure_bars_layer() -> void:
	if _bars_layer != null and is_instance_valid(_bars_layer):
		return
	_bars_layer = Control.new()
	_bars_layer.set_anchors_preset(Control.PRESET_FULL_RECT)
	_bars_layer.mouse_filter = Control.MOUSE_FILTER_IGNORE
	add_child(_bars_layer)
	move_child(_bars_layer, get_child_count() - 1)


func _ensure_hover_ui() -> void:
	if _hover_ui_layer == null or not is_instance_valid(_hover_ui_layer):
		_hover_ui_layer = Control.new()
		_hover_ui_layer.set_anchors_preset(Control.PRESET_FULL_RECT)
		_hover_ui_layer.mouse_filter = Control.MOUSE_FILTER_IGNORE
		add_child(_hover_ui_layer)
		move_child(_hover_ui_layer, get_child_count() - 1)
	if _hover_info_panel != null and is_instance_valid(_hover_info_panel):
		return
	_hover_info_panel = PanelContainer.new()
	_hover_info_panel.mouse_filter = Control.MOUSE_FILTER_IGNORE
	_hover_info_panel.visible = false
	var style_bg := StyleBoxFlat.new()
	style_bg.bg_color = Color(0.03, 0.08, 0.14, 1.0)
	style_bg.border_width_left = 1
	style_bg.border_width_top = 1
	style_bg.border_width_right = 1
	style_bg.border_width_bottom = 1
	style_bg.border_color = Color(0.46, 0.66, 0.88, 1.0)
	_hover_info_panel.add_theme_stylebox_override("panel", style_bg)
	_hover_info_panel.custom_minimum_size = Vector2(146.0, 0.0)
	_hover_ui_layer.add_child(_hover_info_panel)
	var box := VBoxContainer.new()
	box.add_theme_constant_override("separation", 0)
	_hover_info_panel.add_child(box)
	_hover_info_labels.clear()
	for _i in range(6):
		var lbl := Label.new()
		lbl.add_theme_font_size_override("font_size", 11)
		lbl.add_theme_color_override("font_color", Color(0.80, 0.90, 0.99, 0.96))
		lbl.custom_minimum_size = Vector2(138.0, 14.0)
		box.add_child(lbl)
		_hover_info_labels.append(lbl)


func _clear_bar_nodes() -> void:
	for one in _wick_nodes:
		if one != null and is_instance_valid(one):
			one.queue_free()
	for one in _body_nodes:
		if one != null and is_instance_valid(one):
			one.queue_free()
	for one in _vol_nodes:
		if one != null and is_instance_valid(one):
			one.queue_free()
	_wick_nodes.clear()
	_body_nodes.clear()
	_vol_nodes.clear()


func _ensure_node_capacity(count: int) -> void:
	_ensure_bars_layer()
	while _wick_nodes.size() < count:
		var wick := ColorRect.new()
		wick.mouse_filter = Control.MOUSE_FILTER_IGNORE
		wick.visible = true
		_bars_layer.add_child(wick)
		_wick_nodes.append(wick)
	while _body_nodes.size() < count:
		var body := ColorRect.new()
		body.mouse_filter = Control.MOUSE_FILTER_IGNORE
		body.visible = true
		_bars_layer.add_child(body)
		_body_nodes.append(body)
	while _vol_nodes.size() < count:
		var vol := ColorRect.new()
		vol.mouse_filter = Control.MOUSE_FILTER_IGNORE
		vol.visible = true
		_bars_layer.add_child(vol)
		_vol_nodes.append(vol)
	while _wick_nodes.size() > count:
		var w: ColorRect = _wick_nodes.pop_back()
		if w != null and is_instance_valid(w):
			w.queue_free()
	while _body_nodes.size() > count:
		var b: ColorRect = _body_nodes.pop_back()
		if b != null and is_instance_valid(b):
			b.queue_free()
	while _vol_nodes.size() > count:
		var v: ColorRect = _vol_nodes.pop_back()
		if v != null and is_instance_valid(v):
			v.queue_free()


func _rebuild_all_bar_nodes(visible: Array) -> void:
	var count: int = visible.size()
	_ensure_node_capacity(count)
	for i in range(count):
		_update_bar_node(i, visible[i])


func _incremental_shift_and_append(visible: Array, delta: int) -> void:
	var count: int = visible.size()
	if delta <= 0 or delta >= count:
		_rebuild_all_bar_nodes(visible)
		return
	var shift_x: float = _slot_w * float(delta)
	for w in _wick_nodes:
		w.position.x -= shift_x
	for b in _body_nodes:
		b.position.x -= shift_x
	for v in _vol_nodes:
		v.position.x -= shift_x
	for _i in range(delta):
		var rw: ColorRect = _wick_nodes.pop_front()
		var rb: ColorRect = _body_nodes.pop_front()
		var rv: ColorRect = _vol_nodes.pop_front()
		_wick_nodes.append(rw)
		_body_nodes.append(rb)
		_vol_nodes.append(rv)
	for idx in range(count - delta, count):
		_update_bar_node(idx, visible[idx])


func _update_bar_node(index: int, bar: Dictionary) -> void:
	if index < 0 or index >= _body_nodes.size() or not _has_range:
		return
	var open := _f(bar.get("open", 0.0))
	var high := _f(bar.get("high", 0.0))
	var low := _f(bar.get("low", 0.0))
	var close := _f(bar.get("close", 0.0))
	var vol := _f(bar.get("volume", 0.0))
	var red := close >= open
	var color := Color(0.95, 0.33, 0.46, 1.0) if red else Color(0.18, 0.80, 0.54, 1.0)
	var vol_color := Color(color.r, color.g, color.b, 0.86)

	var cx := _kline_rect.position.x + _slot_w * float(index) + _slot_w * 0.5
	var y_high := _price_to_y(high, _view_min_low, _view_max_high, _kline_rect)
	var y_low := _price_to_y(low, _view_min_low, _view_max_high, _kline_rect)
	var y_open := _price_to_y(open, _view_min_low, _view_max_high, _kline_rect)
	var y_close := _price_to_y(close, _view_min_low, _view_max_high, _kline_rect)

	var wick := _wick_nodes[index]
	wick.position = Vector2(cx - _wick_w * 0.5, y_high)
	wick.size = Vector2(_wick_w, maxf(1.0, y_low - y_high))
	wick.color = color

	var top := minf(y_open, y_close)
	var bottom := maxf(y_open, y_close)
	var body := _body_nodes[index]
	body.position = Vector2(cx - _body_w * 0.5, top)
	body.size = Vector2(_body_w, maxf(1.0, bottom - top))
	body.color = color

	var vol_node := _vol_nodes[index]
	if _max_vol > 0.0:
		var vh := (vol / _max_vol) * (_vol_rect.size.y - 4.0)
		vol_node.position = Vector2(cx - _body_w * 0.5, _vol_rect.position.y + _vol_rect.size.y - vh)
		vol_node.size = Vector2(_body_w, maxf(0.0, vh))
	else:
		vol_node.position = Vector2(cx - _body_w * 0.5, _vol_rect.end.y)
		vol_node.size = Vector2(_body_w, 0.0)
	vol_node.color = vol_color


func _draw_price_grid(rect: Rect2) -> void:
	var lines: int = 4
	for i in range(1, lines):
		var y := rect.position.y + rect.size.y * (float(i) / float(lines))
		draw_line(
			Vector2(rect.position.x, y),
			Vector2(rect.end.x, y),
			Color(0.18, 0.32, 0.45, 0.35),
			1.0
		)


func _draw_volume_grid(rect: Rect2) -> void:
	var y := rect.position.y + rect.size.y * 0.50
	draw_line(
		Vector2(rect.position.x, y),
		Vector2(rect.end.x, y),
		Color(0.16, 0.28, 0.40, 0.28),
		1.0
	)


func _draw_section_labels(price_rect: Rect2, vol_rect: Rect2) -> void:
	var font: Font = ThemeDB.fallback_font
	if font == null:
		return
	draw_string(
		font,
		price_rect.position + Vector2(8.0, 16.0),
		"PRICE",
		HORIZONTAL_ALIGNMENT_LEFT,
		-1.0,
		12,
		Color(0.68, 0.79, 0.93, 0.86)
	)
	draw_string(
		font,
		vol_rect.position + Vector2(8.0, 16.0),
		"VOLUME",
		HORIZONTAL_ALIGNMENT_LEFT,
		-1.0,
		12,
		Color(0.57, 0.72, 0.89, 0.82)
	)


func _draw_price_axis_labels(axis_left: float, axis_width: float, price_rect: Rect2, min_v: float, max_v: float) -> void:
	var font: Font = ThemeDB.fallback_font
	if font == null:
		return
	var tick_count: int = 5
	var label_color := Color(0.66, 0.80, 0.95, 0.92)
	for i in range(tick_count):
		var ratio := float(i) / float(tick_count - 1)
		var y := price_rect.position.y + price_rect.size.y * ratio
		var price := max_v - (max_v - min_v) * ratio
		draw_line(
			Vector2(price_rect.position.x - 4.0, y),
			Vector2(price_rect.position.x + 2.0, y),
			Color(0.44, 0.61, 0.80, 0.66),
			1.0
		)
		draw_string(
			font,
			Vector2(axis_left + 2.0, y + 4.0),
			_format_price(price),
			HORIZONTAL_ALIGNMENT_RIGHT,
			max(8.0, axis_width - 6.0),
			11,
			label_color
		)


func _draw_hover_overlay() -> void:
	if not _hover_active:
		return
	if _hover_bar_idx < 0 or _hover_bar_idx >= _display_bars.size():
		return
	var bar_v: Variant = _display_bars[_hover_bar_idx]
	if typeof(bar_v) != TYPE_DICTIONARY:
		return
	var bar: Dictionary = bar_v
	var cx := _kline_rect.position.x + _slot_w * float(_hover_bar_idx) + _slot_w * 0.5
	var cross_col := Color(0.92, 0.96, 1.0, 0.52)
	draw_line(
		Vector2(cx, _plot_rect.position.y),
		Vector2(cx, _plot_rect.end.y),
		cross_col,
		1.0
	)
	if _hover_pos.y >= _kline_rect.position.y and _hover_pos.y <= _kline_rect.end.y:
		draw_line(
			Vector2(_kline_rect.position.x, _hover_pos.y),
			Vector2(_kline_rect.end.x, _hover_pos.y),
			Color(0.88, 0.95, 1.0, 0.34),
			1.0
		)
		var hover_price := _y_to_price(_hover_pos.y, _view_min_low, _view_max_high, _kline_rect)
		_draw_hover_price_tag(_hover_pos.y, hover_price)
	_update_hover_info_ui(bar)


func _draw_hover_info_panel(bar: Dictionary) -> void:
	var font: Font = ThemeDB.fallback_font
	if font == null:
		return
	var o := _f(bar.get("open", 0.0))
	var h := _f(bar.get("high", 0.0))
	var l := _f(bar.get("low", 0.0))
	var c := _f(bar.get("close", 0.0))
	var v := _f(bar.get("volume", 0.0))
	var delta_pct := 0.0
	if o > 0.0:
		delta_pct = (c / o - 1.0) * 100.0
	var lines: Array[String] = [
		"O " + _format_price(o),
		"H " + _format_price(h),
		"L " + _format_price(l),
		"C " + _format_price(c),
		"Δ " + ("%.2f%%" % delta_pct),
		"V " + _format_volume(v),
	]
	var font_size := 11
	var line_h := 14.0
	var panel_w := 146.0
	var panel_h := 8.0 + line_h * float(lines.size())
	var margin := 8.0
	var panel_pos := Vector2(_kline_rect.position.x + margin, _kline_rect.position.y + margin)
	var rect := Rect2(panel_pos, Vector2(panel_w, panel_h))
	draw_rect(rect, Color(0.03, 0.08, 0.14, 1.0), true)
	draw_rect(rect, Color(0.46, 0.66, 0.88, 1.0), false, 1.0)
	for i in range(lines.size()):
		var col := Color(0.80, 0.90, 0.99, 0.96)
		if i == 4:
			if delta_pct > 0.0:
				col = Color(0.97, 0.48, 0.58, 1.0)
			elif delta_pct < 0.0:
				col = Color(0.26, 0.86, 0.58, 1.0)
		draw_string(
			font,
			rect.position + Vector2(8.0, 15.0 + float(i) * line_h),
			lines[i],
			HORIZONTAL_ALIGNMENT_LEFT,
			panel_w - 12.0,
			font_size,
			col
		)


func _draw_hover_price_tag(y: float, price: float) -> void:
	var font: Font = ThemeDB.fallback_font
	if font == null:
		return
	var font_size := 11
	var text := _format_price(price)
	var tag_w := _axis_w - 6.0
	var tag_h := 16.0
	var yy := clampf(y - tag_h * 0.5, _kline_rect.position.y, _kline_rect.end.y - tag_h)
	var rect := Rect2(Vector2(_plot_rect.position.x + 2.0, yy), Vector2(tag_w, tag_h))
	draw_rect(rect, Color(0.10, 0.22, 0.34, 0.92), true)
	draw_rect(rect, Color(0.58, 0.79, 0.98, 0.85), false, 1.0)
	draw_string(
		font,
		rect.position + Vector2(3.0, 12.0),
		text,
		HORIZONTAL_ALIGNMENT_RIGHT,
		tag_w - 4.0,
		font_size,
		Color(0.92, 0.97, 1.0, 1.0)
	)


func _format_price(v: float) -> String:
	if abs(v) >= 1000.0:
		return "%.0f" % v
	if abs(v) >= 100.0:
		return "%.1f" % v
	return "%.2f" % v


func _visible_bars() -> Array:
	if _bars.is_empty():
		return []
	var count: int = min(_bars.size(), max(1, _visible_count))
	var revealed: Array = _bars.slice(0, count)
	if revealed.size() <= RENDER_WINDOW_LIMIT:
		return revealed
	var start: int = revealed.size() - RENDER_WINDOW_LIMIT
	return revealed.slice(start, revealed.size())


func _price_to_y(value: float, min_v: float, max_v: float, rect: Rect2) -> float:
	var span: float = maxf(max_v - min_v, RANGE_EPS)
	var ratio: float = (value - min_v) / span
	ratio = clampf(ratio, 0.0, 1.0)
	return rect.position.y + (1.0 - ratio) * rect.size.y


func _y_to_price(y: float, min_v: float, max_v: float, rect: Rect2) -> float:
	var span: float = maxf(max_v - min_v, RANGE_EPS)
	var ratio: float = 1.0 - ((y - rect.position.y) / maxf(rect.size.y, 1.0))
	ratio = clampf(ratio, 0.0, 1.0)
	return min_v + span * ratio


func _refresh_view_range() -> void:
	if not _has_range:
		_view_min_low = 0.0
		_view_max_high = 0.0
		return
	var full_span: float = maxf(_max_high - _min_low, RANGE_EPS)
	var center: float = (_max_high + _min_low) * 0.5
	var zoom_half_span: float = full_span * 0.5 / _price_zoom
	zoom_half_span = maxf(zoom_half_span, RANGE_EPS * 0.5)
	_view_min_low = center - zoom_half_span
	_view_max_high = center + zoom_half_span


func _adjust_price_zoom(zoom_in: bool) -> void:
	var prev_zoom: float = _price_zoom
	if zoom_in:
		_price_zoom = minf(PRICE_ZOOM_MAX, _price_zoom * PRICE_ZOOM_STEP)
	else:
		_price_zoom = maxf(PRICE_ZOOM_MIN, _price_zoom / PRICE_ZOOM_STEP)
	if is_equal_approx(prev_zoom, _price_zoom):
		return
	_refresh_view_range()
	_rebuild_all_bar_nodes(_display_bars)
	_refresh_hover_state()
	queue_redraw()


func _on_mouse_exited() -> void:
	_clear_hover()


func _clear_hover() -> void:
	if not _hover_active and _hover_bar_idx < 0:
		return
	_hover_active = false
	_hover_bar_idx = -1
	_hover_pos = Vector2.ZERO
	_hide_hover_info_ui()
	queue_redraw()


func _refresh_hover_state() -> void:
	if not _hover_active:
		return
	_update_hover_from_pos(_hover_pos)


func _update_hover_from_pos(pos: Vector2) -> void:
	if not _has_range or _display_bars.is_empty():
		_clear_hover()
		return
	var inside_x: bool = pos.x >= _kline_rect.position.x and pos.x <= _kline_rect.end.x
	var inside_y: bool = pos.y >= _plot_rect.position.y and pos.y <= _plot_rect.end.y
	if not inside_x or not inside_y:
		_clear_hover()
		return
	var local_x: float = pos.x - _kline_rect.position.x
	var idx: int = clampi(int(floor(local_x / maxf(_slot_w, 1.0))), 0, _display_bars.size() - 1)
	var changed: bool = (not _hover_active) or idx != _hover_bar_idx or not _hover_pos.is_equal_approx(pos)
	_hover_active = true
	_hover_pos = pos
	_hover_bar_idx = idx
	var bar_v: Variant = _display_bars[idx]
	if typeof(bar_v) == TYPE_DICTIONARY:
		_update_hover_info_ui(bar_v)
	if changed:
		queue_redraw()


func _hide_hover_info_ui() -> void:
	if _hover_info_panel != null and is_instance_valid(_hover_info_panel):
		_hover_info_panel.visible = false


func _update_hover_info_ui(bar: Dictionary) -> void:
	if _hover_info_panel == null or not is_instance_valid(_hover_info_panel):
		return
	if _hover_info_labels.is_empty():
		return
	var o := _f(bar.get("open", 0.0))
	var h := _f(bar.get("high", 0.0))
	var l := _f(bar.get("low", 0.0))
	var c := _f(bar.get("close", 0.0))
	var v := _f(bar.get("volume", 0.0))
	var delta_pct := 0.0
	if o > 0.0:
		delta_pct = (c / o - 1.0) * 100.0
	var lines: Array[String] = [
		"O " + _format_price(o),
		"H " + _format_price(h),
		"L " + _format_price(l),
		"C " + _format_price(c),
		"Δ " + ("%.2f%%" % delta_pct),
		"V " + _format_volume(v),
	]
	var line_count: int = mini(lines.size(), _hover_info_labels.size())
	for i in range(_hover_info_labels.size()):
		var lbl: Label = _hover_info_labels[i]
		if i < line_count:
			lbl.text = lines[i]
			var col := Color(0.80, 0.90, 0.99, 0.96)
			if i == 4:
				if delta_pct > 0.0:
					col = Color(0.97, 0.48, 0.58, 1.0)
				elif delta_pct < 0.0:
					col = Color(0.26, 0.86, 0.58, 1.0)
			lbl.add_theme_color_override("font_color", col)
			lbl.visible = true
		else:
			lbl.visible = false
	var margin := 8.0
	_hover_info_panel.position = Vector2(_kline_rect.position.x + margin, _kline_rect.position.y + margin)
	_hover_info_panel.visible = true


func _f(v: Variant) -> float:
	if typeof(v) == TYPE_INT or typeof(v) == TYPE_FLOAT:
		return float(v)
	return float(str(v))


func _format_volume(v: float) -> String:
	var av: float = abs(v)
	if av >= 100000000.0:
		return "%.1f亿" % (v / 100000000.0)
	if av >= 10000.0:
		return "%.1f万" % (v / 10000.0)
	if av >= 1000.0:
		return "%.1fk" % (v / 1000.0)
	return "%.0f" % v


func _build_stage_key(stage: Dictionary, bars: Array) -> String:
	var stage_no: int = int(stage.get("stage_no", 0))
	var symbol: String = str(stage.get("symbol", ""))
	var bar_count: int = bars.size()
	if bar_count <= 0:
		return "%s|%s|0" % [stage_no, symbol]
	var first: Dictionary = bars[0] if typeof(bars[0]) == TYPE_DICTIONARY else {}
	var last: Dictionary = bars[bar_count - 1] if typeof(bars[bar_count - 1]) == TYPE_DICTIONARY else {}
	var first_date: String = str(first.get("date", ""))
	var last_date: String = str(last.get("date", ""))
	return "%s|%s|%s|%s|%s" % [stage_no, symbol, bar_count, first_date, last_date]
