extends Resource
class_name CardVisuals

@export var frame_tier_1: Texture2D
@export var frame_tier_2: Texture2D
@export var frame_tier_3: Texture2D
@export var frame_tier_4: Texture2D
@export var frame_selected_overlay: Texture2D

@export var tier_1_border: Color = Color(0.40, 0.56, 0.78, 1.0)
@export var tier_2_border: Color = Color(0.84, 0.60, 0.30, 1.0)
@export var tier_3_border: Color = Color(0.96, 0.84, 0.42, 1.0)
@export var tier_4_border: Color = Color(0.82, 0.58, 0.97, 1.0)

@export var tier_1_glow: Color = Color(0.20, 0.35, 0.50, 1.0)
@export var tier_2_glow: Color = Color(0.38, 0.30, 0.16, 1.0)
@export var tier_3_glow: Color = Color(0.48, 0.40, 0.16, 1.0)
@export var tier_4_glow: Color = Color(0.34, 0.23, 0.47, 1.0)

@export var name_bar_tint_tier_1: Color = Color(0.20, 0.31, 0.45, 0.95)
@export var name_bar_tint_tier_2: Color = Color(0.47, 0.33, 0.16, 0.95)
@export var name_bar_tint_tier_3: Color = Color(0.52, 0.42, 0.12, 0.95)
@export var name_bar_tint_tier_4: Color = Color(0.39, 0.24, 0.48, 0.95)

@export var type_badge_bg_tier_1: Color = Color(0.16, 0.24, 0.33, 0.96)
@export var type_badge_bg_tier_2: Color = Color(0.38, 0.27, 0.16, 0.96)
@export var type_badge_bg_tier_3: Color = Color(0.41, 0.34, 0.13, 0.96)
@export var type_badge_bg_tier_4: Color = Color(0.32, 0.20, 0.42, 0.96)

var _fallback_frames: Dictionary = {}
var _fallback_selected_overlay: Texture2D


func border_for_tier(tier: int) -> Color:
	match int(tier):
		4:
			return tier_4_border
		3:
			return tier_3_border
		2:
			return tier_2_border
		_:
			return tier_1_border


func glow_for_tier(tier: int) -> Color:
	match int(tier):
		4:
			return tier_4_glow
		3:
			return tier_3_glow
		2:
			return tier_2_glow
		_:
			return tier_1_glow


func frame_for_tier(tier: int) -> Texture2D:
	var fallback_color := border_for_tier(tier)
	match int(tier):
		4:
			if frame_tier_4 != null:
				return frame_tier_4
		3:
			if frame_tier_3 != null:
				return frame_tier_3
		2:
			if frame_tier_2 != null:
				return frame_tier_2
		_:
			if frame_tier_1 != null:
				return frame_tier_1
	return _fallback_frame_for_tier(int(tier), fallback_color)


func selected_overlay_texture() -> Texture2D:
	if frame_selected_overlay != null:
		return frame_selected_overlay
	if _fallback_selected_overlay == null:
		var grad := Gradient.new()
		grad.offsets = PackedFloat32Array([0.0, 1.0])
		grad.colors = PackedColorArray([Color(1, 1, 1, 0.22), Color(1, 1, 1, 0.02)])
		var tex := GradientTexture2D.new()
		tex.gradient = grad
		tex.fill = GradientTexture2D.FILL_LINEAR
		tex.fill_from = Vector2(0.0, 0.0)
		tex.fill_to = Vector2(1.0, 1.0)
		tex.width = 256
		tex.height = 356
		_fallback_selected_overlay = tex
	return _fallback_selected_overlay


func name_bar_tint_for_tier(tier: int) -> Color:
	match int(tier):
		4:
			return name_bar_tint_tier_4
		3:
			return name_bar_tint_tier_3
		2:
			return name_bar_tint_tier_2
		_:
			return name_bar_tint_tier_1


func type_badge_color_for_tier(tier: int) -> Color:
	match int(tier):
		4:
			return type_badge_bg_tier_4
		3:
			return type_badge_bg_tier_3
		2:
			return type_badge_bg_tier_2
		_:
			return type_badge_bg_tier_1


func _fallback_frame_for_tier(tier: int, main_color: Color) -> Texture2D:
	var key := str(tier)
	if _fallback_frames.has(key):
		return _fallback_frames[key]
	var grad := Gradient.new()
	grad.offsets = PackedFloat32Array([0.0, 0.52, 1.0])
	grad.colors = PackedColorArray([
		Color(main_color.r * 0.72, main_color.g * 0.72, main_color.b * 0.72, 0.94),
		Color(main_color.r * 0.45, main_color.g * 0.45, main_color.b * 0.45, 0.90),
		Color(0.07, 0.09, 0.13, 0.95),
	])
	var tex := GradientTexture2D.new()
	tex.gradient = grad
	tex.fill = GradientTexture2D.FILL_LINEAR
	tex.fill_from = Vector2(0.5, 0.0)
	tex.fill_to = Vector2(0.5, 1.0)
	tex.width = 256
	tex.height = 356
	_fallback_frames[key] = tex
	return tex
