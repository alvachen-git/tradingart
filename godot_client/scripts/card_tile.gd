extends Button
class_name CardTile

var card_index: int = -1
var card_id: String = ""
var source_zone: String = "hand"
var drag_enabled: bool = true


func setup(index: int, one_card_id: String, zone: String, can_drag: bool = true) -> void:
	card_index = int(index)
	card_id = str(one_card_id)
	source_zone = str(zone)
	drag_enabled = bool(can_drag)


func _get_drag_data(_position: Vector2) -> Variant:
	if not drag_enabled:
		return null
	if source_zone != "hand" and source_zone != "queue":
		return null
	if card_index < 0:
		return null
	var drag_type := "hand_card" if source_zone == "hand" else "queue_card"
	var payload := {
		"type": drag_type,
		"index": card_index,
		"card_id": card_id,
		"source_zone": source_zone,
	}
	var preview := Label.new()
	preview.text = "拖拽: %s" % card_id
	preview.add_theme_color_override("font_color", Color(0.95, 0.97, 1.0, 1.0))
	set_drag_preview(preview)
	return payload
