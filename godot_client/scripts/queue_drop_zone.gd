extends PanelContainer
class_name QueueDropZone

signal hand_card_dropped(card_index: int, card_id: String)
signal queue_card_dropped(card_index: int, card_id: String, global_y: float)
signal drag_hover(global_y: float, drag_type: String)
signal drag_ended()


func _can_drop_data(_position: Vector2, data: Variant) -> bool:
	if typeof(data) != TYPE_DICTIONARY:
		return false
	var drag_type := str(data.get("type", ""))
	var ok := drag_type == "hand_card" or drag_type == "queue_card"
	if ok:
		drag_hover.emit(get_global_mouse_position().y, drag_type)
	return ok


func _drop_data(_position: Vector2, data: Variant) -> void:
	if typeof(data) != TYPE_DICTIONARY:
		return
	var drag_type := str(data.get("type", ""))
	if drag_type == "hand_card":
		hand_card_dropped.emit(int(data.get("index", -1)), str(data.get("card_id", "")))
		return
	if drag_type == "queue_card":
		queue_card_dropped.emit(int(data.get("index", -1)), str(data.get("card_id", "")), get_global_mouse_position().y)


func _notification(what: int) -> void:
	if what == NOTIFICATION_DRAG_END:
		drag_ended.emit()
