extends PanelContainer
class_name HandDropZone

signal queue_card_returned(card_index: int, card_id: String)


func _can_drop_data(_position: Vector2, data: Variant) -> bool:
	if typeof(data) != TYPE_DICTIONARY:
		return false
	return str(data.get("type", "")) == "queue_card"


func _drop_data(_position: Vector2, data: Variant) -> void:
	if typeof(data) != TYPE_DICTIONARY:
		return
	queue_card_returned.emit(int(data.get("index", -1)), str(data.get("card_id", "")))
