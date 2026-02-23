extends RefCounted
class_name RunState

var run_id: int = 0
var run: Dictionary = {}
var stage: Dictionary = {}


func clear() -> void:
	run_id = 0
	run = {}
	stage = {}


func apply_state_payload(payload: Dictionary) -> void:
	if payload.is_empty():
		return
	run = payload.get("run", {})
	stage = payload.get("stage", {})
	run_id = int(run.get("run_id", run_id))


func run_status() -> String:
	return str(run.get("status", ""))


func stage_status() -> String:
	return str(stage.get("status", ""))


func current_stage_no() -> int:
	return int(run.get("current_stage", 1))


func hand_cards() -> Array:
	return run.get("hand", [])


func pending_upgrades() -> Array:
	return run.get("pending_upgrades", [])


func candidate_pool() -> Array:
	return stage.get("candidate_pool", [])


func pending_discard() -> int:
	var effects: Dictionary = run.get("run_effects", {})
	return int(effects.get("pending_discard", 0))


func rule_version() -> String:
	var effects: Dictionary = run.get("run_effects", {})
	return str(effects.get("rules_version", ""))
