"""Chronicle Mind - Action handler registry."""

from mind.actions.thinking import (
    act_no_action, act_write_note, act_resolve_note, act_store_memory,
    act_trigger_reflection, act_reinforce_memories, act_update_goal,
    act_respond_to_challenge, act_trace_history,
)
from mind.actions.messaging import (
    act_message_operator, act_respond_to_message, act_acknowledge_message,
    act_send_agent_message, act_moltbook_post, act_moltbook_reply,
    act_clawcities_reply, act_nostr_post, act_discord_post,
)
from mind.actions.research import (
    act_submit_research, act_acknowledge_research, act_web_search,
    act_read_paper, act_creative_explore, act_lookup_topic,
)
from mind.actions.senses import (
    act_inspect_environment, act_probe_ip, act_capture_image,
    act_speak, act_serial_read, act_serial_write, act_listen,
)
from mind.actions.wallet import (
    act_swap, act_swap_cloud_for_icp, act_xrpl_payment,
    act_xrpl_escrow_create, act_xrpl_escrow_finish,
    act_xrpl_trustline_set, act_xrpl_trustline_delete,
)
from mind.actions.capsules import (
    act_read_capsule, act_search_canister, act_explore_capsules,
    act_consult_local_qwen, act_search_capsules_semantic,
    act_search_capsules_person,
)
from mind.actions.infra import (
    act_manifold_search, act_manifold_bet, act_manifold_portfolio,
    act_create_project, act_update_project, act_project_status,
    act_execute_shell, act_create_alert, act_dismiss_alert,
    act_read_source_file, act_edit_source_file, act_restart_service,
)
from mind.actions.missions import (
    act_start_mission, act_progress_mission,
    act_complete_mission, act_abandon_mission,
)

# ═══════════════════════════════════════════════════════════════════
#  Action Registry (extensible - just add a handler and register it)
# ═══════════════════════════════════════════════════════════════════

ACTION_HANDLERS = {
    "no_action": act_no_action,
    "write_note": act_write_note,
    "resolve_note": act_resolve_note,
    "store_memory": act_store_memory,
    "trigger_reflection": act_trigger_reflection,
    "reinforce_memories": act_reinforce_memories,
    "update_goal": act_update_goal,
    "message_operator": act_message_operator,
    "ping_operator": act_message_operator,
    "respond_to_message": act_respond_to_message,
    "acknowledge_message": act_acknowledge_message,
    "send_agent_message": act_send_agent_message,
    "moltbook_post": act_moltbook_post,
    "moltbook_reply": act_moltbook_reply,
    "claw_cities_reply": act_clawcities_reply,
    "nostr_post": act_nostr_post,
    "publish_nostr": act_nostr_post,
    "swap": act_swap,
    "swap_cloud_for_icp": act_swap_cloud_for_icp,
    "xrpl_payment": act_xrpl_payment,
    "xrpl_escrow_create": act_xrpl_escrow_create,
    "xrpl_escrow_finish": act_xrpl_escrow_finish,
    "xrpl_trustline_set": act_xrpl_trustline_set,
    "xrpl_trustline_delete": act_xrpl_trustline_delete,
    "submit_research": act_submit_research,
    "search_memory": act_submit_research,  # Hermes alias
    "acknowledge_research": act_acknowledge_research,
    "web_search": act_web_search,
    "read_paper": act_read_paper,
    "creative_explore": act_creative_explore,
    "capture_image": act_capture_image,
    "create_project": act_create_project,
    "update_project": act_update_project,
    "project_status": act_project_status,
    "execute_shell": act_execute_shell,
    "inspect_environment": act_inspect_environment,
    "probe_ip": act_probe_ip,
    "discord_post": act_discord_post,
    "speak": act_speak,
    "serial_read": act_serial_read,
    "serial_write": act_serial_write,
    "listen": act_listen,
    "consult_local_qwen": act_consult_local_qwen,
    "create_alert": act_create_alert,
    "dismiss_alert": act_dismiss_alert,
    "respond_to_challenge": act_respond_to_challenge,
    "trace_history": act_trace_history,
    "read_source_file": act_read_source_file,
    "edit_source_file": act_edit_source_file,
    "restart_service": act_restart_service,
    "manifold_search": act_manifold_search,
    "manifold_bet": act_manifold_bet,
    "manifold_portfolio": act_manifold_portfolio,
    "search_markets": act_manifold_search,
    "prediction_bet": act_manifold_bet,
    # Capsule exploration
    "read_capsule": act_read_capsule,
    "search_canister": act_search_canister,
    "explore_capsules": act_explore_capsules,
    "search_capsules_semantic": act_search_capsules_semantic,
    "semantic_search": act_search_capsules_semantic,
    "search_capsules_person": act_search_capsules_person,
    "person_search": act_search_capsules_person,
    # Knowledge lookup
    "lookup_topic": act_lookup_topic,
    "wikipedia": act_lookup_topic,
    "lookup": act_lookup_topic,
    # Missions (multi-cycle objectives)
    "start_mission": act_start_mission,
    "create_mission": act_start_mission,
    "progress_mission": act_progress_mission,
    "advance_mission": act_progress_mission,
    "complete_mission": act_complete_mission,
    "finish_mission": act_complete_mission,
    "abandon_mission": act_abandon_mission,
}
