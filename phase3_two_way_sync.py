# phase3_two_way_sync.py
import logging
import json
import re
import os
from datetime import datetime, timezone
import common_utils
from pyairtable import Api as PyAirtableApi

# --- COMMENTS MANAGEMENT ---
 
def extract_sync_id(comment_body, prefix):
    """Extracts a sync ID like [Prefix: 12345] from a comment body."""
    if not comment_body: return None
    match = re.search(rf"\[{re.escape(prefix)}:(\S+)\]", comment_body)
    return match.group(1) if match else None

def sync_native_comments(jira_client, airtable_table, jira_key, airtable_id):
    """
    Performs two-way sync for native comments on a matched pair.
    Returns a list of action strings for the QA report.
    """
    logging.info(f"  [Comment Sync] Starting native comment sync for Jira {jira_key} <-> Airtable {airtable_id}.")
    actions = []
    
    try:
        # 1. Get all comments from both systems
        jira_comments = jira_client.comments(jira_key)
        airtable_comments = airtable_table.comments(airtable_id)

        # 2. Create sets of existing synced IDs for quick lookups
        synced_from_airtable_ids = {extract_sync_id(c.body, "AirtableCommentID") for c in jira_comments}
        synced_from_jira_ids = {extract_sync_id(c.text, "JiraCommentID") for c in airtable_comments}

        # 3. Sync NEW Airtable comments TO Jira
        for a_comment in airtable_comments:
            if a_comment.id not in synced_from_airtable_ids:
                logging.info(f"    Found new Airtable comment {a_comment.id} to sync to Jira.")
                
                # --- THIS IS THE FIX ---
                # Access attributes directly on the Collaborator object, not with .get()
                author_name = "Unknown Airtable User"
                if a_comment.author: # Check if author object exists
                    author_name = a_comment.author.name or a_comment.author.email or "Unknown Airtable User"
                # -------------------------

                timestamp_str = a_comment.created_time.strftime('%Y-%m-%d %H:%M')
                
                jira_comment_body = (
                    f"{author_name} (from Airtable at {timestamp_str}):\n"
                    f"{a_comment.text}\n\n"
                    f"[AirtableCommentID:{a_comment.id}]"
                )
                
                actions.append(f"Jira: Plan to add new comment from Airtable user '{author_name}'.")
                if not common_utils.DRY_RUN:
                    try:
                        jira_client.add_comment(jira_key, jira_comment_body)
                        logging.info(f"      [Live] Jira: Added comment from Airtable {a_comment.id} to {jira_key}.")
                    except Exception as e:
                        logging.error(f"      [Live] Jira: Failed to add comment to {jira_key}: {e}")
                        actions.append(f"Jira: ERROR adding comment from Airtable: {e}")

        # 4. Sync NEW Jira comments TO Airtable
        for j_comment in jira_comments:
            if j_comment.id not in synced_from_jira_ids:
                if not extract_sync_id(j_comment.body, "AirtableCommentID"):
                    logging.info(f"    Found new Jira comment {j_comment.id} to sync to Airtable.")
                    
                    # Jira's author object is also an object, not a dict. Accessing via attribute is safer.
                    author_name = "Unknown Jira User"
                    if hasattr(j_comment, 'author') and j_comment.author:
                        author_name = getattr(j_comment.author, 'displayName', 'Unknown Jira User')

                    timestamp_str = datetime.strptime(j_comment.created, '%Y-%m-%dT%H:%M:%S.%f%z').strftime('%Y-%m-%d %H:%M')
                    
                    airtable_comment_text = (
                    f"{author_name} (from Jira at {timestamp_str}):\n"
                    f"{j_comment.body}\n\n"
                    f"[JiraCommentID:{j_comment.id}]"
                )
                    
                    actions.append(f"Airtable: Plan to add new comment from Jira user '{author_name}'.")
                    if not common_utils.DRY_RUN:
                        try:
                            airtable_table.add_comment(airtable_id, airtable_comment_text)
                            logging.info(f"      [Live] Airtable: Added comment from Jira {j_comment.id} to {airtable_id}.")
                        except Exception as e:
                            logging.error(f"      [Live] Airtable: Failed to add comment to {airtable_id}: {e}")
                            actions.append(f"Airtable: ERROR adding comment from Jira: {e}")

    except Exception as e:
        logging.error(f"  [Comment Sync] Failed for pair {jira_key}/{airtable_id}: {e}", exc_info=True)
        actions.append(f"Comment Sync: FAILED with error: {e}")

    return actions


# --- SPRINT MANAGEMENT HELPER ---
# This helper function can stay within this module as it's specific to Phase 3.
def find_or_create_sprint(jira_client, board_id, sprint_name):
    """
    Finds an existing, active sprint by name. If not found, creates a new one.
    Returns the sprint ID.
    """
    try:
        # --- THIS IS THE CORRECTED LINE ---
        # It uses jira_client.sprints(board_id)
        all_sprints = jira_client.sprints(board_id)
        # ------------------------------------

        for sprint in all_sprints:
            # Sprints can have the same name, but we look for one that isn't closed.
            if sprint.name.lower() == sprint_name.lower() and sprint.state != 'closed':
                logging.debug(f"  [Sprint] Found existing sprint '{sprint.name}' with ID {sprint.id}.")
                return sprint.id
        
        # If no active/future sprint is found with that name, create a new one.
        logging.info(f"  [Sprint] No existing active/future sprint found for '{sprint_name}'. Planning to create.")
        if not common_utils.DRY_RUN:
            # The board_id must be an integer for this call.
            new_sprint = jira_client.create_sprint(name=sprint_name, board_id=board_id)
            logging.info(f"    [Live] Sprint: Created new sprint '{new_sprint.name}' with ID {new_sprint.id}.")
            return new_sprint.id
        else:
            logging.info(f"    [DRY RUN] Would create new sprint '{sprint_name}'.")
            return f"DRYRUN_SPRINT_ID_FOR_{sprint_name}"

    except Exception as e:
        # Provide more context in the error log
        logging.error(f"  [Sprint] Error finding or creating sprint '{sprint_name}' for board ID {board_id}: {e}", exc_info=True)
        return None


# --- MAIN PHASE 3 FUNCTION ---
def run_phase3(jira_client, airtable_api_token, airtable_base_id, airtable_table_name,
               all_jira_issues_map,
               all_airtable_records_map,
               jira_key_to_airtable_id_map
               ):
    """
    Phase 3: Performs synchronization for items that are already linked.
    - Chunk A: Two-Way Status Sync (using timestamps).
    - Chunk B: One-Way Field Sync (Airtable -> Jira).
    - Chunk C: Two-Way Comment Sync.
    - Chunk D: Sprint Management.
    """
    logging.info("--- Executing Phase 3: Two-Way Sync for Matched Items ---")
    actions_log = []
    
    try:
        airtable_table = PyAirtableApi(airtable_api_token).table(airtable_base_id, airtable_table_name)
    except Exception as e:
        logging.critical(f"Phase 3: Failed to initialize Airtable table object: {e}")
        return actions_log

    processed_item_count = 0
    issues_to_sprint = []

    for jira_key, airtable_id in jira_key_to_airtable_id_map.items():
        processed_item_count += 1
        
        jira_issue = all_jira_issues_map.get(jira_key)
        airtable_record = all_airtable_records_map.get(airtable_id)

        if not jira_issue or not airtable_record:
            logging.warning(f"Phase 3: Could not find full data for matched pair Jira {jira_key} / Airtable {airtable_id}. Skipping.")
            continue
            
        logging.info(f"--- Syncing Matched Pair: Jira {jira_key} <-> Airtable {airtable_id} ---")
        action_details = {
            "phase": 3, "type": "Sync", "jira_key": jira_key, "airtable_id": airtable_id,
            "actions": [], "error": None
        }

        airtable_fields = airtable_record.get('fields', {})
        if common_utils.ENABLE_SPRINT_MANAGEMENT:
            issues_to_sprint.append(jira_issue)

        # --- Chunk 3.A: Two-Way Status Sync (with Timestamps) ---
        try:
            airtable_status = airtable_fields.get(common_utils.AIRTABLE_STATUS_FIELD)
            jira_status = jira_issue.fields.status.name
            expected_jira_status = common_utils.STATUS_MAPPING_AIRTABLE_TO_JIRA.get(airtable_status)
            expected_airtable_status = common_utils.STATUS_MAPPING_JIRA_TO_AIRTABLE.get(jira_status)

            is_jira_out_of_sync = expected_jira_status and jira_status.lower() != expected_jira_status.lower()
            is_airtable_out_of_sync = expected_airtable_status and airtable_status.lower() != expected_airtable_status.lower()

            if is_jira_out_of_sync or is_airtable_out_of_sync:
                logging.info(f"  [Status Sync] Mismatch detected! Airtable is '{airtable_status}', Jira is '{jira_status}'.")
                
                jira_updated_dt = datetime.strptime(jira_issue.fields.updated, '%Y-%m-%dT%H:%M:%S.%f%z')
                
                # Use ythe "Last Updated At" field from Airtable
                airtable_updated_str = airtable_fields.get(common_utils.AIRTABLE_LAST_MODIFIED_FIELD_NAME)
                airtable_updated_dt = None
                if airtable_updated_str:
                    try:
                        # Parse the ISO string from Airtable into a naive datetime object first
                        naive_dt = datetime.fromisoformat(airtable_updated_str.replace('Z', ''))
                        # Then, make it offset-aware by attaching the UTC timezone
                        airtable_updated_dt = naive_dt.replace(tzinfo=timezone.utc)
                        # -------------------------
                        logging.debug(f"  Fetched and processed Airtable timestamp: {airtable_updated_dt}")
                    except ValueError:
                        logging.error(f"  Could not parse Airtable timestamp string: '{airtable_updated_str}'")
                
                if not airtable_updated_dt:
                    logging.warning(f"  Airtable timestamp field '{common_utils.AIRTABLE_LAST_MODIFIED_FIELD_NAME}' not found or empty for {airtable_id}. Cannot sync status based on time.")
                    action_details["actions"].append(f"Status Sync: SKIPPED - Missing Airtable timestamp.")
                else:
                    if jira_updated_dt > airtable_updated_dt and is_airtable_out_of_sync:
                        logging.info(f"  Jira is newer. Planning to update Airtable status to '{expected_airtable_status}'.")
                        action_details["actions"].append(f"Airtable: Plan to update status to '{expected_airtable_status}'.")
                        if not common_utils.DRY_RUN:
                            try:
                                airtable_table.update(airtable_id, {common_utils.AIRTABLE_STATUS_FIELD: expected_airtable_status})
                                logging.info(f"    [Live] Airtable: Updated status for {airtable_id}.")
                            except Exception as e:
                                logging.error(f"    [Live] Airtable: Failed to update status for {airtable_id}: {e}")
                                action_details["error"] = f"Airtable Status Update Failed: {e}"
                    
                    elif airtable_updated_dt > jira_updated_dt and is_jira_out_of_sync:
                        logging.info(f"  Airtable is newer. Planning to update Jira status to '{expected_jira_status}'.")
                        action_details["actions"].append(f"Jira: Plan to update status to '{expected_jira_status}'.")
                        if not common_utils.DRY_RUN:
                            transition_id = common_utils.find_jira_transition_id_by_name(jira_client, jira_key, expected_jira_status)
                            if transition_id:
                                try:
                                    jira_client.transition_issue(jira_key, transition_id)
                                except Exception as e:
                                    logging.error(f"    [Live] Jira: Failed to transition {jira_key}: {e}")
                                    action_details["error"] = f"Jira Transition Failed: {e}"
                            else:
                                logging.warning(f"    [Live] Jira: No transition found to '{expected_jira_status}' for {jira_key}.")
            else:
                logging.info(f"  [Status Check] Statuses are in sync.")
                action_details["actions"].append("Status Sync: OK.")
        except Exception as e:
            logging.error(f"Phase 3: Failed during status sync for Jira {jira_key}: {e}", exc_info=True)
            action_details["error"] = f"Status Sync Failed: {e}"

        # --- Chunk 3.B: One-Way Field Sync (Airtable -> Jira) ---
        # Only add to log if there was a meaningful action or error
        if len(action_details["actions"]) > 1 or action_details["error"]:
             actions_log.append(action_details)

# --- Chunk 3.C: Two-Way Comment Sync ---
        if common_utils.ENABLE_COMMENT_SYNC:
            try:
                comment_actions = sync_native_comments(jira_client, airtable_table, jira_key, airtable_id)
                if comment_actions:
                    # Extend the list of actions for the QA report
                    action_details["actions"].extend(comment_actions)
            except Exception as e:
                logging.error(f"Phase 3: Failed during native comment sync for Jira {jira_key}: {e}", exc_info=True)
                action_details["error"] = (action_details.get("error") or "") + f" Comment Sync Failed: {e}"
        else:
            logging.debug("  [Comment Sync] Native comment sync is disabled by feature flag.")

        # --- Only add to log if there was a meaningful action or error ---
        # We check for > 0 now because even a single "OK" is not worth reporting.
        # The new comment actions will add to this list.
        if len(action_details["actions"]) > 0 or action_details["error"]:
             # Filter out simple "OK" messages for a cleaner report
             action_details["actions"] = [action for action in action_details["actions"] if action != "Status Sync: OK."]
             if action_details["actions"] or action_details["error"]: # Check again after filtering
                actions_log.append(action_details)


    # --- Chunk 3.D: Sprint Management (processed after all items are checked) ---
    if common_utils.ENABLE_SPRINT_MANAGEMENT and issues_to_sprint:
        logging.info("--- Starting Sprint Management ---")
    # Use the constant from common_utils instead of os.getenv directly here
    board_id = common_utils.JIRA_BOARD_ID_CONFIG
    if not board_id:
        logging.warning("Sprint Management is enabled, but JIRA_BOARD_ID_FOR_SPRINTS is not set in .env or loaded in common_utils. Skipping.")
    else:
            # Group issues by their "Wxx" ID
            sprint_groups = {}
            for issue in issues_to_sprint:
                wxx_id = common_utils.get_experiment_wxx_txx_id(issue.fields.summary)
                if wxx_id:
                    sprint_name = wxx_id.split('T')[0] # Get the "Wxx" part
                    if sprint_name not in sprint_groups:
                        sprint_groups[sprint_name] = []
                    sprint_groups[sprint_name].append(issue.key)
            
            # For each group, find/create the sprint and add issues
            for sprint_name, issue_keys in sprint_groups.items():
                logging.info(f"  Processing sprint '{sprint_name}' for {len(issue_keys)} issues.")
                sprint_id = find_or_create_sprint(jira_client, int(board_id), sprint_name)
                if sprint_id and not str(sprint_id).startswith("DRYRUN"):
                    try:
                        # Add issues to the sprint. The API can take a list of keys.
                        # Note: Issues can only be in one active sprint. This call might fail if an issue
                        # is already in another active sprint.
                        if not common_utils.DRY_RUN:
                            jira_client.add_issues_to_sprint(sprint_id, issue_keys)
                            logging.info(f"    [Live] Sprint: Added {len(issue_keys)} issues to sprint '{sprint_name}' (ID: {sprint_id}).")
                        else:
                            logging.info(f"    [DRY RUN] Would add {len(issue_keys)} issues to sprint '{sprint_name}' (ID: {sprint_id}).")
                    except Exception as e:
                        logging.error(f"  [Sprint] Failed to add issues to sprint '{sprint_name}': {e}. Some issues might already be in another active sprint.")
                elif common_utils.DRY_RUN:
                     logging.info(f"  [DRY RUN] Would process issues for sprint '{sprint_name}'.")

    logging.info(f"Phase 3 finished. Processed {processed_item_count} matched items.")
    return actions_log