# phase1_jira_to_airtable.py
import logging
import json
import re
import common_utils
from pyairtable import Api as PyAirtableApi

def run_phase1(jira_client, airtable_api_token, airtable_base_id, airtable_table_name,
               all_jira_issues_list,
               airtable_id_to_jira_key_map,
               jira_key_to_airtable_id_map
               ):
    """
    Phase 1: Finds new Jira issues that are not in Airtable and processes them.
    - Updates Jira issue title and status.
    - Creates a corresponding record in Airtable.
    - Updates the Jira issue description with the new Airtable Record ID metadata.
    """
    logging.info("--- Executing Phase 1: New Jira Issues to Airtable ---")
    actions_log = []
    
    try:
        airtable_table = PyAirtableApi(airtable_api_token).table(airtable_base_id, airtable_table_name)
    except Exception as e:
        logging.critical(f"Phase 1: Failed to initialize Airtable table object: {e}")
        return actions_log

    processed_issue_count = 0

    for issue in all_jira_issues_list:
        jira_key = issue.key

        # Filter out issues that should not be processed
        if jira_key == common_utils.JIRA_CRO_INTAKE_FORM_KEY:
            logging.debug(f"Phase 1: Skipping intake form template {jira_key}.")
            continue

        if jira_key in jira_key_to_airtable_id_map:
            logging.debug(f"Phase 1: Jira issue {jira_key} is already linked. Skipping.")
            continue
        
        if issue.fields.summary.strip().startswith(common_utils.JIRA_NOT_EVALUATED_PREFIX_CONST):
            logging.warning(f"Phase 1: Jira issue {jira_key} already has prefix but no Airtable link. Skipping for safety.")
            continue

        # If we reach here, this is a new Jira issue to process.
        processed_issue_count += 1
        logging.info(f"--- Processing New Jira Issue: {jira_key} ('{issue.fields.summary}') ---")
        action_details = {
            "phase": 1, "type": "Jira->Airtable (New)", "jira_key": jira_key,
            "original_summary": issue.fields.summary, "actions": [], "new_airtable_id": None, "error": None
        }

        # --- Step A: Update Jira Issue ---
        try:
            # A-1. Prepend prefix to summary
            new_summary = f"{common_utils.JIRA_NOT_EVALUATED_PREFIX_CONST} {issue.fields.summary}"
            action_details["actions"].append(f"Jira: Plan to update summary to '{new_summary}'")
            logging.info(f"  [Plan] Jira {jira_key}: Update summary to '{new_summary}'")
            if not common_utils.DRY_RUN:
                issue.update(summary=new_summary)
                logging.info(f"    [Live] Jira: Updated summary for {jira_key}.")
                action_details["actions"][-1] = f"Jira: Successfully updated summary."

            # A-2. Set Jira Status to "Backlog"
            target_jira_status = common_utils.JIRA_NEW_ISSUE_STATUS
            current_jira_status = issue.fields.status.name
            action_details["actions"].append(f"Jira: Plan to set status to '{target_jira_status}'")
            logging.info(f"  [Plan] Jira {jira_key}: Set status to '{target_jira_status}' (from '{current_jira_status}')")
            if current_jira_status.lower() != target_jira_status.lower():
                if not common_utils.DRY_RUN:
                    transition_id = common_utils.find_jira_transition_id_by_name(jira_client, jira_key, target_jira_status)
                    if transition_id:
                        jira_client.transition_issue(jira_key, transition_id)
                        logging.info(f"    [Live] Jira: Transitioned {jira_key} to '{target_jira_status}'.")
                        action_details["actions"][-1] = f"Jira: Successfully transitioned to '{target_jira_status}'."
                    else:
                        logging.warning(f"    [Live] Jira: No transition found to '{target_jira_status}' for {jira_key}.")
                        action_details["actions"][-1] = f"Jira: WARNING - No transition found to '{target_jira_status}'."
            else:
                action_details["actions"][-1] = f"Jira: Already in target status '{target_jira_status}'."

        except Exception as e:
            logging.error(f"Phase 1: Failed during Jira update for {jira_key}: {e}")
            action_details["error"] = f"Jira Update Failed: {e}"
            actions_log.append(action_details)
            continue

        # --- Step B: Create Airtable Record ---
        idea_name_field = common_utils.AIRTABLE_IDEA_NAME_FIELD
        if not idea_name_field:
            logging.error("Phase 1: AIRTABLE_IDEA_NAME_FIELD is not set in .env. Cannot create Airtable record.")
            action_details["error"] = "Configuration Error: AIRTABLE_IDEA_NAME_FIELD is not set."
            actions_log.append(action_details)
            continue 
        
        
        airtable_fields_to_create = {
            idea_name_field: f"{common_utils.JIRA_NOT_EVALUATED_PREFIX_CONST} {issue.fields.summary}",
            common_utils.AIRTABLE_STATUS_FIELD: "Idea: Backlog",
            common_utils.AIRTABLE_JIRA_KEY_FIELD: jira_key,
            common_utils.AIRTABLE_JIRA_URL_FIELD: issue.permalink()
        }
        
        if issue.fields.description:
            parsed_desc_data = common_utils.parse_jira_description_table_to_airtable_fields(issue.fields.description)
            airtable_fields_to_create.update(parsed_desc_data)
            action_details["actions"].append(f"Airtable: Parsed {len(parsed_desc_data)} fields from Jira description.")
            logging.info(f"  [Plan] Airtable: Parsed {len(parsed_desc_data)} fields from Jira description.")
        
        action_details["actions"].append(f"Airtable: Plan to create record with {len(airtable_fields_to_create)} fields.")
        logging.info(f"  [Plan] Airtable: Create record with {len(airtable_fields_to_create)} fields.")
        if common_utils.SCRIPT_DEBUG_MODE:
            logging.debug(f"    [Debug] Airtable fields to create: {json.dumps(airtable_fields_to_create, indent=2)}")

        created_airtable_record_id = None
        if not common_utils.DRY_RUN:
            try:
                created_record = airtable_table.create(airtable_fields_to_create)
                created_airtable_record_id = created_record['id']
                logging.info(f"    [Live] Airtable: Created record {created_airtable_record_id} for Jira issue {jira_key}.")
                action_details["actions"][-1] = f"Airtable: Successfully created record." # Cleaned up message
                # Update maps for the current run if successful
                jira_key_to_airtable_id_map[jira_key] = created_airtable_record_id
                airtable_id_to_jira_key_map[created_airtable_record_id] = jira_key
            except Exception as e:
                logging.error(f"    [Live] Airtable: Failed to create record for Jira issue {jira_key}: {e}")
                action_details["error"] = f"Airtable Create Failed: {e}"
                actions_log.append(action_details)
                continue
        else: # In Dry Run
            created_airtable_record_id = f"DRYRUN_REC_FOR_{jira_key}"
        
        action_details["new_airtable_id"] = created_airtable_record_id

        # --- Step C: Update Jira Description with New Airtable Metadata ---
        if created_airtable_record_id:
            try:
                current_jira_desc = issue.fields.description if issue.fields.description else ""
                current_jira_desc_cleaned = re.sub(rf"\n\n{re.escape(common_utils.METADATA_BLOCK_HEADER)}.*$", "", current_jira_desc, flags=re.DOTALL).strip()
                
                metadata_lines = [f"\n\n{common_utils.METADATA_BLOCK_HEADER}"]
                metadata_lines.append(f"{common_utils.METADATA_REC_ID_PREFIX}{created_airtable_record_id}")
                
                exp_id_val = common_utils.get_experiment_wxx_txx_id(new_summary)
                if exp_id_val:
                    metadata_lines.append(f"{common_utils.METADATA_EXP_ID_PREFIX}{exp_id_val}")
                
                airtable_url = f"https://airtable.com/{airtable_base_id}/{airtable_table_name}/{created_airtable_record_id}"
                metadata_lines.append(f"{common_utils.METADATA_URL_PREFIX}{airtable_url}")
                
                updated_jira_description = current_jira_desc_cleaned + "\n" + "\n".join(metadata_lines)

                action_details["actions"].append("Jira: Plan to update description with Airtable metadata.")
                logging.info("  [Plan] Jira: Update description with Airtable metadata.")
                if common_utils.SCRIPT_DEBUG_MODE:
                    logging.debug(f"    [Debug] New Jira description metadata block:\n" + "\n".join(metadata_lines))
                
                if not common_utils.DRY_RUN:
                    issue.update(description=updated_jira_description)
                    logging.info(f"    [Live] Jira: Updated description for {jira_key} with Airtable metadata.")
                    action_details["actions"][-1] = "Jira: Successfully updated description with Airtable metadata."
            
            except Exception as e:
                logging.error(f"Phase 1: Failed during Jira description update for {jira_key}: {e}")
                action_details["error"] = f"Jira Description Update Failed: {e}"
        
        actions_log.append(action_details)

    logging.info(f"Phase 1 finished. Processed {processed_issue_count} new Jira issues.")
    return actions_log