# phase2_airtable_to_jira.py
import logging
import json
import re
import os
import common_utils
from pyairtable import Api as PyAirtableApi

def run_phase2(jira_client, airtable_api_token, airtable_base_id, airtable_table_name,
               all_airtable_records_list,
               airtable_id_to_jira_key_map,
               jira_key_to_airtable_id_map
               ):
    """
    Phase 2: Finds new Airtable records that need to be created in Jira.
    """
    logging.info("--- Executing Phase 2: New Airtable Records to Jira ---")
    actions_log = []
    
    try:
        airtable_table = PyAirtableApi(airtable_api_token).table(airtable_base_id, airtable_table_name)
    except Exception as e:
        logging.critical(f"Phase 2: Failed to initialize Airtable table object: {e}")
        return actions_log

    processed_airtable_record_count = 0

    for record in all_airtable_records_list:
        record_id = record['id']
        airtable_fields = record.get('fields', {})

        # Filter 1: Skip if already linked
        if record_id in airtable_id_to_jira_key_map:
            logging.debug(f"Phase 2: Airtable record {record_id} is already linked. Skipping.")
            continue

        # Filter 2: Skip if status is not in the creation list
        current_airtable_status = airtable_fields.get(common_utils.AIRTABLE_STATUS_FIELD)
        if not current_airtable_status or current_airtable_status not in common_utils.AIRTABLE_STATUSES_FOR_JIRA_CREATION_LIST:
            logging.debug(f"Phase 2: Airtable record {record_id} has status '{current_airtable_status}', which is not in the creation list. Skipping.")
            continue
        
        processed_airtable_record_count += 1
        headline_from_airtable = airtable_fields.get(common_utils.AIRTABLE_HEADLINE_FIELD)
        
        logging.info(f"Phase 2: Found new Airtable record to process: {record_id} ('{headline_from_airtable}')")
        action_details = {
            "phase": 2, "type": "Airtable->Jira (New)", "airtable_id": record_id,
            "airtable_summary": headline_from_airtable, "actions": [], "new_jira_key": None, "error": None
        }

        # --- Step A: Prepare Jira Issue Data ---
        try:
            # A-1. Jira Summary is a direct copy from Airtable's "Full Name" field.
            if not headline_from_airtable:
                logging.warning(f"Airtable record {record_id} is missing its 'Full Name' ({common_utils.AIRTABLE_HEADLINE_FIELD}). Skipping.")
                action_details["error"] = f"Missing '{common_utils.AIRTABLE_HEADLINE_FIELD}' in Airtable."
                actions_log.append(action_details)
                continue
            
            target_jira_headline = headline_from_airtable

            # A-2. Construct Jira Description (Table + Metadata)
            full_jira_description = common_utils.format_full_jira_description(
                record_id, airtable_fields, common_utils.AIRTABLE_BASE_ID_CONFIG, common_utils.AIRTABLE_TABLE_NAME_CONFIG
            )

            # A-3. Prepare the full issue dictionary
            issue_dict = {
                'project': {'key': common_utils.JIRA_PROJECT_KEY_CONFIG},
                'summary': target_jira_headline,
                'description': full_jira_description,
                'issuetype': {'name': common_utils.JIRA_ISSUE_TYPE_NAME_CONFIG},
                'labels': [common_utils.JIRA_CRO_LABEL]
            }
            action_details["actions"].append(f"Jira: Plan to create issue with summary '{target_jira_headline}'.")
            logging.info(f"  [Plan] Jira: Create issue with summary '{target_jira_headline}'")
            if common_utils.SCRIPT_DEBUG_MODE:
                logging.debug(f"    [Debug] Full Jira issue data to be created: {json.dumps(issue_dict, indent=2)}")

        except Exception as e:
            logging.error(f"Phase 2: Failed during Jira data preparation for Airtable record {record_id}: {e}", exc_info=True)
            action_details["error"] = f"Jira Data Prep Failed: {e}"
            actions_log.append(action_details)
            continue

        # --- Step B: Create Jira Issue (or simulate) ---
        created_jira_issue_key = f"DRYRUN_JIRA_FOR_{record_id}"
        action_details["actions"].append(f"Jira: DRY RUN - Would create issue.")
        logging.info(f"  [Plan] Jira: Create issue.")
        
        if not common_utils.DRY_RUN:
            try:
                new_issue = jira_client.create_issue(fields=issue_dict)
                created_jira_issue_key = new_issue.key
                logging.info(f"    [Live] Jira: Successfully created issue {created_jira_issue_key} for Airtable record {record_id}.")
                action_details["actions"][-1] = f"Jira: Successfully created issue {created_jira_issue_key}."
            except Exception as e:
                logging.error(f"    [Live] Jira: Failed to create issue for Airtable record {record_id}: {e}")
                action_details["error"] = f"Jira Create Failed: {e}"
                actions_log.append(action_details)
                continue
        
        action_details["new_jira_key"] = created_jira_issue_key

        # --- Step C: Set Status on Newly Created Jira Issue ---
        if created_jira_issue_key:
            target_jira_status = common_utils.STATUS_MAPPING_AIRTABLE_TO_JIRA.get(current_airtable_status)
            if target_jira_status:
                action_details["actions"].append(f"Jira: Plan to set status of new issue to '{target_jira_status}'.")
                logging.info(f"  [Plan] Jira: Set status of new issue {created_jira_issue_key} to '{target_jira_status}'.")
                if not common_utils.DRY_RUN:
                    transition_id = common_utils.find_jira_transition_id_by_name(jira_client, created_jira_issue_key, target_jira_status)
                    if transition_id:
                        try:
                            jira_client.transition_issue(created_jira_issue_key, transition_id)
                            logging.info(f"    [Live] Jira: Transitioned {created_jira_issue_key} to '{target_jira_status}'.")
                            action_details["actions"][-1] = f"Jira: Successfully set status to '{target_jira_status}'."
                        except Exception as e:
                            logging.error(f"    [Live] Jira: Failed to transition {created_jira_issue_key}: {e}")
                            action_details["actions"][-1] = f"Jira: ERROR setting status: {e}"
                    else:
                        logging.warning(f"    [Live] Jira: No transition found to '{target_jira_status}' for new issue {created_jira_issue_key}.")
                        action_details["actions"][-1] = f"Jira: WARNING - No transition found to '{target_jira_status}'."
            else:
                logging.warning(f"No Jira status mapping found for Airtable status '{current_airtable_status}'.")
                action_details["actions"].append(f"Jira: WARNING - No status mapping for '{current_airtable_status}'.")

        # --- Step D: Update Airtable Record with Jira Key/URL ---
        if os.getenv('ENABLE_AIRTABLE_UPDATES', 'False').lower() == 'true' and created_jira_issue_key:
            fields_to_update = {
                common_utils.AIRTABLE_JIRA_KEY_FIELD: created_jira_issue_key,
                common_utils.AIRTABLE_JIRA_URL_FIELD: f"{os.getenv('JIRA_SERVER_URL')}/browse/{created_jira_issue_key}"
            }
            action_details["actions"].append(f"Airtable: Plan to update record {record_id} with Jira key.")
            logging.info(f"  [Plan] Airtable: Update record {record_id} with Jira key '{created_jira_issue_key}'.")
            if not common_utils.DRY_RUN:
                try:
                    airtable_table.update(record_id, fields_to_update)
                    logging.info(f"    [Live] Airtable: Successfully updated record {record_id}.")
                    action_details["actions"][-1] = f"Airtable: Successfully updated with Jira key."
                except Exception as e:
                    logging.error(f"    [Live] Airtable: Failed to update record {record_id}: {e}")
                    action_details["error"] = f"Airtable Update Failed: {e}"
        else:
            action_details["actions"].append(f"Airtable: Update is disabled by flag or Jira issue not created.")
            logging.info(f"  [Plan] Airtable: Update for record {record_id} is disabled by ENABLE_AIRTABLE_UPDATES flag or creation failure.")

        actions_log.append(action_details)

    logging.info(f"Phase 2 finished. Processed {processed_airtable_record_count} new Airtable records.")
    return actions_log