# common_utils.py
from dotenv import load_dotenv
import os
import logging
import json
import re
from datetime import datetime, timezone
from pyairtable import Api as PyAirtableApi # For type hinting if needed, actual client in main
from jira import JIRA, JIRAError # For type hinting

# --- INITIAL CONFIGURATION LOADING ---
# This should be done ONCE in main_controller.py, and then values passed or imported.
# For simplicity here, we assume .env is loaded if this module is imported.
# In a larger app, you'd use a config object.
if not load_dotenv():
    print("Warning: .env file not found by common_utils.py. Ensure it's loaded by the main script.")

# --- SCRIPT BEHAVIOR & FEATURE FLAGS ---
DRY_RUN = os.getenv('DRY_RUN', 'False').lower() == 'true'
SCRIPT_DEBUG_MODE = os.getenv('SCRIPT_DEBUG_MODE', 'False').lower() == 'true'
LOG_LEVEL = logging.DEBUG if SCRIPT_DEBUG_MODE else logging.INFO

ENABLE_PHASE1 = os.getenv('ENABLE_PHASE1_JIRA_TO_AIRTABLE', 'False').lower() == 'true'
ENABLE_PHASE2 = os.getenv('ENABLE_PHASE2_AIRTABLE_TO_JIRA', 'False').lower() == 'true'
ENABLE_PHASE3 = os.getenv('ENABLE_PHASE3_TWO_WAY_SYNC', 'False').lower() == 'true'
ENABLE_SPRINT_MANAGEMENT = os.getenv('ENABLE_SPRINT_MANAGEMENT_IN_PHASE3', 'False').lower() == 'true'
ENABLE_COMMENT_SYNC = os.getenv('ENABLE_TWO_WAY_COMMENT_SYNC', 'False').lower() == 'true'


# --- AIRTABLE CONFIG (from .env) ---
AIRTABLE_BASE_ID_CONFIG = os.getenv('AIRTABLE_BASE_ID') # Renamed to avoid conflict with function arg
AIRTABLE_TABLE_NAME_CONFIG = os.getenv('AIRTABLE_TABLE_NAME')
AIRTABLE_TOKEN_CONFIG = os.getenv('AIRTABLE_PERSONAL_ACCESS_TOKEN')

# Main Table Fields
AIRTABLE_HEADLINE_FIELD = os.getenv('AIRTABLE_HEADLINE_FIELD_NAME', "Full Name")
AIRTABLE_STATUS_FIELD = os.getenv('AIRTABLE_STATUS_FIELD_NAME', "Status")
AIRTABLE_EXPERIMENT_ID_FIELD = os.getenv('AIRTABLE_EXPERIMENT_ID_FIELD_NAME')
AIRTABLE_TEST_ID_FIELD = os.getenv('AIRTABLE_TEST_ID_FIELD_NAME')
AIRTABLE_JIRA_KEY_FIELD = os.getenv('AIRTABLE_JIRA_KEY_FIELD_NAME') # Critical for matching
AIRTABLE_JIRA_URL_FIELD = os.getenv('AIRTABLE_JIRA_URL_FIELD_NAME')
# ... (Load ALL AIRTABLE_FIELDNAME_FIELD variable from .env here)
AIRTABLE_OBSERVATION_FIELD = os.getenv('AIRTABLE_OBSERVATION_FIELD')
AIRTABLE_IDEA_FIELD = os.getenv('AIRTABLE_IDEA_FIELD')
AIRTABLE_HYPOTHESIS_FIELD = os.getenv('AIRTABLE_HYPOTHESIS_FIELD')
AIRTABLE_COUNTRY_FIELD = os.getenv('AIRTABLE_COUNTRY_FIELD') # ISO Code field
AIRTABLE_PAGE_TYPE_FIELD = os.getenv('AIRTABLE_PAGE_TYPE_FIELD')
AIRTABLE_PRIMARY_METRIC_FIELD = os.getenv('AIRTABLE_PRIMARY_METRIC_FIELD')
AIRTABLE_SECONDARY_METRICS_FIELD = os.getenv('AIRTABLE_SECONDARY_METRICS_FIELD')
AIRTABLE_PLATFORM_FIELD = os.getenv('AIRTABLE_PLATFORM_FIELD')
AIRTABLE_DEVICE_FIELD = os.getenv('AIRTABLE_DEVICE_FIELD')
AIRTABLE_VAIMO_COMMENTS_FIELD = os.getenv('AIRTABLE_VAIMO_COMMENTS_FIELD')
AIRTABLE_SPONSOR_COMMENTS_FIELD = os.getenv('AIRTABLE_SPONSOR_COMMENTS_FIELD')
AIRTABLE_OTHER_COMMENTS_FIELD = os.getenv('AIRTABLE_OTHER_COMMENTS_FIELD')
AIRTABLE_TODO_NEEDED_FIELD = os.getenv('AIRTABLE_TODO_NEEDED_FIELD')
AIRTABLE_HOW_TO_QA_FIELD = os.getenv('AIRTABLE_HOW_TO_QA_FIELD')
AIRTABLE_TYPE_OF_TEST_FIELD = os.getenv('AIRTABLE_TYPE_OF_TEST_FIELD')
AIRTABLE_PLANNED_START_DATE_FIELD = os.getenv('AIRTABLE_PLANNED_START_DATE_FIELD')
AIRTABLE_ESTIMATED_END_DATE_FIELD = os.getenv('AIRTABLE_ESTIMATED_END_DATE_FIELD')
AIRTABLE_GOAL_FIELD = os.getenv('AIRTABLE_GOAL_FIELD') # For Cluster mapping

# Statuses for processing
AIRTABLE_STATUSES_FOR_JIRA_CREATION_LIST = [s.strip() for s in os.getenv('AIRTABLE_STATUSES_FOR_JIRA_CREATION', "Idea: Evaluated").split(',')]
AIRTABLE_STATUSES_FOR_SYNC_LIST = [s.strip() for s in os.getenv('AIRTABLE_STATUSES_FOR_SYNC', "").split(',')]


# --- JIRA CONFIG (from .env) ---
JIRA_PROJECT_KEY_CONFIG = os.getenv('JIRA_PROJECT_KEY') # Renamed to avoid conflict
JIRA_ISSUE_TYPE_NAME_CONFIG = os.getenv('JIRA_ISSUE_TYPE_NAME')
JIRA_CRO_INTAKE_FORM_KEY = os.getenv('JIRA_CRO_INTAKE_FORM_ISSUE_KEY')
JIRA_CRO_LABEL = os.getenv('JIRA_CRO_PLANNING_LABEL')
JIRA_NEW_ISSUE_STATUS = os.getenv('JIRA_NEW_CRO_ISSUE_INITIAL_STATUS')
JIRA_NOT_EVALUATED_PREFIX_CONST = os.getenv('JIRA_NOT_EVALUATED_PREFIX') # Renamed

# Jira Custom Fields
JIRA_AIRTABLE_ID_CF = os.getenv('JIRA_AIRTABLE_RECORD_ID_CUSTOM_FIELD') # Critical for matching
JIRA_CLUSTER_CF = os.getenv('JIRA_CLUSTER_CUSTOM_FIELD')
JIRA_AFFECTED_COUNTRY_CF = os.getenv('JIRA_AFFECTED_COUNTRY_CUSTOM_FIELD')
JIRA_REQUIRED_DATE_CF = os.getenv('JIRA_REQUIRED_DATE_CUSTOM_FIELD')
JIRA_DUE_DATE_CF = os.getenv('JIRA_DUE_DATE_CUSTOM_FIELD', 'duedate')


# --- LINKED RECORD CONFIGS (from .env, for resolving Airtable linked record IDs) ---
# Users
AIRTABLE_USERS_TABLE = os.getenv('AIRTABLE_USERS_TABLE_NAME_OR_ID')
AIRTABLE_USERS_EMAIL_FIELD = os.getenv('AIRTABLE_USERS_EMAIL_FIELD_NAME')
# Site
AIRTABLE_SITE_IS_LINKED = os.getenv('AIRTABLE_SITE_FIELD_IS_LINKED', 'False').lower() == 'true'
AIRTABLE_SITE_LINKED_TABLE = os.getenv('AIRTABLE_SITE_LINKED_TABLE_NAME_OR_ID')
AIRTABLE_SITE_DISPLAY_FIELD = os.getenv('AIRTABLE_SITE_DISPLAY_FIELD_NAME')
# Page Type
AIRTABLE_PAGE_TYPE_IS_LINKED = os.getenv('AIRTABLE_PAGE_TYPE_FIELD_IS_LINKED', 'False').lower() == 'true'
AIRTABLE_PAGE_TYPE_LINKED_TABLE = os.getenv('AIRTABLE_PAGE_TYPE_LINKED_TABLE_NAME_OR_ID')
AIRTABLE_PAGE_TYPE_DISPLAY_FIELD = os.getenv('AIRTABLE_PAGE_TYPE_DISPLAY_FIELD_NAME')
# Primary Metric
AIRTABLE_PRIMARY_METRIC_IS_LINKED = os.getenv('AIRTABLE_PRIMARY_METRIC_FIELD_IS_LINKED', 'False').lower() == 'true'
AIRTABLE_PRIMARY_METRIC_LINKED_TABLE = os.getenv('AIRTABLE_PRIMARY_METRIC_LINKED_TABLE_NAME_OR_ID')
AIRTABLE_PRIMARY_METRIC_DISPLAY_FIELD = os.getenv('AIRTABLE_PRIMARY_METRIC_DISPLAY_FIELD_NAME')
# Platform
AIRTABLE_PLATFORM_IS_LINKED = os.getenv('AIRTABLE_PLATFORM_FIELD_IS_LINKED', 'False').lower() == 'true'
AIRTABLE_PLATFORM_LINKED_TABLE = os.getenv('AIRTABLE_PLATFORM_LINKED_TABLE_NAME_OR_ID')
AIRTABLE_PLATFORM_DISPLAY_FIELD = os.getenv('AIRTABLE_PLATFORM_DISPLAY_FIELD_NAME')
# Goal (for Cluster)
AIRTABLE_GOAL_IS_LINKED = os.getenv('AIRTABLE_GOAL_FIELD_IS_LINKED', 'False').lower() == 'true'
AIRTABLE_GOAL_LINKED_TABLE = os.getenv('AIRTABLE_GOAL_LINKED_TABLE_NAME_OR_ID')
AIRTABLE_GOAL_DISPLAY_FIELD = os.getenv('AIRTABLE_GOAL_DISPLAY_FIELD_NAME')


# --- MAPPINGS ---
STATUS_MAPPING_AIRTABLE_TO_JIRA = {
    "Idea: Evaluated": "Backlog", "Idea: Planning": "Plan",
    "Deploy: Design/Configure/Dev": "Build", "Build: QA": "Deploy",
    "Monitor": "Test in progress", "Learn: Analysis>Recs>Act": "Test completed",
    "Finalised": "Closed", "Idea: Backlog": "Backlog" # Added for Phase 1
}
STATUS_MAPPING_JIRA_TO_AIRTABLE = {v: k for k, v in STATUS_MAPPING_AIRTABLE_TO_JIRA.items()}
# Ensure specific overrides if Jira status maps to multiple Airtable statuses
STATUS_MAPPING_JIRA_TO_AIRTABLE["Backlog"] = "Idea: Backlog" # If new Jira issues start as "Backlog"

COUNTRY_ISO_TO_NAME = { "BE": "Belgium", "RO": "Romania", "GR": "Greece", "CZ": "Czechia", "RS": "Serbia" }
COUNTRY_NAME_TO_ISO = {v: k for k, v in COUNTRY_ISO_TO_NAME.items()}

# Mapping Airtable field variable names (from .env) to Jira Description Table display headers
# This defines the order and content of the Jira description table.
# The keys are the *variables* holding Airtable field names, not the field names themselves directly.
AIRTABLE_TO_JIRA_DESC_TABLE_MAP = {
    # Variable holding Airtable field name : Jira Description Table Header String
    'AIRTABLE_OBSERVATION_FIELD': "1. Observation", 'AIRTABLE_IDEA_FIELD': "2. Idea",
    'AIRTABLE_HYPOTHESIS_FIELD': "3. Hypothesis", 'AIRTABLE_COUNTRY_FIELD': "4. Country",
    'AIRTABLE_PAGE_TYPE_FIELD': "5. Page type", 'AIRTABLE_PRIMARY_METRIC_FIELD': "6. Primary metric",
    'AIRTABLE_SECONDARY_METRICS_FIELD': "7. Secondary metrics", 'AIRTABLE_PLATFORM_FIELD': "8. Platform",
    'AIRTABLE_DEVICE_FIELD': "9. Device", 'AIRTABLE_VAIMO_COMMENTS_FIELD': "10. Vaimo comments",
    'AIRTABLE_SPONSOR_COMMENTS_FIELD': "11. Sponsor comments", 'AIRTABLE_OTHER_COMMENTS_FIELD': "12. Other comments",
    'AIRTABLE_TODO_NEEDED_FIELD': "13. To do/needed", 'AIRTABLE_HOW_TO_QA_FIELD': "14. How to QA",
    'AIRTABLE_TYPE_OF_TEST_FIELD': "15. Type of Test"
}
JIRA_DESC_TABLE_TO_AIRTABLE_MAP = {v: k for k, v in AIRTABLE_TO_JIRA_DESC_TABLE_MAP.items()}

# --- METADATA BLOCK FORMATTING FOR JIRA DESCRIPTION ---
# Define clear markers for the metadata block to help with parsing.
METADATA_BLOCK_HEADER = "--- Airtable Sync Metadata ---"
METADATA_BLOCK_FOOTER = "--- End Airtable Sync Metadata ---" # Optional, but can help delimit.
METADATA_REC_ID_PREFIX = "Airtable Record ID: "
METADATA_EXP_ID_PREFIX = "Experiment ID: "
METADATA_URL_PREFIX = "Airtable URL: "

# --- GLOBAL AIRTABLE API CLIENT ---
_airtable_api_client_global = None

def init_global_airtable_client():
    global _airtable_api_client_global
    if not _airtable_api_client_global:
        if AIRTABLE_TOKEN_CONFIG: # Use the loaded config name
            _airtable_api_client_global = PyAirtableApi(AIRTABLE_TOKEN_CONFIG)
            logging.info("Global Airtable API client initialized.")
        else:
            logging.error("Failed to initialize global Airtable API client: Token missing from config.")
    return _airtable_api_client_global

def get_global_airtable_client():
    if not _airtable_api_client_global:
        logging.warning("Global Airtable client accessed before initialization.")
        # Optionally try to initialize it here, or rely on main_controller to do it.
        return init_global_airtable_client()
    return _airtable_api_client_global

# --- SHARED HELPER FUNCTIONS (ensure these are complete and correct from previous versions) ---

def get_linked_record_display_values(record_id_list, linked_table_name_or_id, target_field_in_linked_table):
    # Uses get_global_airtable_client() and AIRTABLE_BASE_ID_CONFIG
    # ... (Full function body as provided previously, ensuring it uses the global client and base ID config)
    client = get_global_airtable_client()
    if not client:
        logging.error("Airtable API client not initialized in get_linked_record_display_values.")
        return [str(rid) for rid in record_id_list]
    if not record_id_list: return []
    if not linked_table_name_or_id or not target_field_in_linked_table:
        logging.warning(f"Missing linked table/field config for IDs: {record_id_list}. Returning IDs.")
        return [str(rid) for rid in record_id_list]
    # ... (rest of function)
    display_values = []
    try:
        linked_table = client.table(AIRTABLE_BASE_ID_CONFIG, linked_table_name_or_id)
        for record_id in record_id_list:
            # ... (rest of the loop from previous correct version) ...
            if not isinstance(record_id, str) or not record_id.startswith('rec'):
                logging.warning(f"Invalid record ID format for linked record: {record_id}")
                display_values.append(str(record_id))
                continue
            try:
                linked_record = linked_table.get(record_id)
                if linked_record and 'fields' in linked_record and target_field_in_linked_table in linked_record['fields']:
                    display_values.append(str(linked_record['fields'][target_field_in_linked_table]))
                else:
                    logging.warning(f"Target field '{target_field_in_linked_table}' not found in linked record {record_id} from table {linked_table_name_or_id}. Appending ID.")
                    display_values.append(record_id)
            except Exception as e:
                logging.error(f"Error fetching linked record {record_id} from {linked_table_name_or_id}: {e}")
                display_values.append(record_id)
        return display_values
    except Exception as e:
        logging.error(f"Error accessing linked table {linked_table_name_or_id}: {e}")
        return [str(rid) for rid in record_id_list]


def get_user_emails_from_airtable(user_identifiers_list_or_str):
    # Uses AIRTABLE_USERS_TABLE, AIRTABLE_USERS_EMAIL_FIELD
    # ... (Full function body as provided previously) ...
    if not user_identifiers_list_or_str: return []
    actual_list = []
    # ... (handle string vs list for user_identifiers_list_or_str as before) ...
    if isinstance(user_identifiers_list_or_str, str):
        if ',' in user_identifiers_list_or_str and not user_identifiers_list_or_str.startswith('rec'):
            actual_list = [s.strip() for s in user_identifiers_list_or_str.split(',')]
        elif user_identifiers_list_or_str.startswith('rec') and AIRTABLE_USERS_TABLE and AIRTABLE_USERS_EMAIL_FIELD:
            actual_list = [user_identifiers_list_or_str]
        elif '@' in user_identifiers_list_or_str:
            return [user_identifiers_list_or_str]
        else:
            logging.warning(f"User identifier string '{user_identifiers_list_or_str}' not rec ID, email, or comma-list.")
            return []
    elif isinstance(user_identifiers_list_or_str, list):
        actual_list = user_identifiers_list_or_str
    else:
        logging.warning(f"User identifier is not list or string: {user_identifiers_list_or_str}")
        return []

    record_ids_to_fetch = [item for item in actual_list if isinstance(item, str) and item.startswith('rec')]
    direct_emails = [item for item in actual_list if isinstance(item, str) and '@' in item and not item.startswith('rec')]
    processed_emails = list(direct_emails)

    if record_ids_to_fetch:
        if AIRTABLE_USERS_TABLE and AIRTABLE_USERS_EMAIL_FIELD:
            fetched_emails = get_linked_record_display_values(
                record_ids_to_fetch, AIRTABLE_USERS_TABLE, AIRTABLE_USERS_EMAIL_FIELD
            )
            processed_emails.extend(email for email in fetched_emails if email and '@' in email)
        else:
            logging.warning("User IDs found, but Users Table/Email Field config missing for: " + ", ".join(record_ids_to_fetch))
    return list(set(processed_emails))


def get_jira_account_id(jira_client, email_identifier):
    # ... (Full function body as provided previously) ...
    if not email_identifier or not isinstance(email_identifier, str) or '@' not in email_identifier:
        logging.warning(f"Invalid email for Jira user lookup: {email_identifier}")
        return None
    try:
        users = jira_client.search_users(query=email_identifier, maxResults=5)
        if users:
            for user in users:
                if hasattr(user, 'emailAddress') and user.emailAddress and user.emailAddress.lower() == email_identifier.lower():
                    logging.info(f"Found Jira user '{user.displayName}' (accountId: {user.accountId}) for email '{email_identifier}'.")
                    return user.accountId
            user = users[0]
            logging.info(f"Found Jira user '{user.displayName}' (accountId: {user.accountId}) as best match for '{email_identifier}'.")
            return user.accountId
        else:
            logging.warning(f"Jira user not found for email: {email_identifier}")
            return None
    except JIRAError as e:
        logging.error(f"Jira API error searching for user '{email_identifier}': {e.status_code} - {e.text}")
    except Exception as e:
        logging.error(f"Unexpected error searching for Jira user '{email_identifier}': {e}")
    return None


def format_jira_description_table_from_airtable(airtable_fields):
    # ... (Full function body as provided previously, using the env var constants for field names and linked configs) ...
    # This function now needs to be more dynamic based on the loaded config variables.
    table_rows = ["| Problem or Opportunity | Answers (incl phase) |", "| :--------------------- | :------------------- |"]

    # Helper to get resolved value based on loaded config
    def get_resolved_value_for_desc(airtable_field_var_name):
        airtable_field_name = globals().get(airtable_field_var_name) # Get field name string from the variable
        if not airtable_field_name:
            logging.warning(f"Airtable field name variable '{airtable_field_var_name}' not found in common_utils config.")
            return "[CONFIG_ERROR]"

        raw_value = airtable_fields.get(airtable_field_name)
        if raw_value is None: return ""

        # Determine if this field is linked and its specific linked config
        # This requires a more structured way to access linked field configs if we iterate by AIRTABLE_TO_JIRA_DESC_TABLE_MAP keys
        # For now, let's assume a direct mapping for simplicity in this example,
        # but a better way would be to have a dictionary mapping airtable_field_name to its linked configs.
        is_linked = False
        linked_table_name = None
        display_field_name = None

        if airtable_field_name == AIRTABLE_COUNTRY_FIELD:
            is_linked = AIRTABLE_SITE_IS_LINKED
            linked_table_name = AIRTABLE_SITE_LINKED_TABLE
            display_field_name = AIRTABLE_SITE_DISPLAY_FIELD
        elif airtable_field_name == AIRTABLE_PAGE_TYPE_FIELD:
            is_linked = AIRTABLE_PAGE_TYPE_IS_LINKED
            linked_table_name = AIRTABLE_PAGE_TYPE_LINKED_TABLE
            display_field_name = AIRTABLE_PAGE_TYPE_DISPLAY_FIELD
        # ... Add similar elif for Primary Metric, Platform, Goal ...
        elif airtable_field_name == AIRTABLE_PRIMARY_METRIC_FIELD:
            is_linked = AIRTABLE_PRIMARY_METRIC_IS_LINKED
            linked_table_name = AIRTABLE_PRIMARY_METRIC_LINKED_TABLE
            display_field_name = AIRTABLE_PRIMARY_METRIC_DISPLAY_FIELD
        elif airtable_field_name == AIRTABLE_PLATFORM_FIELD:
            is_linked = AIRTABLE_PLATFORM_IS_LINKED
            linked_table_name = AIRTABLE_PLATFORM_LINKED_TABLE
            display_field_name = AIRTABLE_PLATFORM_DISPLAY_FIELD
        elif airtable_field_name == AIRTABLE_GOAL_FIELD: # If Goal is part of description
            is_linked = AIRTABLE_GOAL_IS_LINKED
            linked_table_name = AIRTABLE_GOAL_LINKED_TABLE
            display_field_name = AIRTABLE_GOAL_DISPLAY_FIELD


        if is_linked and linked_table_name and display_field_name:
            ids_to_resolve = []
            if isinstance(raw_value, list) and all(isinstance(item, str) and item.startswith('rec') for item in raw_value):
                ids_to_resolve = raw_value
            elif isinstance(raw_value, str) and raw_value.startswith('rec'):
                ids_to_resolve = [raw_value]

            if ids_to_resolve:
                resolved_items = get_linked_record_display_values(ids_to_resolve, linked_table_name, display_field_name)
                if airtable_field_name == AIRTABLE_COUNTRY_FIELD: # Special: ISO to Name for display
                    resolved_items = [COUNTRY_ISO_TO_NAME.get(item, item) for item in resolved_items]
                return ', '.join(resolved_items) if resolved_items else f"[Unresolved: {', '.join(ids_to_resolve)}]"
        
        # Fallback for non-linked or if linked resolution fails/not applicable
        return str(raw_value) if not isinstance(raw_value, list) else ', '.join(map(str, raw_value))

    for airtable_field_var, jira_header in AIRTABLE_TO_JIRA_DESC_TABLE_MAP.items():
        value = get_resolved_value_for_desc(airtable_field_var)
        value_sanitized = value.replace("|", "\\|").replace("\n", " ") # Basic sanitization
        table_rows.append(f"| **{jira_header}** | {value_sanitized} |")
    
    # Add Airtable Record ID and Test ID to description
    airtable_rec_id = airtable_fields.get('id') # This is not a field, but the record object's id
    if airtable_rec_id: # This won't be available directly in airtable_fields, need to pass record object
        # This function should ideally take record['id'] as a separate argument if needed
        # For now, assuming it's not directly part of the description table but added elsewhere
        pass

    airtable_test_id_val = airtable_fields.get(AIRTABLE_TEST_ID_FIELD, "")
    if airtable_test_id_val:
        table_rows.append(f"| **Airtable Test ID** | {airtable_test_id_val} |")

    return "\n".join(table_rows)


def parse_jira_description_table_to_airtable_fields(description_string):
    # ... (Full function body as provided previously, ensure it uses JIRA_DESC_TABLE_TO_AIRTABLE_MAP and COUNTRY_NAME_TO_ISO) ...
    parsed_data = {}
    if not description_string: return parsed_data
    lines = description_string.splitlines()
    # This parsing is fragile. It assumes a consistent two-column markdown table.
    # | **Jira Header** | Value |
    for line in lines:
        if not line.strip().startswith("| **"): continue
        parts = [part.strip() for part in line.split('|')]
        if len(parts) >= 4: # e.g. ['', '**Jira Header**', 'Value', '']
            jira_header_text = parts[1].replace("**", "").strip()
            raw_value = parts[2].strip()

            # Find the corresponding Airtable field variable name
            airtable_field_var_name = JIRA_DESC_TABLE_TO_AIRTABLE_MAP.get(jira_header_text)
            if airtable_field_var_name:
                # Get the actual Airtable field name string using the variable name
                airtable_field_name = globals().get(airtable_field_var_name)
                if airtable_field_name:
                    # Reverse transformations
                    if airtable_field_name == AIRTABLE_COUNTRY_FIELD: # Map Country Name back to ISO
                        parsed_data[airtable_field_name] = COUNTRY_NAME_TO_ISO.get(raw_value, raw_value)
                    # Add other reverse transformations if needed (e.g., for multi-select, dates)
                    else:
                        parsed_data[airtable_field_name] = raw_value
                else:
                    logging.warning(f"Jira desc key '{jira_header_text}' mapped to unknown Airtable field var '{airtable_field_var_name}'")
            elif jira_header_text == "Airtable Test ID": # Special case if we add this to desc
                 parsed_data[AIRTABLE_TEST_ID_FIELD] = raw_value # Assuming Test ID is simple text

    logging.debug(f"Parsed Jira description into Airtable fields: {parsed_data}")
    return parsed_data


def find_jira_transition_id_by_name(jira_client, issue_key, target_status_name):
    # ... (Full function body as provided previously) ...
    try:
        transitions = jira_client.transitions(issue_key)
        for t in transitions:
            if t['to'].name.lower() == target_status_name.lower():
                return t['id']
        logging.warning(f"No transition found to target status '{target_status_name}' for issue {issue_key}.")
    except JIRAError as e:
        logging.error(f"Error fetching transitions for {issue_key}: {e}")
    return None


def get_experiment_wxx_txx_id(text_string):
    # ... (Full function body as provided previously) ...
    if not text_string: return None
    match = re.search(r"(W\d+T\d+)", text_string, re.IGNORECASE)
    return match.group(1).upper() if match else None


def construct_jira_headline(exp_id, country_iso, page_type_val, name_val):
    # ... (Full function body as provided previously) ...
    # Name val should be the core idea name, not the full existing headline
    parts = []
    if exp_id: parts.append(exp_id)
    
    # Resolve country_iso if it's a record ID (should be resolved before calling this)
    # For now, assume country_iso is the actual ISO code.
    if country_iso: parts.append(country_iso.upper()) # Ensure ISO is uppercase
    
    if page_type_val: parts.append(page_type_val)
    if name_val: parts.append(name_val)
    
    headline = " - ".join(filter(None, parts)) # Filter out any None or empty strings
    
    # Handle [NOT EVALUATED] prefix separately if needed, or assume it's added/removed by specific logic
    return headline


def format_date_for_jira(date_str):
    # ... (Full function body as provided previously) ...
    if not date_str: return None
    try:
        # Handle Airtable's format 'YYYY-MM-DDTHH:MM:SS.sssZ' or simple 'YYYY-MM-DD'
        if 'T' in date_str:
            dt_obj = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            return dt_obj.strftime('%Y-%m-%d')
        elif len(date_str) == 10 and date_str.count('-') == 2: # Basic YYYY-MM-DD check
            datetime.strptime(date_str, '%Y-%m-%d') # Validate
            return date_str
        else:
            logging.warning(f"Unrecognized date format for Jira conversion: {date_str}")
            return None
    except ValueError:
        logging.warning(f"Invalid date string for Jira conversion: {date_str}")
        return None


def format_date_for_airtable(date_str):
    # ... (Full function body as provided previously) ...
    # Airtable Date field (without time) accepts 'YYYY-MM-DD'
    if not date_str: return None
    try:
        datetime.strptime(date_str, '%Y-%m-%d') # Validate it's YYYY-MM-DD
        return date_str
    except ValueError:
        logging.warning(f"Invalid date format for Airtable: {date_str} (expected YYYY-MM-DD)")
        return None

def format_full_jira_description(airtable_record_id, airtable_fields, airtable_base_id_for_url, airtable_table_id_or_name_for_url):
    """
    Formats the entire Jira description, including the main content table
    and the appended metadata block.
    """
    # 1. Generate the main description table (from existing logic)
    # This function 'format_jira_description_table_from_airtable' needs to be defined
    # or its logic integrated here. Let's assume it's here for now.
    
    # --- Start of format_jira_description_table_from_airtable logic ---
    table_rows = ["| Problem or Opportunity | Answers (incl phase) |", "| :--------------------- | :------------------- |"]
    def get_resolved_value_for_desc(airtable_field_var_name_str):
        airtable_field_name = globals().get(airtable_field_var_name_str)
        if not airtable_field_name:
            logging.warning(f"Airtable field name variable '{airtable_field_var_name_str}' not found in common_utils config.")
            return "[CONFIG_ERROR]"
        raw_value = airtable_fields.get(airtable_field_name)
        if raw_value is None: return ""

        is_linked = False
        linked_table_name = None
        display_field_name = None

        # Dynamically check linked status based on the field name
        # This requires a mapping or a series of if/elifs for each configurable linked field
        if airtable_field_name == AIRTABLE_COUNTRY_FIELD:
            is_linked = AIRTABLE_SITE_IS_LINKED # Using SITE for COUNTRY field linkage
            linked_table_name = AIRTABLE_SITE_LINKED_TABLE
            display_field_name = AIRTABLE_SITE_DISPLAY_FIELD
        elif airtable_field_name == AIRTABLE_PAGE_TYPE_FIELD:
            is_linked = AIRTABLE_PAGE_TYPE_IS_LINKED
            linked_table_name = AIRTABLE_PAGE_TYPE_LINKED_TABLE
            display_field_name = AIRTABLE_PAGE_TYPE_DISPLAY_FIELD
        elif airtable_field_name == AIRTABLE_PRIMARY_METRIC_FIELD:
            is_linked = AIRTABLE_PRIMARY_METRIC_IS_LINKED
            linked_table_name = AIRTABLE_PRIMARY_METRIC_LINKED_TABLE
            display_field_name = AIRTABLE_PRIMARY_METRIC_DISPLAY_FIELD
        elif airtable_field_name == AIRTABLE_PLATFORM_FIELD:
            is_linked = AIRTABLE_PLATFORM_IS_LINKED
            linked_table_name = AIRTABLE_PLATFORM_LINKED_TABLE
            display_field_name = AIRTABLE_PLATFORM_DISPLAY_FIELD
        elif airtable_field_name == AIRTABLE_GOAL_FIELD:
            is_linked = AIRTABLE_GOAL_IS_LINKED
            linked_table_name = AIRTABLE_GOAL_LINKED_TABLE
            display_field_name = AIRTABLE_GOAL_DISPLAY_FIELD
        
        if is_linked and linked_table_name and display_field_name:
            ids_to_resolve = []
            if isinstance(raw_value, list) and all(isinstance(item, str) and item.startswith('rec') for item in raw_value):
                ids_to_resolve = raw_value
            elif isinstance(raw_value, str) and raw_value.startswith('rec'):
                ids_to_resolve = [raw_value]

            if ids_to_resolve:
                resolved_items = get_linked_record_display_values(ids_to_resolve, linked_table_name, display_field_name)
                if airtable_field_name == AIRTABLE_COUNTRY_FIELD:
                    resolved_items = [COUNTRY_ISO_TO_NAME.get(item, item) for item in resolved_items]
                return ', '.join(resolved_items) if resolved_items else f"[Unresolved: {', '.join(ids_to_resolve)}]"
        
        return str(raw_value) if not isinstance(raw_value, list) else ', '.join(map(str, raw_value))

    for airtable_field_var, jira_header in AIRTABLE_TO_JIRA_DESC_TABLE_MAP.items(): # Uses the map from common_utils
        value = get_resolved_value_for_desc(airtable_field_var) # Pass the string name of the global var
        value_sanitized = value.replace("|", "\\|").replace("\n", " ")
        table_rows.append(f"| **{jira_header}** | {value_sanitized} |")
    
    main_description_table = "\n".join(table_rows)
    # --- End of format_jira_description_table_from_airtable logic ---

    # 2. Construct the metadata block
    metadata_lines = [f"\n\n{METADATA_BLOCK_HEADER}"] # Start with a newline for separation

    if airtable_record_id:
        metadata_lines.append(f"{METADATA_REC_ID_PREFIX}{airtable_record_id}")

    experiment_id = airtable_fields.get(AIRTABLE_EXPERIMENT_ID_FIELD)
    if experiment_id: # This now comes from airtable_fields
        metadata_lines.append(f"{METADATA_EXP_ID_PREFIX}{experiment_id}")
    
    # Construct Airtable URL
    # Ensure airtable_base_id_for_url and airtable_table_id_or_name_for_url are the actual IDs
    if airtable_record_id and airtable_base_id_for_url and airtable_table_id_or_name_for_url:
        airtable_url = f"https://airtable.com/{airtable_base_id_for_url}/{airtable_table_id_or_name_for_url}/{airtable_record_id}"
        metadata_lines.append(f"{METADATA_URL_PREFIX}{airtable_url}")

    if len(metadata_lines) > 1: # If any metadata was added
        # metadata_lines.append(METADATA_BLOCK_FOOTER) # Optional footer
        return main_description_table + "\n" + "\n".join(metadata_lines)
    else:
        return main_description_table


def parse_metadata_from_jira_description(description_string):
    """
    Parses the metadata block from the Jira description.
    Returns a dictionary with 'airtable_record_id', 'experiment_id', 'airtable_url'.
    """
    metadata = {
        'airtable_record_id': None,
        'experiment_id': None,
        'airtable_url': None
    }
    if not description_string:
        return metadata

    # Attempt to find the metadata block more robustly
    # This regex looks for the header and captures everything until a potential footer or end of string
    # It's non-greedy for the content part (.*?)
    # metadata_block_match = re.search(rf"{re.escape(METADATA_BLOCK_HEADER)}(.*?)(?:{re.escape(METADATA_BLOCK_FOOTER)}|$)", description_string, re.DOTALL)
    
    # Simpler: search for known prefixes within the whole description if block markers are unreliable
    # This might be safer if users edit the description heavily.
    
    rec_id_match = re.search(rf"^{re.escape(METADATA_REC_ID_PREFIX)}(rec[a-zA-Z0-9]{{14}})$", description_string, re.MULTILINE)
    if rec_id_match:
        metadata['airtable_record_id'] = rec_id_match.group(1)

    exp_id_match = re.search(rf"^{re.escape(METADATA_EXP_ID_PREFIX)}(W\d+T\d+)$", description_string, re.IGNORECASE | re.MULTILINE)
    if exp_id_match:
        metadata['experiment_id'] = exp_id_match.group(1).upper()

    url_match = re.search(rf"^{re.escape(METADATA_URL_PREFIX)}(https://airtable\.com/[^/\s]+/[^/\s]+/\S+)$", description_string, re.MULTILINE)
    if url_match:
        metadata['airtable_url'] = url_match.group(1)
    
    if metadata['airtable_record_id'] or metadata['experiment_id'] or metadata['airtable_url']:
        logging.debug(f"Parsed metadata from Jira description: {metadata}")
    else:
        logging.debug("No Airtable metadata block found or parsed from Jira description.")
        
    return metadata


def construct_jira_headline(exp_id, country_iso, page_type_val, name_val, current_prefix=None):
    """Constructs the standard Jira headline, preserving existing prefix if provided."""
    parts = []
    if exp_id: parts.append(exp_id)
    
    if country_iso: parts.append(country_iso.upper())
    
    if page_type_val: parts.append(page_type_val)
    if name_val: parts.append(name_val) # This should be the core "name" part of the idea
    
    main_headline_part = " - ".join(filter(None, parts))
    
    if current_prefix and current_prefix.strip(): # e.g., "[NOT EVALUATED]"
        return f"{current_prefix.strip()} {main_headline_part}"
    return main_headline_part

# --- END OF common_utils.py ---