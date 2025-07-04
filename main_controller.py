# main_controller.py
import os
import logging # Basic logging for this initial check
import re
from dotenv import load_dotenv, find_dotenv # Ensure find_dotenv is imported

# --- Import the phase-specific run function ---
from phase1_jira_to_airtable import run_phase1
from phase2_airtable_to_jira import run_phase2
from phase3_two_way_sync import run_phase3
from qa_report import generate_qa_summary_table


# --- Initial .env check BEFORE anything else ---
# This block should be the very first executable code.
# Configure basic logging just for this part, will be reconfigured later.
_initial_log_format = '%(asctime)s - %(levelname)s - [INITIAL_CHECK] - %(message)s'
logging.basicConfig(level=logging.INFO, format=_initial_log_format, handlers=[logging.StreamHandler()])

logging.info(f"Script execution started. Python's CWD: {os.getcwd()}")

# Attempt to find and load .env
# find_dotenv() searches for .env in current and parent directories.
# It returns the path to .env if found, or None.
dotenv_path = find_dotenv(usecwd=True) # Prioritize CWD



if dotenv_path and os.path.exists(dotenv_path):
    logging.info(f".env file found by find_dotenv() at: {dotenv_path}")
    if load_dotenv(dotenv_path):
        logging.info("SUCCESS: .env file was explicitly loaded.")
    else:
        logging.warning("WARNING: load_dotenv() called with a path but returned False (might indicate an empty .env or other issue).")
elif os.path.exists(os.path.join(os.getcwd(), '.env')):
    # Fallback if find_dotenv with usecwd=True didn't work but it's directly in CWD
    dotenv_path = os.path.join(os.getcwd(), '.env')
    logging.info(f".env file found directly in CWD: {dotenv_path}")
    if load_dotenv(dotenv_path):
        logging.info("SUCCESS: .env file was explicitly loaded from CWD.")
    else:
        logging.warning("WARNING: load_dotenv() called with CWD path but returned False.")
else:
    logging.error("CRITICAL: .env file NOT found in current working directory or parent directories by find_dotenv().")
    logging.error(f"Please ensure '.env' exists in: {os.getcwd()} or its parent directories if not using usecwd=True.")
    # Optionally, list files in CWD for debugging from Python's perspective:
    try:
        logging.info(f"Files in CWD ({os.getcwd()}): {os.listdir(os.getcwd())}")
    except Exception as e:
        logging.error(f"Could not list files in CWD: {e}")
    # Exit here if .env is absolutely critical and not found
    # exit() # Uncomment if you want to stop if .env is not found

# --- Now print the specific Jira variables to see if they are loaded ---
# These os.getenv calls will now reflect what load_dotenv managed to load.
jira_url_val = os.getenv('JIRA_SERVER_URL')
jira_user_val = os.getenv('JIRA_USERNAME')
jira_token_val = os.getenv('JIRA_API_TOKEN')

logging.info(f"DEBUG CHECK - JIRA_SERVER_URL: '{jira_url_val}' (Type: {type(jira_url_val)})")
logging.info(f"DEBUG CHECK - JIRA_USERNAME: '{jira_user_val}' (Type: {type(jira_user_val)})")
if jira_token_val:
    logging.info(f"DEBUG CHECK - JIRA_API_TOKEN: '********{jira_token_val[-4:] if len(jira_token_val) > 4 else '****'}' (Loaded: True, Length: {len(jira_token_val)})")
else:
    logging.info("DEBUG CHECK - JIRA_API_TOKEN: Not loaded or empty.")

# --- Now, proceed with other imports and the rest of the script ---
# The main logging will be reconfigured after common_utils is imported.
import common_utils # common_utils will also try to load .env, which is fine.
from jira import JIRA
from pyairtable import Api as PyAirtableApi

# --- Main Logging Reconfiguration (as it was before) ---
LOG_FILE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'airtable_jira_sync.log')
logging.basicConfig( # This reconfigures, replacing the initial basicConfig
    level=common_utils.LOG_LEVEL,
    format='%(asctime)s - %(levelname)s - [%(module)s.%(funcName)s:%(lineno)d] - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE_PATH, mode='a'),
        logging.StreamHandler()
    ],
    force=True # Force reconfiguration of logging
)
logging.info("Main logging reconfigured.") # Test message for new config

# ... (rest of the main_controller.py: initialize_clients, fetch_all_data, build_initial_mappings, main function) ...
# Ensure the definitions of initialize_clients, fetch_all_data, etc. are below this point.
# The 'main()' function call should be within the if __name__ == "__main__": block.



# Example of where your previous functions would go:
def initialize_clients():
    # ... (your existing initialize_clients logic, it will use the already loaded env vars) ...
    # It should now find the Jira vars loaded correctly.
    airtable_api_global_client = common_utils.init_global_airtable_client()
    if not airtable_api_global_client:
        logging.critical("Failed to initialize Airtable client (via global common_utils). Exiting.")
        return None, None
    try:
        # These are already loaded by the initial block
        jira_url = os.getenv('JIRA_SERVER_URL')
        jira_user = os.getenv('JIRA_USERNAME')
        jira_token = os.getenv('JIRA_API_TOKEN')

        if not all([jira_url, jira_user, jira_token]):
            logging.critical("Jira connection details are None after initial load. Check .env content and names.")
            return None, None
        
        jira_client_instance = JIRA(server=jira_url, basic_auth=(jira_user, jira_token))
        jira_client_instance.myself()
        logging.info(f"Successfully connected to Jira server: {jira_url}")
        return jira_client_instance, airtable_api_global_client
    except Exception as e:
        logging.critical(f"Failed to connect to Jira in initialize_clients: {e}. Exiting.")
        return None, None

# ... (define fetch_all_data, build_initial_mappings, and main_logic here) ...

def fetch_all_data(jira_client_instance, airtable_api_token_for_direct_call, airtable_base_id_for_direct_call, airtable_table_name_for_direct_call):
    all_jira_issues = []
    all_airtable_records = []
    try:
        jql = f"project = '{common_utils.JIRA_PROJECT_KEY_CONFIG}' AND labels = '{common_utils.JIRA_CRO_LABEL}' ORDER BY created DESC"
        logging.info(f"Fetching Jira issues with JQL: {jql}")
        issues_batch = jira_client_instance.search_issues(jql, startAt=0, maxResults=500)
        all_jira_issues.extend(issues_batch)
        logging.info(f"Fetched {len(all_jira_issues)} Jira issues.")
    except Exception as e:
        logging.error(f"Failed to fetch Jira issues: {e}")
    try:
        if airtable_api_token_for_direct_call and airtable_base_id_for_direct_call and airtable_table_name_for_direct_call:
            temp_airtable_api = PyAirtableApi(airtable_api_token_for_direct_call)
            airtable_main_table = temp_airtable_api.table(airtable_base_id_for_direct_call, airtable_table_name_for_direct_call)
            all_airtable_records = airtable_main_table.all()
            logging.info(f"Fetched {len(all_airtable_records)} Airtable records.")
        else:
            logging.error("Airtable connection details missing for fetching all records.")
    except Exception as e:
        logging.error(f"Failed to fetch Airtable records: {e}")
    return all_jira_issues, all_airtable_records

def build_initial_mappings(all_jira_issues, all_airtable_records):
    jira_key_to_airtable_id = {}
    airtable_id_to_jira_key = {}
    for record in all_airtable_records:
        record_id = record['id']
        airtable_fields = record['fields']
        jira_key_from_airtable = airtable_fields.get(common_utils.AIRTABLE_JIRA_KEY_FIELD)
        if jira_key_from_airtable and re.match(r"^[A-Z][A-Z0-9]+-\d+$", jira_key_from_airtable.strip()):
            jira_key = jira_key_from_airtable.strip()
            if jira_key in jira_key_to_airtable_id and jira_key_to_airtable_id[jira_key] != record_id:
                logging.warning(f"Conflict: Jira key {jira_key} in Airtable record {record_id} "
                                f"is already mapped to Airtable record {jira_key_to_airtable_id[jira_key]}.")
            else:
                jira_key_to_airtable_id[jira_key] = record_id
                airtable_id_to_jira_key[record_id] = jira_key
        elif jira_key_from_airtable:
             # Log that we are ignoring placeholder text
             logging.debug(f"Ignoring invalid or placeholder text in Jira Key field for Airtable record {record_id}: '{jira_key_from_airtable}'")
    for issue in all_jira_issues:
        jira_key = issue.key
        description = getattr(issue.fields, 'description', None)
        if jira_key in jira_key_to_airtable_id: continue
        if description:
            parsed_meta = common_utils.parse_metadata_from_jira_description(description)
            rec_id_from_jira_desc = parsed_meta.get('airtable_record_id')
            if rec_id_from_jira_desc:
                if rec_id_from_jira_desc in airtable_id_to_jira_key and airtable_id_to_jira_key[rec_id_from_jira_desc] != jira_key:
                    logging.warning(f"Jira {jira_key} desc has Airtable ID {rec_id_from_jira_desc}, but that Airtable record is already linked to Jira {airtable_id_to_jira_key[rec_id_from_jira_desc]}.")
                else:
                    jira_key_to_airtable_id[jira_key] = rec_id_from_jira_desc
                    airtable_id_to_jira_key[rec_id_from_jira_desc] = jira_key
                    logging.info(f"Established link: Jira {jira_key} <-> Airtable {rec_id_from_jira_desc} (from Jira description).")
    logging.info(f"Built initial mappings: {len(jira_key_to_airtable_id)} Jira keys mapped, {len(airtable_id_to_jira_key)} Airtable IDs mapped.")
    return jira_key_to_airtable_id, airtable_id_to_jira_key

def main_logic():
    if common_utils.DRY_RUN:
        logging.info("<<<<< SCRIPT IS RUNNING IN DRY RUN MODE - NO LIVE CHANGES WILL BE MADE >>>>>")
    else:
        logging.info("<<<<< SCRIPT IS RUNNING IN LIVE MODE - CHANGES MAY BE MADE >>>>>")

    jira_client_instance, _ = initialize_clients()
    if not jira_client_instance or not common_utils.get_global_airtable_client():
        return

    airtable_token_for_bulk = os.getenv('AIRTABLE_PERSONAL_ACCESS_TOKEN')
    airtable_base_id_for_bulk = os.getenv('AIRTABLE_BASE_ID')
    airtable_main_table_name_for_bulk = os.getenv('AIRTABLE_TABLE_NAME')

    # 1. Fetch all raw data
    all_jira_issues_raw, all_airtable_records_raw = fetch_all_data(
        jira_client_instance, airtable_token_for_bulk, airtable_base_id_for_bulk, airtable_main_table_name_for_bulk
    )
    
    # 2. Create lookup maps for easier access
    
    jira_issues_map_by_key = {issue.key: issue for issue in all_jira_issues_raw}
    airtable_records_map_by_id = {record['id']: record for record in all_airtable_records_raw}
    logging.info(f"Created lookup maps: {len(jira_issues_map_by_key)} Jira issues, {len(airtable_records_map_by_id)} Airtable records.")

    # 3. Build initial mappings (Jira Key <-> Airtable Record ID)
    jira_key_to_airtable_id_map, airtable_id_to_jira_key_map = build_initial_mappings(
        all_jira_issues_raw, all_airtable_records_raw
    )

    overall_actions_log = []

    # --- Execute Phases ---
    if common_utils.ENABLE_PHASE1:
        logging.info("--- Starting Phase 1: New Jira Issues to Airtable ---")
        actions_phase1 = run_phase1(
            jira_client_instance,
            airtable_token_for_bulk, airtable_base_id_for_bulk, airtable_main_table_name_for_bulk,
            all_jira_issues_raw,
            airtable_id_to_jira_key_map,
            jira_key_to_airtable_id_map
        )
        if actions_phase1:
            overall_actions_log.extend(actions_phase1)
            # In a real scenario, you might update the maps here after phase 1 runs
    else:
        logging.info("Phase 1 (Jira to Airtable) is disabled by feature flag.")

    if common_utils.ENABLE_PHASE2:
        logging.info("--- Starting Phase 2: New/Specified Airtable Records to Jira ---")
        actions_phase2 = run_phase2(
            jira_client_instance,
            airtable_token_for_bulk, airtable_base_id_for_bulk, airtable_main_table_name_for_bulk,
            all_airtable_records_raw,
            airtable_id_to_jira_key_map,
            jira_key_to_airtable_id_map
        )
        if actions_phase2:
            overall_actions_log.extend(actions_phase2)
    else:
        logging.info("Phase 2 (Airtable to Jira) is disabled by feature flag.")

    if common_utils.ENABLE_PHASE3:
        logging.info("--- Starting Phase 3: Two-Way Sync for Matched Items ---")
        # Now, the maps being passed here are defined.
        actions_phase3 = run_phase3(
           jira_client_instance,
           airtable_token_for_bulk, 
           airtable_base_id_for_bulk, 
           airtable_main_table_name_for_bulk,
           jira_issues_map_by_key,       # Pass the map of {key: issue_obj}
           airtable_records_map_by_id,   # Pass the map of {id: record_obj}
           jira_key_to_airtable_id_map   # Pass the definitive links
        )
        if actions_phase3:
           overall_actions_log.extend(actions_phase3)
    else:
        logging.info("Phase 3 (Two-Way Sync) is disabled by feature flag.")

    
    # --- QA Report Generation ---
    """ if common_utils.DRY_RUN:
        logging.info("--- Generating QA Dry Run Report ---")
        # This is now an active call to the new module
        generate_qa_summary_table(
            overall_actions_log,
            jira_issues_map_by_key,
            airtable_records_map_by_id
        )
    elif overall_actions_log:
        logging.info("--- Live Run Action Summary ---")
        # You could have a simpler summary for live runs if desired
        for action in overall_actions_log:
            logging.info(f"Action: {action.get('type')} on Jira:{action.get('jira_key')}/Airtable:{action.get('airtable_id')}. Details: {action.get('actions')}")
            if action.get('error'):
                logging.error(f"  -> Error during action: {action.get('error')}")

    logging.info("Main controller finished execution.") """
    if common_utils.DRY_RUN:
        logging.info("--- Generating QA Dry Run Report ---")
        # This call now triggers the new functionality in qa_report.py
        generate_qa_summary_table(
            overall_actions_log,
            jira_issues_map_by_key,
            airtable_records_map_by_id
        )
    elif overall_actions_log:
        # For live runs
        logging.info("--- Live Run Action Summary ---")
        generate_qa_summary_table(
            overall_actions_log,
            jira_issues_map_by_key,
            airtable_records_map_by_id
        )

    logging.info("Main controller finished execution.")   

if __name__ == "__main__":
    # The initial .env check and debug prints for Jira vars are already done above.
    main_logic() # Call the main logic function