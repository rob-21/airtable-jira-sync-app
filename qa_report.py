# qa_report.py
import logging
import os
import re
from datetime import datetime
from tabulate import tabulate
import common_utils
from collections import defaultdict

def clean_jira_description_for_report(description):
    """Removes Jira's markdown-like syntax for a cleaner report view."""
    if not description: return ""
    # Remove panel macros
    text = re.sub(r"\{panel:.*?\}", "", description, flags=re.DOTALL)
    # Remove color macros
    text = re.sub(r"\{color:.*?\}", "", text, flags=re.DOTALL)
    # Remove h-level headers
    text = re.sub(r"h\d\.", "", text)
    # Remove asterisks (bold) and pluses
    text = text.replace("*", "").replace("+", "")
    # Replace table row separators with newlines and clean up
    text = re.sub(r"\|", "\n", text)
    # Remove excessive blank lines
    text = re.sub(r'\n\s*\n', '\n', text).strip()
    return text

def generate_qa_summary_table(actions_log, all_jira_issues_map, all_airtable_records_map):
    """
    Generates a formatted summary table of all actions and writes it to a log file.
    Includes a totals summary and a detailed action log.
    """
    if not actions_log:
        logging.info("QA Report: No actions were logged to report.")
        return

    # --- Define Detailed Table Headers ---
    detailed_headers = [
        "Sync Mode", "Timestamp", "Sync Status", "Airtable ID", "Airtable Test ID",
        "Airtable Exp ID", "Airtable Full Name", "Airtable Status", "Jira Issue Key",
        "Jira Issue Title", "JIRA Issue Status"
    ]
    if common_utils.QA_REPORT_INCLUDE_DESCRIPTION_FLAG:
        detailed_headers.append("Jira Description")
    detailed_headers.append("Sync Actions / Error")
    
    # --- Prepare Data & Stats ---
    table_data = []
    report_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    sync_mode = "QA (Dry Run)" if common_utils.DRY_RUN else "PROD (Live)"
    status_stats = defaultdict(lambda: {"success": 0, "error": 0})

    for action in actions_log:
        row_dict = {h: "" for h in detailed_headers}
        row_dict["Sync Mode"] = sync_mode
        row_dict["Timestamp"] = report_timestamp
        
        sync_type = "Unknown"
        if action.get("type") == "Jira->Airtable (New)": sync_type = "Jira -> Airtable"
        elif action.get("type") == "Airtable->Jira (New)": sync_type = "Airtable -> Jira"
        elif action.get("type") == "Sync": sync_type = "Synced/Updated"
        row_dict["Sync Status"] = sync_type

        # Populate data from Jira and Airtable objects
        jira_key = action.get("jira_key")
        airtable_id = action.get("airtable_id")
        
        if jira_key:
            row_dict["Jira Issue Key"] = jira_key
            jira_issue_obj = all_jira_issues_map.get(jira_key)
            if jira_issue_obj:
                row_dict["Jira Issue Title"] = jira_issue_obj.fields.summary
                row_dict["JIRA Issue Status"] = jira_issue_obj.fields.status.name
                if common_utils.QA_REPORT_INCLUDE_DESCRIPTION_FLAG:
                    raw_desc = str(getattr(jira_issue_obj.fields, 'description', "") or "")
                    row_dict["Jira Description"] = clean_jira_description_for_report(raw_desc)
        
        if airtable_id:
            row_dict["Airtable ID"] = airtable_id
            airtable_record_obj = all_airtable_records_map.get(airtable_id)
            if airtable_record_obj:
                fields = airtable_record_obj.get('fields', {})
                row_dict["Airtable Test ID"] = str(fields.get(common_utils.AIRTABLE_TEST_ID_FIELD, ""))
                row_dict["Airtable Exp ID"] = str(fields.get(common_utils.AIRTABLE_EXPERIMENT_ID_FIELD, ""))
                row_dict["Airtable Full Name"] = str(fields.get(common_utils.AIRTABLE_HEADLINE_FIELD, ""))
                row_dict["Airtable Status"] = str(fields.get(common_utils.AIRTABLE_STATUS_FIELD, ""))

        # Handle simulated data for new items
        if sync_type == "Airtable -> Jira":
            row_dict["Jira Issue Key"] = action.get("new_jira_key", "(simulated)")
            row_dict["Jira Issue Title"] = action.get("airtable_summary", "")
            airtable_status_for_map = row_dict["Airtable Status"]
            row_dict["JIRA Issue Status"] = common_utils.STATUS_MAPPING_AIRTABLE_TO_JIRA.get(airtable_status_for_map, "(unknown map)")
            if common_utils.QA_REPORT_INCLUDE_DESCRIPTION_FLAG and airtable_record_obj:
                raw_simulated_desc = common_utils.format_full_jira_description(
                    airtable_id, airtable_record_obj.get('fields', {}),
                    common_utils.AIRTABLE_BASE_ID_CONFIG, common_utils.AIRTABLE_TABLE_NAME_CONFIG
                )
                row_dict["Jira Description"] = clean_jira_description_for_report(raw_simulated_desc)
        elif sync_type == "Jira -> Airtable":
            row_dict["Airtable ID"] = action.get("new_airtable_id", "(simulated)")
            row_dict["Airtable Full Name"] = f"{common_utils.JIRA_NOT_EVALUATED_PREFIX_CONST} {action.get('original_summary', '')}"
            row_dict["Airtable Status"] = "Idea: Backlog"
            row_dict["Airtable Test ID"] = "(new)"
            row_dict["Airtable Exp ID"] = "(new)"

        # Update stats and format action summary
        status_key = f"{sync_type} | {row_dict['Airtable Status'] or 'N/A'}"
        if action.get("error"):
            row_dict["Sync Status"] = "Error"
            status_stats[status_key]["error"] += 1
            row_dict["Sync Actions / Error"] = str(action["error"])
        else:
            status_stats[status_key]["success"] += 1
            row_dict["Sync Actions / Error"] = "\n".join(action.get("actions", []))
            
        table_data.append(row_dict)
        
    # --- Format and Write Summary Table (Totals) ---
    summary_headers = ["QA / Prod", "Sync Type", "Airtable Record Status", "Jira Issue Status", "Success", "Error", "Total"]
    summary_rows = []
    total_success, total_error = 0, 0
    
    for status_key, counts in sorted(status_stats.items()):
        if '|' not in status_key:
            logging.warning(f"Skipping malformed status_key in summary report: '{status_key}'")
            continue
        sync_type_from_key, airtable_status_from_key = [s.strip() for s in status_key.split('|')]
        jira_status = common_utils.STATUS_MAPPING_AIRTABLE_TO_JIRA.get(airtable_status_from_key, "N/A")
        if sync_type_from_key == "Jira -> Airtable": jira_status = "N/A"
        
        total = counts["success"] + counts["error"]
        total_success += counts["success"]
        total_error += counts["error"]
        
        summary_rows.append([sync_mode, sync_type_from_key, airtable_status_from_key, jira_status, counts["success"], counts["error"], total])
        
    summary_rows.append(["-"*len(sync_mode), "-"*15, "-"*22, "-"*19, "-"*7, "-"*5, "-"*5])
    summary_rows.append(["GRAND TOTAL", "", "", "", total_success, total_error, (total_success + total_error)])
                         
    formatted_summary_table = tabulate(summary_rows, headers=summary_headers, tablefmt="grid")

    # --- Format and Write Detailed Table ---
    detailed_table_rows = [[row.get(h, "") for h in detailed_headers] for row in table_data]
    max_width = 45 # Keep a reasonable max width
    if common_utils.QA_REPORT_INCLUDE_DESCRIPTION_FLAG: max_width = 60
    formatted_detailed_table = tabulate(detailed_table_rows, headers=detailed_headers, tablefmt="grid", maxcolwidths=max_width)
    
    # --- Write to File and Console (Summary on Top) ---
    report_filename = "sync_summary_report.txt"
    report_filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), report_filename)
    
    try:
        with open(report_filepath, "a", encoding="utf-8") as f:
            f.write(f"\n\n========================= SCRIPT RUN SUMMARY: {report_timestamp} ({sync_mode}) =========================\n\n")
            f.write("--- RUN TOTALS BY STATUS ---\n")
            f.write(formatted_summary_table)
            f.write("\n\n--- DETAILED ACTION LOG ---\n")
            f.write(formatted_detailed_table)
            f.write("\n")
        logging.info(f"Successfully wrote full summary to {report_filepath}")
        
        print("\n--- RUN TOTALS BY STATUS ---")
        print(formatted_summary_table)
        print("\n--- DETAILED ACTION LOG ---")
        print(formatted_detailed_table)
        print(f"\nFull report also saved to: {report_filepath}\n")

    except Exception as e:
        logging.error(f"Failed to write QA summary report to file: {e}")