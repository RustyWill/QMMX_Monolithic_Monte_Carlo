import sqlite3
import json
from collections import defaultdict

DB_PATH = "qmmx.db"

def analyze_policy_events():
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Query for 'entry' phase skips from policy_events table
        cursor.execute("SELECT ts, phase, action, features_json FROM policy_events WHERE phase=\'entry\' AND action=\'skip\'")
        skip_entries = cursor.fetchall()

        if not skip_entries:
            print("No 'entry' phase skip events found in the policy_events table.")
            return

        print(f"Found {len(skip_entries)} 'entry' phase skip events.\n")

        # Aggregate reasons and their details
        reason_counts = defaultdict(int)
        reason_details = defaultdict(lambda: defaultdict(int))

        for ts, phase, action, features_json_str in skip_entries:
            try:
                extras = json.loads(features_json_str)
                reason = extras.get(\'reason\', \'UNKNOWN_REASON\')
                reason_counts[reason] += 1

                # Collect specific details for each reason
                if reason == \'PRICE_STALE\':
                    last_ts_ms = extras.get(\'last_ts_ms\', \'N/A\')
                    now_ms = extras.get(\'now\', \'N/A\')
                    reason_details[reason][f\'last_ts_ms: {last_ts_ms}, now: {now_ms}\'] += 1
                elif reason == \'COOLDOWN\':
                    cooldown_until_ms = extras.get(\'cooldown_until_ms\', \'N/A\')
                    reason_details[reason][f\'cooldown_until: {cooldown_until_ms}\'] += 1
                elif reason == \'LEVEL_OVERTOUCHED\':
                    level_key = tuple(extras.get(\'level\', [\'N/A\', \'N/A\', \'N/A\']))
                    touch_count = extras.get(\'touch_count\', \'N/A\')
                    reason_details[reason][f\'level: {level_key}, touches: {touch_count}\'] += 1
                elif reason == \'CONF_LOW\' or reason == \'ML_CONF_LOW\':
                    conf = extras.get(\'conf\', \'N/A\')
                    q_min_prob = extras.get(\'Q_MIN_PROB\', \'N/A\')
                    level_price = extras.get(\'level_price\', \'N/A\')
                    proximity_abs = extras.get(\'proximity_abs\', \'N/A\')
                    reason_details[reason][f\'conf: {conf:.2f}, min_prob: {q_min_prob:.2f}, level: {level_price}, prox: {proximity_abs:.2f}\'] += 1
                elif reason == \'TOO_FAR\':
                    level_price = extras.get(\'level_price\', \'N/A\')
                    proximity_abs = extras.get(\'proximity_abs\', \'N/A\')
                    contact_prox = extras.get(\'CONTACT_PROX\', \'N/A\')
                    reason_details[reason][f\'level: {level_price}, prox: {proximity_abs:.2f}, contact_prox: {contact_prox:.2f}\'] += 1
                elif reason == \'VETO\':
                    veto_code = extras.get(\'veto\', \'N/A\')
                    reason_details[reason][f\'veto_code: {veto_code}\'] += 1
                elif reason == \'NO_PLANNER_SIGNAL\':
                    reason_details[reason][\'General\'] += 1
                else:
                    reason_details[reason][\'General\'] += 1 # Catch-all for other reasons

            except json.JSONDecodeError:
                print(f"Warning: Could not parse features_json for entry at {ts}. Raw: {features_json_str[:100]}...")
                reason_counts[\'JSON_PARSE_ERROR\'] += 1
                reason_details[\'JSON_PARSE_ERROR\'][\'General\'] += 1

        print("--- Summary of Skip Reasons ---")
        for reason, count in sorted(reason_counts.items(), key=lambda item: item[1], reverse=True):
            print(f"Reason: {reason} (Count: {count})")
            for detail, detail_count in sorted(reason_details[reason].items(), key=lambda item: item[1], reverse=True):
                print(f"  - {detail}: {detail_count} times")
            print()

    except sqlite3.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    analyze_policy_events()