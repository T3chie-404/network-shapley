# generate_csv_data.py

import pandas as pd
import os
import random
import math # For Haversine distance
import re
import json
from datetime import datetime, timedelta
from pathlib import Path
import requests # For API calls
from dotenv import load_dotenv # For .env file
from collections import Counter # For operator link counting

# --- Initial Setup & Configuration ---
load_dotenv()

VALIDATORS_APP_API_KEY = os.getenv("VALIDATORS_APP_API_KEY")
VALIDATORS_API_ENDPOINT = "https://www.validators.app/api/v1/validators/mainnet.json"
CACHE_FILE_PATH = Path("validators_app_cache.json")
CACHE_STALE_DAYS = 7 # How old the API cache can be before prompting for a refresh
HUMAN_READABLE_VALIDATOR_SUMMARY_FILE = "validator_api_summary.txt"
CITIES_NEEDING_REVIEW_FILE = "cities_needing_region_review.csv"

LAMPORTS_PER_SOL = 1_000_000_000

# --- Simulation Parameters (User Configurable) ---
# Bandwidth settings for private links
HIGH_BANDWIDTH_VALUE = 100000  # Representing 100G link capacity (e.g., in Mbps)
STANDARD_BANDWIDTH_VALUE = 10000 # Representing 10G link capacity (e.g., in Mbps)

# Operator & Link Generation Configuration
NUM_TOTAL_OPERATORS = 25       # Total number of unique operators in the simulation (OperatorZ, DZ_Ops, Contributors)
NUM_TOP_OPERATORS = 5         # Number of operators considered "top-tier" for link distribution preferences
TOTAL_PRIVATE_LINKS_TARGET = 60 # Desired total number of private links (fixed + randomly generated)
HIGH_BANDWIDTH_RATIO_FOR_TOP_OPS = 0.90 # 90% of *randomly generated* links for top operators get high bandwidth

# Demand Generation Configuration
# This factor influences how much a source city's validator stake contributes to its generated traffic demand.
# Higher value = stake has a stronger influence. Lower value = stake has weaker influence.
# This is used in conjunction with per-route 'stake_influence' multipliers defined in 'raw_demand_definitions'.
# The effective multiplier in demand calculation is: (source_stake / max_stake) * STAKE_INFLUENCE_AGGREGATE_FACTOR * per_route_stake_influence
STAKE_INFLUENCE_AGGREGATE_FACTOR = 10.0 # General scaling factor for stake influence on demand.
MIN_DEMAND_PER_ROUTE = 1      # Minimum traffic units for any demand pair.


# --- City Data & Templates ---
# This dictionary is CRUCIAL for standardizing city names from the API to 3-letter codes
# and for providing/overriding latitude/longitude and country codes.
# Keys should be lowercase, underscore-separated "cityname_cc" (e.g., "frankfurt_de").
# 'code': Your desired 3-letter IATA-like code.
# 'lat', 'lon': Verified latitude/longitude. These will be prioritized.
# 'country_code_override': Use to correct or set a specific 2-letter country code if API data is ambiguous/wrong.
EXISTING_CITIES_TEMPLATE = {
    "ashburn_us":    {'code': "ASH", 'lat': 39.0438, 'lon': -77.4874},
    "new_york_us":   {'code': "NYC", 'lat': 40.7126, 'lon': -74.0066},
    "chicago_us":    {'code': "CHI", 'lat': 41.8835, 'lon': -87.6305},
    "dallas_us":     {'code': "DAL", 'lat': 32.7889, 'lon': -96.8021},
    "los_angeles_us":{'code': "LAX", 'lat': 34.0515, 'lon': -118.2707},
    "seattle_us":    {'code': "SEA", 'lat': 47.6062, 'lon': -122.3321},
    "miami_us":      {'code': "MIA", 'lat': 25.7701, 'lon': -80.1928},
    "salt_lake_city_us": {'code': "SLC", 'lat': 40.7592, 'lon': -111.8875},
    "toronto_ca":    {'code': "YYZ", 'lat': 43.7090, 'lon': -79.4057},
    "montreal_ca":   {'code': "YUL", 'lat': 45.5075, 'lon': -73.5887},
    "frankfurt_de":  {'code': "FRA", 'lat': 50.1169, 'lon': 8.6837, 'country_code_override': 'DE'},
    "amsterdam_nl":  {'code': "AMS", 'lat': 52.3759, 'lon': 4.8975, 'country_code_override': 'NL'},
    "london_gb":     {'code': "LON", 'lat': 51.4964, 'lon': -0.1224, 'country_code_override': 'GB'},
    "city_of_london_gb":{'code': "LON", 'lat': 51.5164, 'lon': -0.0930, 'country_code_override': 'GB'}, # Alias for LON
    "paris_fr":      {'code': "PAR", 'lat': 48.8558, 'lon': 2.3494, 'country_code_override': 'FR'},
    "prague_cz":     {'code': "PRG", 'lat': 50.0883, 'lon': 14.4124, 'country_code_override': 'CZ'},
    "tokyo_jp":      {'code': "TYO", 'lat': 35.6893, 'lon': 139.6899},
    "singapore_sg":  {'code': "SIN", 'lat': 1.3140, 'lon': 103.6839},
    "vaduz_li":      {'code': "VDZ", 'lat': 47.1410, 'lon': 9.5215, 'country_code_override': 'LI' },
    "vilnius_lt":    {'code': "VNO", 'lat': 54.6872, 'lon': 25.2797, 'country_code_override': 'LT'},
    "madrid_es":     {'code': "MAD", 'lat': 40.4153, 'lon': -3.6940, 'country_code_override': 'ES'},
    "stockholm_se":  {'code': "ARN", 'lat': 59.3287, 'lon': 18.0717, 'country_code_override': 'SE'},
    "helsinki_fi":   {'code': "HEL", 'lat': 60.1717, 'lon': 24.9349, 'country_code_override': 'FI'},
    "oslo_no":       {'code': "OSL", 'lat': 59.9133, 'lon': 10.7389, 'country_code_override': 'NO'},
    "warsaw_pl":     {'code': "WAW", 'lat': 52.2299, 'lon': 21.0093, 'country_code_override': 'PL'},
    "zurich_ch":     {'code': "ZRH", 'lat': 47.3643, 'lon': 8.5437, 'country_code_override': 'CH'},
    "dublin_ie":     {'code': "DUB", 'lat': 53.3382, 'lon': -6.2591, 'country_code_override': 'IE'},
    "berlin_de":     {'code': "BER", 'lat': 52.5200, 'lon': 13.4050, 'country_code_override': 'DE'},
    "hong_kong_hk":   {'code': "HKG", 'lat': 22.2578, 'lon': 114.1657},
    "sydney_au":     {'code': "SYD", 'lat': -33.8688, 'lon': 151.2093},
    "sao_paulo_br":   {'code': "GRU", 'lat': -23.5475, 'lon': -46.6361, 'country_code_override': 'BR'},
    "gravelines_fr":  {'code': "GVL", 'lat': 50.9830, 'lon': 2.1300, 'country_code_override': 'FR'}, # Often OVH
    "fechenheim_de":  {'code': "FEC", 'lat': 50.1210, 'lon': 8.7470, 'country_code_override': 'DE'}, # Near Frankfurt
    "strasbourg_fr": {'code': "SXB", 'lat': 48.5848, 'lon': 7.7419, 'country_code_override': 'FR'}, # Often OVH
    "buenos_aires_ar":{'code': "EZE", 'lat': -34.6037, 'lon': -58.3816, 'country_code_override': 'AR'},
    "ogden_us":      {'code': "OGD", 'lat': 41.2627, 'lon': -111.9837}, # Near SLC
    "roubaix_fr":    {'code': "RBX", 'lat': 50.6974, 'lon': 3.1780, 'country_code_override': 'FR'}, # Often OVH
    "remscheid_de":  {'code': "REM", 'lat': 51.1784, 'lon': 7.1601, 'country_code_override': 'DE'},
    "rotterdam_nl":  {'code': "RTM", 'lat': 51.9281, 'lon': 4.4220, 'country_code_override': 'NL'},
    "edinburgh_gb":  {'code': "EDI", 'lat': 55.9552, 'lon': -3.2000, 'country_code_override': 'GB'},
    "aubervilliers_fr": {'code': "AUB", 'lat': 48.9163, 'lon': 2.3869, 'country_code_override': 'FR'}, # Near Paris
    "bratislava_sk": {'code': "BTS", 'lat': 48.1577, 'lon': 17.1474, 'country_code_override': 'SK'},
    "bucharest_ro":  {'code': "OTP", 'lat': 44.4152, 'lon': 26.1660, 'country_code_override': 'RO'},
    "córdoba_es":    {'code': "ODB", 'lat': 37.8994, 'lon': -4.7741, 'country_code_override': 'ES'},
    "cluj_napoca_ro":{'code': "CLJ", 'lat': 46.7656, 'lon': 23.5945, 'country_code_override': 'RO'},
    "hattersheim_de":{'code': "HAT", 'lat': 50.0845, 'lon': 8.4719, 'country_code_override': 'DE'}, # Near Frankfurt
    "moscow_ru":     {'code': "SVO", 'lat': 55.7386, 'lon': 37.6068, 'country_code_override': 'RU'},
    "fryazino_ru":   {'code': "FRY", 'lat': 55.9606, 'lon': 38.0456, 'country_code_override': 'RU'}, # Near Moscow
    "whitechapel_gb":{'code': "WCL", 'lat': 51.5128, 'lon': -0.0638, 'country_code_override': 'GB'}, # Near London
    "offenbach_de":  {'code': "OFF", 'lat': 50.1093, 'lon': 8.7321, 'country_code_override': 'DE'}, # Near Frankfurt
    "riga_lv":       {'code': "RIX", 'lat': 56.9473, 'lon': 24.0979, 'country_code_override': 'LV'},
    "perm_ru":       {'code': "PEE", 'lat': 58.0047, 'lon': 56.2514, 'country_code_override': 'RU'},
    "kyiv_ua":       {'code': "KBP", 'lat': 50.4580, 'lon': 30.5303, 'country_code_override': 'UA'},
    "swinton_gb":    {'code': "SWN", 'lat': 53.4809, 'lon': -2.2374, 'country_code_override': 'GB'}, # Near Manchester
    "kemerovo_ru":   {'code': "KEJ", 'lat': 55.3299, 'lon': 86.0765, 'country_code_override': 'RU'},
    "spanga_se":     {'code': "SPA", 'lat': 59.3779, 'lon': 17.9155, 'country_code_override': 'SE'}, # Near Stockholm
    "lisbon_pt":     {'code': "LIS", 'lat': 38.7219, 'lon': -9.1398, 'country_code_override': 'PT'},
    "vienna_at":     {'code': "VIE", 'lat': 48.1773, 'lon': 16.2456, 'country_code_override': 'AT'},
    "tower_hamlets_gb":{'code': "THM", 'lat': 51.5064, 'lon': -0.0200, 'country_code_override': 'GB'}, # Near London
    "groningen_nl":  {'code': "GRQ", 'lat': 53.2222, 'lon': 6.5664, 'country_code_override': 'NL'},
    "leichlingen_de":{'code': "LEI", 'lat': 51.1060, 'lon': 7.0128, 'country_code_override': 'DE'},
    "worms_de":      {'code': "WOR", 'lat': 49.6357, 'lon': 8.3305, 'country_code_override': 'DE'},
    "lviv_ua":       {'code': "LWO", 'lat': 49.8390, 'lon': 24.0191, 'country_code_override': 'UA'},
    "rome_it":       {'code': "ROM", 'lat': 41.8904, 'lon': 12.5126, 'country_code_override': 'IT'},
    "espoo_fi":      {'code': "ESP", 'lat': 60.2050, 'lon': 24.6455, 'country_code_override': 'FI'}, # Near Helsinki
    "chelyabinsk_ru":{'code': "CEK", 'lat': 55.1581, 'lon': 61.4313, 'country_code_override': 'RU'},
    "kursk_ru":      {'code': "URS", 'lat': 51.7280, 'lon': 36.1895, 'country_code_override': 'RU'},
    "šaľa_sk":       {'code': "SAL", 'lat': 48.1592, 'lon': 17.8834, 'country_code_override': 'SK'},
    "halfweg_nl":    {'code': "HFW", 'lat': 52.3862, 'lon': 4.7506, 'country_code_override': 'NL'}, # Near Amsterdam
    "falkenstein_de":{'code': "FAL", 'lat': 50.4777, 'lon': 12.3649, 'country_code_override': 'DE'},
    "nuremberg_de":  {'code': "NUR", 'lat': 49.4521, 'lon': 11.0767, 'country_code_override': 'DE'},
    "milan_it":      {'code': "MIL", 'lat': 45.4642, 'lon': 9.1900, 'country_code_override': 'IT'},
    "brussels_be":   {'code': "BRU", 'lat': 50.8503, 'lon': 4.3517, 'country_code_override': 'BE'},
    "cognac_fr":     {'code': "COG", 'lat': 45.6955, 'lon': -0.3288, 'country_code_override': 'FR' },
    "guigang_cn":    {'code': "GUI", 'lat': 23.0964, 'lon': 109.6072, 'country_code_override': 'CN'},
    "madang_pg":     {'code': "MAG", 'lat': -5.2227, 'lon': 145.7947, 'country_code_override': 'PG'},
    "osasco_br":     {'code': "OSA", 'lat': -23.5312, 'lon': -46.7901, 'country_code_override': 'BR'}, # Near Sao Paulo
    "jerusalem_il":  {'code': "JRS", 'lat': 31.7683, 'lon': 35.2137, 'country_code_override': 'IL'},
    "bogotá_co":     {'code': "BOG", 'lat': 4.6115, 'lon': -74.0833, 'country_code_override': 'CO'},
    "santiago_cl":   {'code': "SCL", 'lat': -33.4521, 'lon': -70.6536, 'country_code_override': 'CL'},
    "durbanville_za":{'code': "DRB", 'lat': -33.8409, 'lon': 18.6566, 'country_code_override': 'ZA'}, # Near Cape Town
    "seoul_kr":      {'code': "SEL", 'lat': 37.5665, 'lon': 126.9780},
    "mumbai_in":     {'code': "BOM", 'lat': 19.0760, 'lon': 72.8777},
    "dubai_ae":      {'code': "DXB", 'lat': 25.2048, 'lon': 55.2708},
    "bangkok_th":    {'code': "BKK", 'lat': 13.7563, 'lon': 100.5018},
    "taipei_tw":     {'code': "TPE", 'lat': 25.0330, 'lon': 121.5654},
    "jakarta_id":    {'code': "JKT", 'lat': -6.2088, 'lon': 106.8456},
    "lima_pe":       {'code': "LIM", 'lat': -12.0464, 'lon': -77.0428},
    "johannesburg_za": {'code': "JNB", 'lat': -26.2041, 'lon': 28.0473},
    "nairobi_ke":    {'code': "NBO", 'lat': -1.2921, 'lon': 36.8219},
    "lagos_ng":      {'code': "LOS", 'lat': 6.5244, 'lon': 3.3792},
    "cayman_ky":     {'code': "GCM", 'lat': 19.3133, 'lon': -81.2546, 'country_code_override': 'KY'}, # Grand Cayman
    "UnknownCity_XX": {'code': "UNK", 'lat': 0.0, 'lon': 0.0, 'country_code_override': 'XX' }, # Fallback
}

# --- API Data Fetching and Caching ---
def fetch_validator_data_from_api(api_key):
    if not api_key:
        print("ERROR: VALIDATORS_APP_API_KEY not found in .env file or environment variables.")
        return None
    headers = {"Token": api_key}
    print(f"Fetching fresh data from {VALIDATORS_API_ENDPOINT}...")
    try:
        response = requests.get(VALIDATORS_API_ENDPOINT, headers=headers, params={'limit': 9999}, timeout=60) # Increased timeout
        response.raise_for_status() # Raises an HTTPError for bad responses (4XX or 5XX)
        print("Successfully fetched data from API.")
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err} - Status Code: {response.status_code}")
        if response.status_code == 401:
            print("This might be due to an invalid or expired API key.")
        elif response.status_code == 403:
            print("This might be due to API rate limits or permission issues.")
    except requests.exceptions.ConnectionError as conn_err:
        print(f"Connection error occurred: {conn_err}")
    except requests.exceptions.Timeout as timeout_err:
        print(f"Timeout error occurred: {timeout_err}")
    except requests.exceptions.RequestException as req_err:
        print(f"An unexpected error occurred during API request: {req_err}")
    return None

def load_or_fetch_validator_data(force_refresh=False):
    cached_data = None
    if CACHE_FILE_PATH.exists() and not force_refresh:
        try:
            with open(CACHE_FILE_PATH, 'r', encoding='utf-8') as f:
                cached_data_content = json.load(f)
            cache_timestamp = datetime.fromisoformat(cached_data_content['timestamp'])
            if datetime.now() - cache_timestamp > timedelta(days=CACHE_STALE_DAYS):
                print(f"Cache is older than {CACHE_STALE_DAYS} days.")
                if input("Fetch fresh data from API? (y/n): ").lower() != 'y':
                    print("Using stale cached data.")
                    return cached_data_content['data']
                else:
                    cached_data = None # Force refresh
            else:
                print("Using recent cached validator data.")
                return cached_data_content['data']
        except Exception as e:
            print(f"Error reading or parsing cache file '{CACHE_FILE_PATH}': {e}. Attempting to fetch fresh data.")
            cached_data = None # Invalidate cache on error

    if VALIDATORS_APP_API_KEY:
        fresh_data = fetch_validator_data_from_api(VALIDATORS_APP_API_KEY)
        if fresh_data:
            try:
                with open(CACHE_FILE_PATH, 'w', encoding='utf-8') as f:
                    json.dump({'timestamp': datetime.now().isoformat(), 'data': fresh_data}, f, indent=4)
                print(f"Saved fresh API data to {CACHE_FILE_PATH}")
            except Exception as e:
                print(f"Error writing cache file '{CACHE_FILE_PATH}': {e}")
            return fresh_data
        else:
            print("Failed to fetch fresh data from API.")
            if cached_data and isinstance(cached_data, dict) and 'data' in cached_data :
                 print("Falling back to previously loaded (but possibly stale) cache if available.")
                 return cached_data['data']
            return None
    else:
        print("No API key provided and no valid cache available. Cannot obtain validator data.")
        return None

def save_validator_api_summary_to_file(api_data, filename=HUMAN_READABLE_VALIDATOR_SUMMARY_FILE):
    if not api_data:
        print("No API data provided for summary.")
        return
    print(f"Saving human-readable validator API summary to '{filename}'...")
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(f"--- Validator API Data Summary (Total: {len(api_data)} entries, Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}) ---\n")
            for i, validator in enumerate(api_data):
                name = validator.get("name", "N/A")
                account = validator.get("account", "N/A")
                stake_lamports = validator.get("active_stake", 0)
                stake_sol = stake_lamports / LAMPORTS_PER_SOL if stake_lamports else 0.0
                dc_key = validator.get("data_center_key", "N/A")
                lat = validator.get("latitude", "N/A")
                lon = validator.get("longitude", "N/A")
                ip = validator.get("ip_address", validator.get("ip", "N/A"))
                asn = validator.get("autonomous_system_number", "N/A")
                f.write(f"\nValidator #{i+1}:\n  Name: {name}\n  Account: {account}\n  Active Stake: {stake_sol:,.2f} SOL ({stake_lamports:,} Lamports)\n")
                f.write(f"  Data Center Key: {dc_key}\n  API Location: Lat={lat}, Lon={lon}\n")
                if ip != "N/A": f.write(f"  IP: {ip}\n")
                if asn != "N/A": f.write(f"  ASN: {asn}\n")
            f.write("\n--- End of Validator API Data Summary ---")
        print(f"Successfully saved validator summary to '{filename}'")
        print(f"\n--- Console Validator API Data Summary (First 5 of {len(api_data)} entries) ---")
        for i, validator in enumerate(api_data[:5]):
            stake_lamports_console = validator.get('active_stake',0)
            stake_sol_console = stake_lamports_console / LAMPORTS_PER_SOL if stake_lamports_console else 0.0
            print(f"  Name: {validator.get('name', 'N/A')}, DC Key: {validator.get('data_center_key', 'N/A')}, Stake: {stake_sol_console:,.2f} SOL")
        if len(api_data) > 5: print("  ... (see full list in validator_api_summary.txt)")
    except Exception as e:
        print(f"Error writing validator summary to file '{filename}': {e}")

parsing_issues_count = 0
def parse_api_validator_data(api_data):
    global parsing_issues_count
    city_aggregates = {}
    if not api_data: return city_aggregates
    for validator in api_data:
        dc_key = validator.get("data_center_key"); stake = validator.get("active_stake", 0)
        lat_str = validator.get("latitude"); lon_str = validator.get("longitude")
        city_name_raw, country_code = "UnknownCity", "XX"
        if not dc_key or not isinstance(dc_key, str) or dc_key == "0--Unknown" or dc_key.strip() == "":
            parsing_issues_count +=1
        else:
            if "--" in dc_key:
                parts = dc_key.split('--', 1)
                if len(parts) > 1: city_name_raw = parts[1].strip()
                else: parsing_issues_count +=1; city_name_raw = dc_key
                city_parts_comma = city_name_raw.rsplit(',', 1)
                if len(city_parts_comma) == 2 and len(city_parts_comma[1].strip()) == 2 and city_parts_comma[1].strip().isalpha():
                    country_code = city_parts_comma[1].strip().upper(); city_name_raw = city_parts_comma[0].strip()
            else:
                parts = dc_key.split('-', 2)
                if len(parts) >= 3 and parts[0].isalpha() and len(parts[0])==2 : country_code = parts[0].upper(); city_name_raw = parts[1]
                elif len(parts) == 2 and parts[0].isalpha() and len(parts[0])==2 : country_code = parts[0].upper(); city_name_raw = parts[1]
                elif len(parts) == 1:
                    city_comma_parts = dc_key.rsplit(',', 1)
                    if len(city_comma_parts) == 2 and len(city_comma_parts[1].strip()) == 2 and city_comma_parts[1].strip().isalpha():
                        country_code = city_comma_parts[1].strip().upper(); city_name_raw = city_comma_parts[0].strip()
                    else: city_name_raw = dc_key.strip()
                else: city_name_raw = dc_key.strip()
        city_name_cleaned = "".join(filter(lambda x: x.isalnum() or x.isspace() or x == '-', city_name_raw)).strip()
        city_name_cleaned = city_name_cleaned.replace(" am Main", "").replace(" (Oder)", "").replace(" an der ", " ").strip()
        if '/' in city_name_cleaned: city_name_cleaned = city_name_cleaned.split('/')[-1].strip()
        city_name_cleaned = re.sub(r"^[^\w\s-]+|[^\w\s-]+$", "", city_name_cleaned).strip()
        if not city_name_cleaned or city_name_cleaned.lower() == "unknown": city_name_cleaned = "UnknownCity"
        if not (country_code and len(country_code) == 2 and country_code.isalpha() and country_code.isupper()):
            if country_code != "XX": parsing_issues_count +=1
            country_code = "XX"
        if country_code == "EN": country_code = "GB"
        norm_city_template_key = city_name_cleaned.lower().replace(" ", "_").replace("-", "_").strip('_') + "_" + country_code.lower()
        if country_code == "XX":
            for t_key, t_val in EXISTING_CITIES_TEMPLATE.items():
                if "_".join(t_key.split('_')[:-1]) == city_name_cleaned.lower().replace(" ", "_").replace("-", "_").strip('_') and t_val.get('country_code_override'):
                    country_code = t_val['country_code_override']; break
        elif EXISTING_CITIES_TEMPLATE.get(norm_city_template_key, {}).get('country_code_override'):
            country_code = EXISTING_CITIES_TEMPLATE[norm_city_template_key]['country_code_override']
        agg_key = f"{city_name_cleaned}, {country_code}"
        if agg_key not in city_aggregates:
            city_aggregates[agg_key] = {'city_name': city_name_cleaned, 'country_code': country_code, 'stake': 0, 'population': 0, 'lat': 0.0, 'lon': 0.0, 'raw_dc_keys': set()}
        city_aggregates[agg_key]['stake'] += int(stake) if stake else 0
        city_aggregates[agg_key]['population'] += 1
        if dc_key: city_aggregates[agg_key]['raw_dc_keys'].add(dc_key)
        for coord_str, coord_field in [(lat_str, 'lat'), (lon_str, 'lon')]:
            if city_aggregates[agg_key][coord_field] == 0.0 and coord_str and isinstance(coord_str, (str, float, int)) and re.match(r"^-?\d+(\.\d+)?$", str(coord_str)):
                city_aggregates[agg_key][coord_field] = float(coord_str)
    if parsing_issues_count > 0: print(f"Note (parse_api): Encountered {parsing_issues_count} potential parsing issues.")
    return city_aggregates

def get_or_assign_code(city_name, country_code, used_codes_session):
    generated_code = None; norm_city_name = city_name.lower().replace(" ", "_").replace("-", "_").strip('_')
    norm_cc = country_code.lower().strip(); lookup_key = f"{norm_city_name}_{norm_cc}"
    if EXISTING_CITIES_TEMPLATE.get(lookup_key): generated_code = EXISTING_CITIES_TEMPLATE[lookup_key].get('code')
    if not generated_code:
        for t_key, t_val in EXISTING_CITIES_TEMPLATE.items():
            t_city_part, t_cc_part = "_".join(t_key.split('_')[:-1]), t_key.split('_')[-1]
            name_match = norm_city_name == t_city_part or (len(norm_city_name) >=3 and norm_city_name in t_city_part) or (len(t_city_part) >=3 and t_city_part in norm_city_name)
            cc_match = norm_cc == t_cc_part or norm_cc == t_val.get('country_code_override','').lower()
            if name_match and cc_match and t_val.get('code'): generated_code = t_val['code']; break
    if not generated_code:
        base = "".join(filter(str.isalpha, city_name.upper()))[:2] or "".join(filter(str.isalnum, city_name.upper()))[:2] or "XX"
        cc_part = "".join(filter(str.isalpha, country_code.upper()))[:1] or "X"
        generated_code = "".join(filter(str.isalpha, (base + cc_part).upper()))
        generated_code = (generated_code + "XXX")[:3] if len(generated_code) < 3 else generated_code[:3]
    final_code, counter, original_code, alphabet = generated_code or "UNK", 0, generated_code or "UNK", "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    current_try = final_code
    while current_try in used_codes_session:
        counter += 1; idx = (counter-1) % len(alphabet)
        if counter <= len(alphabet): current_try = (original_code[:2] if len(original_code)>=2 else original_code[0]+"X") + alphabet[idx]
        elif counter <= 2*len(alphabet): current_try = (original_code[0] if original_code else "X") + alphabet[idx] + (original_code[2] if len(original_code)==3 else "Y")
        elif counter <= 3*len(alphabet): current_try = alphabet[idx] + (original_code[1:] if len(original_code)>1 else "XY")
        else: current_try = "".join(random.sample(alphabet, 3))
        current_try = "".join(filter(str.isalnum, current_try.upper()))[:3]; current_try = (current_try + "XXX")[:3]
        if current_try in used_codes_session and counter > 3*len(alphabet)+5: return "ERR" # Give up earlier
        if counter > 3*len(alphabet) + 50: final_code = "FLX"; if final_code in used_codes_session: return "ERR"; break
        final_code = current_try
    return final_code

def initialize_cities_database(api_parsed_data):
    final_db = {}; used_codes = set()
    if not api_parsed_data: return final_db
    for city_key, api_data_item in api_parsed_data.items():
        city_name, country_code = api_data_item['city_name'], api_data_item['country_code']
        if country_code == "XX" and city_name != "UnknownCity":
            norm_city = city_name.lower().replace(" ", "_").replace("-","_").strip('_')
            for t_key, t_val in EXISTING_CITIES_TEMPLATE.items():
                if "_".join(t_key.split('_')[:-1]) == norm_city and t_val.get('country_code_override'):
                    country_code = t_val['country_code_override']; city_key = f"{city_name}, {country_code}"; api_data_item['country_code'] = country_code; break
        code = get_or_assign_code(city_name, country_code, used_codes)
        if code == "ERR": continue
        used_codes.add(code); lat, lon = api_data_item.get('lat',0.0), api_data_item.get('lon',0.0)
        template_entry = EXISTING_CITIES_TEMPLATE.get(f"{city_name.lower().replace(' ','_')}_{country_code.lower()}")
        if not template_entry:
            for _k, v in EXISTING_CITIES_TEMPLATE.items():
                if v.get('code') == code: template_entry = v; break
        if template_entry and (lat == 0.0 and lon == 0.0 or template_entry.get('code') != code): # Prioritize template if API is 0,0 or code came from this template
            if template_entry.get('lat',0.0) != 0.0 or template_entry.get('lon',0.0) != 0.0:
                lat, lon = template_entry.get('lat',0.0), template_entry.get('lon',0.0)
        if lat == 0.0 and lon == 0.0 and code not in ["UNK","ERR"]: print(f"ACTION NEEDED: City '{city_key}' (Code: {code}) has NO Lat/Lon. Add to EXISTING_CITIES_TEMPLATE.")
        final_db[code] = {'descriptive_name':city_key, 'parsed_city_name':city_name, 'country_code':country_code, 'lat':lat, 'lon':lon, 'stake':api_data_item['stake'], 'population':api_data_item['population'], 'raw_dc_keys':list(api_data_item.get('raw_dc_keys',[]))}
    print(f"Initialized CITIES_DATABASE with {len(final_db)} cities.")
    return final_db

CITIES_DATABASE = {}
city_codes = []
api_data_content = load_or_fetch_validator_data()
if api_data_content:
    save_validator_api_summary_to_file(api_data_content)
    parsed_api_cities_aggregated = parse_api_validator_data(api_data_content)
    CITIES_DATABASE = initialize_cities_database(parsed_api_cities_aggregated)
else:
    print("CRITICAL ERROR: No validator data. CITIES_DATABASE empty.")
city_codes = list(CITIES_DATABASE.keys()) if CITIES_DATABASE else []

def haversine(lat1, lon1, lat2, lon2):
    R = 3958.8; dLat = math.radians(lat2 - lat1); dLon = math.radians(lon2 - lon1)
    a = math.sin(dLat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dLon/2)**2
    return R * (2 * math.atan2(math.sqrt(a), math.sqrt(1-a)))
LATENCY_PER_MILE_CONTINENTAL = 0.018; LATENCY_PER_MILE_INTERCONTINENTAL_OVERLAND = 0.020
BASE_LATENCY_CONTINENTAL = 2; BASE_LATENCY_INTERCONTINENTAL = 15
MAJOR_REGIONS = {
    'NA': ["ASH", "NYC", "CHI", "DAL", "LAX", "SEA", "MIA", "SLC", "YYZ", "YUL", "ATL", "DEN", "PHX", "YVR", "EWR", "PIT", "BTR", "PIC", "BDM", "OGD", "ANC", "LAS", "MRN", "BGM", "TPA", "BOS", "CBF", "MRC", "KNT", "AUS", "SIY", "FTW", "SEC", "SJC", "EGV", "COU", "CAK", "QRO", "RAW"],
    'EU': ["FRA", "AMS", "LON", "PAR", "MAD", "NUR", "WAW", "ZRH", "FAL", "DUB", "VIE", "ARN", "HEL", "MIL", "BRU", "OSL", "VNO", "LUX", "SXB", "FEC", "BER", "OFF", "GVL", "RBX", "VDZ", "BTS", "SWN", "PRG", "GRQ", "SVO", "HAT", "CLJ", "WOR", "SAL", "CVT", "RIX", "LEI", "OTP", "LIS", "MAN", "ESP", "CEK", "PEE", "KEJ", "KBP", "URS", "REM", "FRY", "ROM", "VIS", "WCL", "SPA", "COG", "THM", "AUB", "EDI"],
    'AS': ["TYO", "SIN", "SEL", "HKG", "BOM", "DXB", "BKK", "TPE", "JKT", "DEL", "JRS", "GUI"],
    'OC': ["SYD", "MAG"], 'SA': ["GRU", "EZE", "LIM", "SCL", "BOG", "OSA"], 'AF': ["JNB", "NBO", "LOS", "DRB"], 'KY': ["GCM"],
    'UNKNOWN_REGION_TEMP': []
}
def get_region(city_code, db, regions_map):
    if not db or city_code not in db: return "UNKNOWN"
    for r, codes in regions_map.items():
        if r != 'UNKNOWN_REGION_TEMP' and city_code in codes: return r
    cc = db.get(city_code, {}).get('country_code','').upper()
    if cc:
        if cc in ["US","CA","MX"]: return 'NA'
        if cc in ["GB","DE","FR","NL","ES","PL","CH","IE","AT","SE","FI","IT","BE","NO","LT","LU","CZ","PT","SK","RO","LV","RU","UA","LI","IS","GR","HU","BG","HR","SI","EE","CY","MT","AL","RS","BA","MK","ME","XK"]: return 'EU'
        if cc in ["JP","SG","HK","KR","IN","AE","TH","TW","ID","IL","CN","VN","PH","MY","SA","QA","TR","PK","BD","LK"]: return 'AS'
        if cc in ["AU","NZ","PG","FJ"]: return 'OC'
        if cc in ["BR","AR","PE","CL","CO","VE","EC","UY","PY","BO"]: return 'SA'
        if cc in ["ZA","KE","NG","EG","MA","GH","CI","DZ","AO","TZ","SD","CM"]: return 'AF'
        if cc == "KY": return 'KY'
    if city_code not in regions_map.get('UNKNOWN_REGION_TEMP',[]): regions_map.get('UNKNOWN_REGION_TEMP',[]).append(city_code)
    return "UNKNOWN"

new_public_links_data = []
if city_codes:
    for i in range(len(city_codes)):
        for j in range(i + 1, len(city_codes)):
            c1, c2 = city_codes[i], city_codes[j]
            if not (c1 and c2) or c1=="UNK" or c2=="UNK" or c1=="ERR" or c2=="ERR": continue
            cost, note, dist = -1, "", -1
            d1, d2 = CITIES_DATABASE.get(c1), CITIES_DATABASE.get(c2)
            if d1 and d2 and all(d.get(k,0.0)!=0.0 for d in [d1,d2] for k in ['lat','lon']):
                dist = haversine(d1['lat'],d1['lon'],d2['lat'],d2['lon'])
                r1,r2 = get_region(c1,CITIES_DATABASE,MAJOR_REGIONS), get_region(c2,CITIES_DATABASE,MAJOR_REGIONS)
                if r1 and r2 and r1!="UNKNOWN" and r2!="UNKNOWN":
                    cost = (BASE_LATENCY_INTERCONTINENTAL + dist*LATENCY_PER_MILE_INTERCONTINENTAL_OVERLAND) if r1!=r2 else (BASE_LATENCY_CONTINENTAL + dist*LATENCY_PER_MILE_CONTINENTAL)
                    note = f"Est. {'Inter' if r1!=r2 else 'Intra'}Cont ({r1}{'-'+r2 if r1!=r2 else ''}): ~{dist:.0f}mi"
                else: cost=BASE_LATENCY_INTERCONTINENTAL+dist*LATENCY_PER_MILE_INTERCONTINENTAL_OVERLAND; note=f"Est. Default (region {r1 or 'N/A'}/{r2 or 'N/A'} unk): ~{dist:.0f}mi"
                cost = max(1, int(round(cost)))
            else: cost=150; note=f"Fallback (Lat/Lon missing for {c1 if not (d1 and d1.get('lat')) else ''}{', ' if not (d1 and d1.get('lat')) and not (d2 and d2.get('lat')) else ''}{c2 if not (d2 and d2.get('lat')) else ''})"
            cap = 1000 + int(dist/10) if dist > 0 else 1000
            if cost > 0: new_public_links_data.append( ((c1,c2), cost, cap, note) )
else: print("Warning: No city_codes for public links.")

DZ_TESTNET_LINKS_RAW_DESCRIPTIVE = [
    {'cities': (('Singapore', 'SG'), ('Tokyo', 'JP')), 'latency_rtt_ms': 67.20, 'owner_pubkey': "66yfemxTAjCL686R4FFpGugx1myQ7X6m274MzWB82xBy"},
    {'cities': (('Amsterdam', 'NL'), ('London', 'GB')), 'latency_rtt_ms': 5.76,  'owner_pubkey': "66yfemxTAjCL686R4FFpGugx1myQ7X6m274MzWB82xBy"},
    {'cities': (('Frankfurt', 'DE'), ('Prague', 'CZ')), 'latency_rtt_ms': 7.01,  'owner_pubkey': "RoXFXFQAqBxYx6QZYG9AmGMWpSyr7xJPPqAy3FCafpv"},
    {'cities': (('New York', 'US'), ('London', 'GB')), 'latency_rtt_ms': 66.93, 'owner_pubkey': "66yfemxTAjCL686R4FFpGugx1myQ7X6m274MzWB82xBy"},
    {'cities': (('London', 'GB'), ('Singapore', 'SG')), 'latency_rtt_ms': 152.54,'owner_pubkey': "66yfemxTAjCL686R4FFpGugx1myQ7X6m274MzWB82xBy"},
    {'cities': (('Los Angeles', 'US'), ('New York', 'US')), 'latency_rtt_ms': 69.71, 'owner_pubkey': "66yfemxTAjCL686R4FFpGugx1myQ7X6m274MzWB82xBy"},
    {'cities': (('Tokyo', 'JP'), ('Los Angeles', 'US')), 'latency_rtt_ms': 98.71, 'owner_pubkey': "66yfemxTAjCL686R4FFpGugx1myQ7X6m274MzWB82xBy"},
    {'cities': (('London', 'GB'), ('Frankfurt', 'DE')), 'latency_rtt_ms': 11.09, 'owner_pubkey': "66yfemxTAjCL686R4FFpGugx1myQ7X6m274MzWB82xBy"},
]
DZ_TESTNET_BANDWIDTH = STANDARD_BANDWIDTH_VALUE
OPERATOR_Z = "OperatorZ" # Primary operator, always included if NUM_TOTAL_OPERATORS >= 1

descriptive_name_to_code_map = {data['descriptive_name']: code for code, data in CITIES_DATABASE.items()} if CITIES_DATABASE else {}
if CITIES_DATABASE: # Add parsed_city_name mappings
    for code, data in CITIES_DATABASE.items():
        parsed_key = f"{data['parsed_city_name']}, {data['country_code']}"
        if parsed_key not in descriptive_name_to_code_map: descriptive_name_to_code_map[parsed_key] = code

dz_op_map = {link['owner_pubkey']: f"DZ_Op_{link['owner_pubkey'][:4]}" for link in DZ_TESTNET_LINKS_RAW_DESCRIPTIVE}
all_ops = []
if OPERATOR_Z and NUM_TOTAL_OPERATORS > 0: all_ops.append(OPERATOR_Z)
for dz_name in sorted(list(set(dz_op_map.values()))):
    if dz_name not in all_ops and len(all_ops) < NUM_TOTAL_OPERATORS: all_ops.append(dz_name)
idx = 1
while len(all_ops) < NUM_TOTAL_OPERATORS:
    c_name = f"Contributor{idx}"; idx+=1
    if c_name not in all_ops: all_ops.append(c_name)
    if idx > NUM_TOTAL_OPERATORS*2 + len(dz_op_map): break # Safety
all_operator_names = all_ops[:NUM_TOTAL_OPERATORS]

top_operators = []
if OPERATOR_Z in all_operator_names: top_operators.append(OPERATOR_Z)
potential_tops = sorted([op for op in all_operator_names if op.startswith("Contributor")])
for op_name in potential_tops:
    if len(top_operators) < NUM_TOP_OPERATORS and op_name not in top_operators: top_operators.append(op_name)
idx = 0
while len(top_operators) < NUM_TOP_OPERATORS and idx < len(all_operator_names):
    if all_operator_names[idx] not in top_operators: top_operators.append(all_operator_names[idx])
    idx += 1
other_operators = [name for name in all_operator_names if name not in top_operators]

new_private_links_data = []
for link in DZ_TESTNET_LINKS_RAW_DESCRIPTIVE:
    (c1_n, c1_cc), (c2_n, c2_cc) = link['cities']
    sc, ec = descriptive_name_to_code_map.get(f"{c1_n}, {c1_cc}"), descriptive_name_to_code_map.get(f"{c2_n}, {c2_cc}")
    if sc and ec:
        cost = max(1, int(round(link['latency_rtt_ms']/2.0))); op_name = dz_op_map[link['owner_pubkey']]
        new_private_links_data.append({'operator':op_name, 'start':sc, 'end':ec, 'cost':cost, 'bandwidth':DZ_TESTNET_BANDWIDTH, 'shared_tag':f"dz_{sc}_{ec}_{op_name}"})
    else: print(f"Warning (DZ): Could not map cities for link: {c1_n},{c1_cc} to {c2_n},{c2_cc}. Skipping.")

chi_c, slc_c = EXISTING_CITIES_TEMPLATE.get("chicago_us",{}).get('code'), EXISTING_CITIES_TEMPLATE.get("salt_lake_city_us",{}).get('code')
if slc_c and chi_c and slc_c in CITIES_DATABASE and chi_c in CITIES_DATABASE:
    if OPERATOR_Z in all_operator_names:
        new_private_links_data.append({'operator':OPERATOR_Z, 'start':slc_c, 'end':chi_c, 'cost':14, 'bandwidth':HIGH_BANDWIDTH_VALUE, 'shared_tag':"opz_slc_chi"})
    c1_name = "Contributor1"
    if c1_name in top_operators and c1_name in all_operator_names:
         new_private_links_data.append({'operator':c1_name, 'start':slc_c, 'end':chi_c, 'cost':13, 'bandwidth':HIGH_BANDWIDTH_VALUE, 'shared_tag':"c1_slc_chi"}) # Changed to HIGH_BANDWIDTH_VALUE
else: print(f"Warning (Fixed): SLC ({slc_c}) or CHI ({chi_c}) not found for OperatorZ/C1 link. Skipping.")

random_links_to_generate = max(0, TOTAL_PRIVATE_LINKS_TARGET - len(new_private_links_data))
public_link_cost_lookup = {(s,e):c for ((s,e),c,_,_) in new_public_links_data}
public_link_cost_lookup.update({(e,s):c for ((s,e),c,_,_) in new_public_links_data})

raw_demand_definitions = [
    {'name': 'P_US_EU', 'source_desc': ("New York", "US"), 'destination_desc': ("Frankfurt", "DE"), 'base_traffic_weight': 20, 'value': 500, 'stake_influence': 0.7},
    {'name': 'P_ASIA_US', 'source_desc': ("Tokyo", "JP"), 'destination_desc': ("Los Angeles", "US"), 'base_traffic_weight': 15, 'value': 400, 'stake_influence': 0.8},
    {'name': 'P_EU_Regional', 'source_desc': ("Paris", "FR"), 'destination_desc': ("Amsterdam", "NL"), 'base_traffic_weight': 10, 'value': 150, 'stake_influence': 0.5},
    {'name': 'P_US_Internal', 'source_desc': ("Chicago", "US"), 'destination_desc': ("Dallas", "US"), 'base_traffic_weight': 12, 'value': 200, 'stake_influence': 0.6},
    {'name': 'P_EU_Nordic', 'source_desc': ("Stockholm", "SE"), 'destination_desc': ("Helsinki", "FI"), 'base_traffic_weight': 5,  'value': 90,  'stake_influence': 0.4},
    {'name': 'P_SIN_HK', 'source_desc': ("Singapore", "SG"), 'destination_desc': ("Hong Kong", "HK"), 'base_traffic_weight': 18, 'value': 300, 'stake_influence': 0.7},
    {'name': 'P_SLC_CHI', 'source_desc': ("Salt Lake City", "US"), 'destination_desc': ("Chicago", "US"), 'base_traffic_weight': 8,  'value': 180, 'stake_influence': 0.9},
    {'name': 'P_MajorStakeSource', 'source_desc': ("Frankfurt", "DE"), 'destination_desc': ("New York", "US"), 'base_traffic_weight': 25, 'value': 550, 'stake_influence': 1.0},
    {'name': 'P_AnotherStakeRoute', 'source_desc': ("Amsterdam", "NL"), 'destination_desc': ("London", "GB"), 'base_traffic_weight': 20, 'value': 250, 'stake_influence': 0.8},
    {'name': 'P_DZ_SIN_TYO', 'source_desc': ("Singapore", "SG"), 'destination_desc': ("Tokyo", "JP"), 'base_traffic_weight': 20, 'value': 400, 'stake_influence': 0.8},
    {'name': 'P_DZ_AMS_LON', 'source_desc': ("Amsterdam", "NL"), 'destination_desc': ("London", "GB"), 'base_traffic_weight': 15, 'value': 300, 'stake_influence': 0.7},
    {'name': 'P_DZ_FRA_PRG', 'source_desc': ("Frankfurt", "DE"), 'destination_desc': ("Prague", "CZ"), 'base_traffic_weight': 10, 'value': 200, 'stake_influence': 0.6},
    {'name': 'P_NA_TransCon_West', 'source_desc': ("Los Angeles", "US"), 'destination_desc': ("Chicago", "US"), 'base_traffic_weight': 22, 'value': 280, 'stake_influence': 0.65},
    {'name': 'P_NA_TransCon_East', 'source_desc': ("New York", "US"), 'destination_desc': ("Seattle", "US"), 'base_traffic_weight': 18, 'value': 250, 'stake_influence': 0.6},
    {'name': 'P_EU_WestEast',  'source_desc': ("Paris", "FR"), 'destination_desc': ("Warsaw", "PL"), 'base_traffic_weight': 15, 'value': 180, 'stake_influence': 0.5},
    {'name': 'P_EU_NorthSouth','source_desc': ("Stockholm", "SE"), 'destination_desc': ("Madrid", "ES"), 'base_traffic_weight': 12, 'value': 160, 'stake_influence': 0.45},
    {'name': 'P_ASIA_Intra1', 'source_desc': ("Singapore", "SG"), 'destination_desc': ("Mumbai", "IN"), 'base_traffic_weight': 20, 'value': 220, 'stake_influence': 0.7},
    {'name': 'P_ASIA_Intra2', 'source_desc': ("Hong Kong", "HK"), 'destination_desc': ("Seoul", "KR"), 'base_traffic_weight': 15, 'value': 190, 'stake_influence': 0.6},
    {'name': 'P_US_Canada',   'source_desc': ("Chicago", "US"), 'destination_desc': ("Toronto", "CA"), 'base_traffic_weight': 10, 'value': 130, 'stake_influence': 0.5},
    {'name': 'P_EU_UK',       'source_desc': ("Frankfurt", "DE"), 'destination_desc': ("London", "GB"), 'base_traffic_weight': 25, 'value': 300, 'stake_influence': 0.75},
    {'name': 'P_US_SouthAm',  'source_desc': ("Miami", "US"), 'destination_desc': ("Sao Paulo", "BR"), 'base_traffic_weight': 18, 'value': 260, 'stake_influence': 0.6},
    {'name': 'P_EU_Africa',   'source_desc': ("Madrid", "ES"), 'destination_desc': ("Johannesburg", "ZA"), 'base_traffic_weight': 10, 'value': 150, 'stake_influence': 0.4},
    {'name': 'P_Asia_Oceania','source_desc': ("Singapore", "SG"), 'destination_desc': ("Sydney", "AU"), 'base_traffic_weight': 20, 'value': 240, 'stake_influence': 0.7},
]

if random_links_to_generate > 0 and city_codes and len(city_codes) >= 2:
    prioritized_routes = []
    if CITIES_DATABASE and descriptive_name_to_code_map:
        temp_scores = []
        for entry in raw_demand_definitions:
            sc_n,sc_cc = entry['source_desc']; dc_n,dc_cc = entry['destination_desc']
            sc_code, dc_code = descriptive_name_to_code_map.get(f"{sc_n}, {sc_cc}"), descriptive_name_to_code_map.get(f"{dc_n}, {dc_cc}")
            if sc_code and dc_code and sc_code in CITIES_DATABASE and dc_code in CITIES_DATABASE:
                s_stake, d_stake = CITIES_DATABASE[sc_code].get('stake',0), CITIES_DATABASE[dc_code].get('stake',0)
                temp_scores.append( ((sc_code,dc_code), s_stake+d_stake) )
        prioritized_routes = [p for p,s in sorted(temp_scores, key=lambda x:x[1], reverse=True)]
    
    valid_city_codes_for_random = [c for c in city_codes if c not in ["UNK", "ERR"]]
    if len(valid_city_codes_for_random) < 2:
        print("Warning: Not enough valid city codes for random link generation.")
    else:
        num_top_op_links = int(random_links_to_generate * 0.80)
        for i in range(num_top_op_links):
            if not top_operators: break
            op = random.choice(top_operators)
            c1,c2 = prioritized_routes[i%len(prioritized_routes)] if prioritized_routes and i < len(prioritized_routes) else random.sample(valid_city_codes_for_random,2)
            if c1==c2: continue
            pub_cost = public_link_cost_lookup.get((c1,c2), public_link_cost_lookup.get((c2,c1),150))
            priv_cost = max(1, int(round(pub_cost * (1-random.uniform(0.03,0.20)))))
            bw = HIGH_BANDWIDTH_VALUE if random.random() < HIGH_BANDWIDTH_RATIO_FOR_TOP_OPS else STANDARD_BANDWIDTH_VALUE
            new_private_links_data.append({'operator':op,'start':c1,'end':c2,'cost':priv_cost,'bandwidth':bw,'shared_tag':f"rand_{op}_{c1}_{c2}"})
        
        num_other_op_links = random_links_to_generate - num_top_op_links
        if other_operators and num_other_op_links > 0:
            for i in range(num_other_op_links):
                op = random.choice(other_operators)
                route_idx = (i+num_top_op_links) % len(prioritized_routes) if prioritized_routes else -1
                c1,c2 = prioritized_routes[route_idx] if prioritized_routes and route_idx < len(prioritized_routes) else random.sample(valid_city_codes_for_random,2)
                if c1==c2: continue
                pub_cost = public_link_cost_lookup.get((c1,c2), public_link_cost_lookup.get((c2,c1),150))
                priv_cost = max(1, int(round(pub_cost * (1-random.uniform(0.03,0.20)))))
                new_private_links_data.append({'operator':op,'start':c1,'end':c2,'cost':priv_cost,'bandwidth':STANDARD_BANDWIDTH_VALUE,'shared_tag':f"rand_{op}_{c1}_{c2}"})

new_participants_data = []
max_obs_stake_lp = 0.0
if CITIES_DATABASE:
    stakes = [float(d.get('stake',0)) for d in CITIES_DATABASE.values() if d.get('stake',0)>0]
    if stakes: max_obs_stake_lp = max(stakes)
if max_obs_stake_lp == 0: max_obs_stake_lp = float(30*LAMPORTS_PER_SOL)

for entry in raw_demand_definitions:
    (sc_n,sc_cc), (dc_n,dc_cc) = entry['source_desc'], entry['destination_desc']
    sc_code, dc_code = descriptive_name_to_code_map.get(f"{sc_n}, {sc_cc}"), descriptive_name_to_code_map.get(f"{dc_n}, {dc_cc}")
    if sc_code and dc_code and sc_code in CITIES_DATABASE and dc_code in CITIES_DATABASE:
        s_stake_lp = float(CITIES_DATABASE[sc_code].get('stake',0))
        stake_mult_eff = (s_stake_lp/max_obs_stake_lp * STAKE_INFLUENCE_AGGREGATE_FACTOR * entry['stake_influence']) if max_obs_stake_lp > 0 else 0
        demand_vol = max(MIN_DEMAND_PER_ROUTE, int(round(entry['base_traffic_weight']*(1+stake_mult_eff))))
        new_participants_data.append({'name':entry['name'], 'source':sc_code, 'destination':dc_code, 'demand':demand_vol, 'value':entry['value']})
    else: print(f"Warning (Demand): Pair Source:'{sc_n},{sc_cc}'->{sc_code}, Dest:'{dc_n},{dc_cc}'->{dc_code} not mapped. Skipping.")

def to_switch_name(city_code):
    return city_code.upper()+"1" if isinstance(city_code,str) and len(city_code)==3 and city_code.isalpha() and not city_code[-1].isdigit() else city_code

def generate_public_links_csv(data, filename="public_links.csv"):
    df_data = [{"Start":to_switch_name(s),"End":to_switch_name(e),"Cost":c} for ((s,e),c,_,_) in data]
    pd.DataFrame(df_data).to_csv(filename, index=False); print(f"Generated '{filename}' ({len(df_data)} links).")
def generate_private_links_csv(data, filename="private_links.csv"):
    df_data, shared_map, next_id = [], {}, 1
    for item in data:
        shared_val = "NA"
        if item.get('shared_tag'):
            if item['shared_tag'] not in shared_map: shared_map[item['shared_tag']] = next_id; next_id+=1
            shared_val = shared_map[item['shared_tag']]
        df_data.append({"Start":to_switch_name(item['start']), "End":to_switch_name(item['end']), "Cost":item['cost'], "Bandwidth":item['bandwidth'], "Operator1":item['operator'], "Operator2":"NA", "Uptime":0.99, "Shared":shared_val})
    pd.DataFrame(df_data).to_csv(filename, index=False); print(f"Generated '{filename}' ({len(df_data)} links).")
def generate_demand_csv(data, filename="demand.csv"):
    df_data = [{"Start":item['source'],"End":item['destination'],"Traffic":item['demand'],"Type":item['name']} for item in data]
    pd.DataFrame(df_data).to_csv(filename, index=False); print(f"Generated '{filename}' ({len(df_data)} pairs).")

if __name__ == "__main__":
    start_time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"--- Starting CSV Generation ({start_time_str}) ---")
    if parsing_issues_count > 0: print(f"\nWARNING: {parsing_issues_count} API parsing issues noted.")
    if not CITIES_DATABASE: print("\nCRITICAL ERROR: CITIES_DATABASE empty. Aborting.")
    else:
        print(f"\nCITIES_DATABASE: {len(CITIES_DATABASE)} entries.")
        generate_public_links_csv(new_public_links_data)
        generate_private_links_csv(new_private_links_data)
        generate_demand_csv(new_participants_data)
        print(f"\n--- CSV generation complete. ---")
        print(f"  Unique cities: {len(CITIES_DATABASE)}, Public links: {len(new_public_links_data)}, Private links: {len(new_private_links_data)}, Demand pairs: {len(new_participants_data)}")
        if new_private_links_data:
            op_counts = Counter(link['operator'] for link in new_private_links_data)
            print("\n--- Operator Link Counts ---")
            for op, count in sorted(op_counts.items(), key=lambda item: item[1], reverse=True): print(f"  {op:<20}: {count}")
            print(f"Total unique operators with links: {len(op_counts)}")
            if 'random_links_to_generate' in globals(): print(f"Target random links: {random_links_to_generate}")
        cities_for_review = []
        if CITIES_DATABASE:
            for code, data in CITIES_DATABASE.items():
                region = get_region(code, CITIES_DATABASE, MAJOR_REGIONS)
                if region == "UNKNOWN" or code in MAJOR_REGIONS.get('UNKNOWN_REGION_TEMP',[]):
                    stake_sol = data.get('stake',0)/LAMPORTS_PER_SOL
                    cities_for_review.append({"GeneratedCode":code, "DescriptiveName":data.get('descriptive_name','N/A'), "ParsedCityName":data.get('parsed_city_name','N/A'), "CountryCode":data.get('country_code','XX'), "OriginalDCKeys":"; ".join(sorted(list(data.get('raw_dc_keys',[])))), "Stake_SOL":f"{stake_sol:,.2f}", "Validators":data.get('population',0), "Lat":data.get('lat',0.0), "Lon":data.get('lon',0.0), "AssignedRegion":region})
        if cities_for_review:
            pd.DataFrame(cities_for_review).sort_values(by="GeneratedCode").to_csv(CITIES_NEEDING_REVIEW_FILE, index=False)
            print(f"\nACTION REQUIRED: {len(cities_for_review)} cities need review. See '{CITIES_NEEDING_REVIEW_FILE}'.")
            print("  Update `EXISTING_CITIES_TEMPLATE` and `MAJOR_REGIONS`.")
        else: print("\nAll cities mapped to a major region.")
    if parsing_issues_count > 0: print(f"\nReminder: {parsing_issues_count} parsing issues noted.")
    print(f"--- Script Finished ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')}) ---")


