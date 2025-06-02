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
CACHE_STALE_DAYS = 7
HUMAN_READABLE_VALIDATOR_SUMMARY_FILE = "validator_api_summary.txt"
CITIES_NEEDING_REVIEW_FILE = "cities_needing_region_review.csv"

LAMPORTS_PER_SOL = 1_000_000_000
_CITIES_DATABASE_CACHE = None

# Operator Configuration
OPERATOR_Z = "OperatorZ" # The "user" or primary operator
OPERATOR_A = "OperatorA" # A competitor operator

NUM_TOTAL_OPERATORS = 20  # Total number of operators in the simulation
NUM_TOP_OPERATORS = 5     # Number of operators considered "top tier" for link distribution

# Private Link Configuration
TOTAL_PRIVATE_LINKS_TARGET = 200
HIGH_BANDWIDTH_VALUE = 100000  # Representing 100G
STANDARD_BANDWIDTH_VALUE = 10000 # Representing 10G
HIGH_BANDWIDTH_RATIO_FOR_TOP_OPS = 0.80    # 80% of random links for top ops get high bandwidth

# Define OperatorZ's specific links here
# Use (City Name, Country Code) tuples for start and end points
OPERATOR_Z_LINKS = [
    {'start_city_tuple': ("Salt Lake City", "US"), 'end_city_tuple': ("Chicago", "US"), 'cost': 14, 'bandwidth': HIGH_BANDWIDTH_VALUE, 'shared_tag': "slc_chi_opz_link"},
    # Add more OperatorZ links here if needed, e.g.:
    # {'start_city_tuple': ("New York", "US"), 'end_city_tuple': ("London", "GB"), 'cost': 35, 'bandwidth': HIGH_BANDWIDTH_VALUE, 'shared_tag': "nyc_lon_opz_link"},
]

# Define OperatorA's specific links here
OPERATOR_A_LINKS = [
    {'start_city_tuple': ("Chicago", "US"), 'end_city_tuple': ("Salt Lake City", "US"), 'cost': 13, 'bandwidth': HIGH_BANDWIDTH_VALUE, 'shared_tag': "slc_chi_opz_link"},
]


# DoubleZero TestNet Link Data (using descriptive city names)
DZ_TESTNET_LINKS_RAW_DESCRIPTIVE = [
    {'cities': (('Singapore', 'SG'), ('Tokyo', 'JP')),       'latency_rtt_ms': 67.20, 'owner_pubkey': "66yfemxTAjCL686R4FFpGugx1myQ7X6m274MzWB82xBy"},
    {'cities': (('Amsterdam', 'NL'), ('London', 'GB')),       'latency_rtt_ms': 5.76,  'owner_pubkey': "66yfemxTAjCL686R4FFpGugx1myQ7X6m274MzWB82xBy"},
    {'cities': (('Frankfurt', 'DE'), ('Prague', 'CZ')),       'latency_rtt_ms': 7.01,  'owner_pubkey': "RoXFXFQAqBxYx6QZYG9AmGMWpSyr7xJPPqAy3FCafpv"},
    {'cities': (('New York', 'US'), ('London', 'GB')),        'latency_rtt_ms': 66.93, 'owner_pubkey': "66yfemxTAjCL686R4FFpGugx1myQ7X6m274MzWB82xBy"},
    {'cities': (('London', 'GB'), ('Singapore', 'SG')),       'latency_rtt_ms': 152.54,'owner_pubkey': "66yfemxTAjCL686R4FFpGugx1myQ7X6m274MzWB82xBy"},
    {'cities': (('Los Angeles', 'US'), ('New York', 'US')),   'latency_rtt_ms': 69.71, 'owner_pubkey': "66yfemxTAjCL686R4FFpGugx1myQ7X6m274MzWB82xBy"},
    {'cities': (('Tokyo', 'JP'), ('Los Angeles', 'US')),      'latency_rtt_ms': 98.71, 'owner_pubkey': "66yfemxTAjCL686R4FFpGugx1myQ7X6m274MzWB82xBy"},
    {'cities': (('London', 'GB'), ('Frankfurt', 'DE')),       'latency_rtt_ms': 11.09, 'owner_pubkey': "66yfemxTAjCL686R4FFpGugx1myQ7X6m274MzWB82xBy"},
]
DZ_TESTNET_BANDWIDTH = 10000 # Typically 10G

# EXISTING_CITIES_TEMPLATE:
# Primary lookup for preferred 3-letter codes and verified lat/lon for cities.
# Keys are normalized descriptive names (lowercase, underscore for space, e.g., "new_york_us") for matching.
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
    "atlanta_us":    {'code': "ATL", 'lat': 33.7838, 'lon': -84.4455},
    "denver_us":     {'code': "DEN", 'lat': 39.7392, 'lon': -104.9903},
    "phoenix_us":    {'code': "PHX", 'lat': 33.4475, 'lon': -112.0866},
    "vancouver_ca":  {'code': "YVR", 'lat': 49.2372, 'lon': -123.1207},
    "ogden_us":      {'code': "OGD", 'lat': 41.2627, 'lon': -111.9837},
    "newark_us":     {'code': "EWR", 'lat': 40.7265, 'lon': -74.1782},
    "pittsburgh_us": {'code': "PIT", 'lat': 40.4422, 'lon': -79.9927},
    "baton_rouge_us":{'code': "BTR", 'lat': 30.4485, 'lon': -91.1300},
    "piscataway_us": {'code': "PIC", 'lat': 40.5511, 'lon': -74.4606},
    "anchorage_us":  {'code': "ANC", 'lat': 61.2199, 'lon': -149.9077},
    "las_vegas_us":  {'code': "LAS", 'lat': 36.1379, 'lon': -115.3213},
    "elk_grove_village_us": {'code': "EGV", 'lat': 42.0048, 'lon': -87.9954},
    "santa_clara_us":{'code': "SJC", 'lat': 37.3931, 'lon': -121.9620},
    "san_jose_us":   {'code': "SJC", 'lat': 37.3388, 'lon': -121.8916},
    "mooresville_us":{'code': "MRN", 'lat': 35.5838, 'lon': -80.8640},
    "boston_us":     {'code': "BOS", 'lat': 42.3634, 'lon': -71.0713},
    "fort_worth_us": {'code': "FTW", 'lat': 32.7446, 'lon': -97.3842},
    "secaucus_us":   {'code': "SEC", 'lat': 40.7876, 'lon': -74.0600},
    "duluth_us":     {'code': "DLH", 'lat': 33.9837, 'lon': -84.1487},
    "charlotte_us":  {'code': "CLT", 'lat': 35.2369, 'lon': -80.8957},
    "mountain_ranch_us": {'code': "MRC", 'lat': 38.2275, 'lon': -120.5440},
    "kent_us":       {'code': "KNT", 'lat': 47.3798, 'lon': -122.2893},
    "austin_us":     {'code': "AUS", 'lat': 30.2625, 'lon': -97.7463},
    "staten_island_us": {'code': "SIY", 'lat': 40.6063, 'lon': -74.1774},
    "boardman_us":   {'code': "BDM", 'lat': 45.8401, 'lon': -119.7050},
    "draper_us":     {'code': "DRP", 'lat': 40.5247, 'lon': -111.8627 },
    "rockaway_us":   {'code': "RAW", 'lat': 40.9241, 'lon': -74.5140},
    "council_bluffs_us": {'code': "CBF", 'lat': 41.2619, 'lon': -95.8608},
    "binghamton_us": {'code': "BGM", 'lat': 42.1471, 'lon': -75.8816},
    "tampa_us":      {'code': "TPA", 'lat': 28.0109, 'lon': -82.4948},
    "columbia_us":   {'code': "COU", 'lat': 38.9517, 'lon': -92.3341},
    "akron_us":      {'code': "CAK", 'lat': 40.9162, 'lon': -81.4330 },
    "mexico_city_mx":{'code': "MEX", 'lat': 19.4324, 'lon': -99.1229},
    "queretaro_city_mx": {'code': "QRO", 'lat': 20.5737, 'lon': -100.2899},

    # Europe
    "frankfurt_de":  {'code': "FRA", 'lat': 50.1169, 'lon': 8.6837, 'country_code_override': 'DE'},
    "amsterdam_nl":  {'code': "AMS", 'lat': 52.3759, 'lon': 4.8975, 'country_code_override': 'NL'},
    "london_gb":     {'code': "LON", 'lat': 51.4964, 'lon': -0.1224, 'country_code_override': 'GB'},
    "city_of_london_gb":{'code': "LON", 'lat': 51.5164, 'lon': -0.0930, 'country_code_override': 'GB'},
    "paris_fr":      {'code': "PAR", 'lat': 48.8558, 'lon': 2.3494, 'country_code_override': 'FR'},
    "prague_cz":     {'code': "PRG", 'lat': 50.0883, 'lon': 14.4124, 'country_code_override': 'CZ'},
    "vilnius_lt":    {'code': "VNO", 'lat': 54.6872, 'lon': 25.2797, 'country_code_override': 'LT'},
    "luxembourg_lu": {'code': "LUX", 'lat': 49.7498, 'lon': 6.1661, 'country_code_override': 'LU'},
    "madrid_es":     {'code': "MAD", 'lat': 40.4153, 'lon': -3.6940, 'country_code_override': 'ES'},
    "stockholm_se":  {'code': "ARN", 'lat': 59.3287, 'lon': 18.0717, 'country_code_override': 'SE'},
    "helsinki_fi":   {'code': "HEL", 'lat': 60.1717, 'lon': 24.9349, 'country_code_override': 'FI'},
    "oslo_no":       {'code': "OSL", 'lat': 59.9133, 'lon': 10.7389, 'country_code_override': 'NO'},
    "warsaw_pl":     {'code': "WAW", 'lat': 52.2299, 'lon': 21.0093, 'country_code_override': 'PL'},
    "zurich_ch":     {'code': "ZRH", 'lat': 47.3643, 'lon': 8.5437, 'country_code_override': 'CH'},
    "dublin_ie":     {'code': "DUB", 'lat': 53.3382, 'lon': -6.2591, 'country_code_override': 'IE'},
    "berlin_de":     {'code': "BER", 'lat': 52.5200, 'lon': 13.4050, 'country_code_override': 'DE'},
    "gravelines_fr": {'code': "GVL", 'lat': 50.9830, 'lon': 2.1300, 'country_code_override': 'FR'},
    "fechenheim_de": {'code': "FEC", 'lat': 50.1210, 'lon': 8.7470, 'country_code_override': 'DE'},
    "strasbourg_fr": {'code': "SXB", 'lat': 48.5848, 'lon': 7.7419, 'country_code_override': 'FR'},
    "roubaix_fr":    {'code': "RBX", 'lat': 50.6974, 'lon': 3.1780, 'country_code_override': 'FR'},
    "remscheid_de":  {'code': "REM", 'lat': 51.1784, 'lon': 7.1601, 'country_code_override': 'DE'},
    "rotterdam_nl":  {'code': "RTM", 'lat': 51.9281, 'lon': 4.4220, 'country_code_override': 'NL'},
    "edinburgh_gb":  {'code': "EDI", 'lat': 55.9552, 'lon': -3.2000, 'country_code_override': 'GB'},
    "aubervilliers_fr": {'code': "AUB", 'lat': 48.9163, 'lon': 2.3869, 'country_code_override': 'FR'},
    "bratislava_sk": {'code': "BTS", 'lat': 48.1577, 'lon': 17.1474, 'country_code_override': 'SK'},
    "bucharest_ro":  {'code': "OTP", 'lat': 44.4152, 'lon': 26.1660, 'country_code_override': 'RO'},
    "cordoba_es":    {'code': "ODB", 'lat': 37.8994, 'lon': -4.7741, 'country_code_override': 'ES'},
    "cluj_napoca_ro":{'code': "CLJ", 'lat': 46.7656, 'lon': 23.5945, 'country_code_override': 'RO'},
    "hattersheim_de":{'code': "HAT", 'lat': 50.0845, 'lon': 8.4719, 'country_code_override': 'DE'},
    "moscow_ru":     {'code': "SVO", 'lat': 55.7386, 'lon': 37.6068, 'country_code_override': 'RU'},
    "fryazino_ru":   {'code': "FRY", 'lat': 55.9606, 'lon': 38.0456, 'country_code_override': 'RU'},
    "whitechapel_gb":{'code': "WCL", 'lat': 51.5128, 'lon': -0.0638, 'country_code_override': 'GB'},
    "offenbach_de":  {'code': "OFF", 'lat': 50.1093, 'lon': 8.7321, 'country_code_override': 'DE'},
    "riga_lv":       {'code': "RIX", 'lat': 56.9473, 'lon': 24.0979, 'country_code_override': 'LV'},
    "perm_ru":       {'code': "PEE", 'lat': 58.0047, 'lon': 56.2514, 'country_code_override': 'RU'},
    "kyiv_ua":       {'code': "KBP", 'lat': 50.4580, 'lon': 30.5303, 'country_code_override': 'UA'},
    "swinton_gb":    {'code': "SWN", 'lat': 53.4809, 'lon': -2.2374, 'country_code_override': 'GB'},
    "kemerovo_ru":   {'code': "KEJ", 'lat': 55.3299, 'lon': 86.0765, 'country_code_override': 'RU'},
    "spanga_se":     {'code': "SPA", 'lat': 59.3779, 'lon': 17.9155, 'country_code_override': 'SE'},
    "lisbon_pt":     {'code': "LIS", 'lat': 38.7219, 'lon': -9.1398, 'country_code_override': 'PT'},
    "vienna_at":     {'code': "VIE", 'lat': 48.1773, 'lon': 16.2456, 'country_code_override': 'AT'},
    "tower_hamlets_gb":{'code': "THM", 'lat': 51.5064, 'lon': -0.0200, 'country_code_override': 'GB'},
    "groningen_nl":  {'code': "GRQ", 'lat': 53.2222, 'lon': 6.5664, 'country_code_override': 'NL'},
    "leichlingen_de":{'code': "LEI", 'lat': 51.1060, 'lon': 7.0128, 'country_code_override': 'DE'},
    "worms_de":      {'code': "WOR", 'lat': 49.6357, 'lon': 8.3305, 'country_code_override': 'DE'},
    "lviv_ua":       {'code': "LWO", 'lat': 49.8390, 'lon': 24.0191, 'country_code_override': 'UA'},
    "rome_it":       {'code': "ROM", 'lat': 41.8904, 'lon': 12.5126, 'country_code_override': 'IT'},
    "espoo_fi":      {'code': "ESP", 'lat': 60.2050, 'lon': 24.6455, 'country_code_override': 'FI'},
    "chelyabinsk_ru":{'code': "CEK", 'lat': 55.1581, 'lon': 61.4313, 'country_code_override': 'RU'},
    "kursk_ru":      {'code': "URS", 'lat': 51.7280, 'lon': 36.1895, 'country_code_override': 'RU'},
    "sala_sk":       {'code': "SAL", 'lat': 48.1592, 'lon': 17.8834, 'country_code_override': 'SK'},
    "halfweg_nl":    {'code': "HFW", 'lat': 52.3862, 'lon': 4.7506, 'country_code_override': 'NL'},
    "falkenstein_de":{'code': "FAL", 'lat': 50.4777, 'lon': 12.3649, 'country_code_override': 'DE'},
    "nuremberg_de":  {'code': "NUR", 'lat': 49.4521, 'lon': 11.0767, 'country_code_override': 'DE'},
    "milan_it":      {'code': "MIL", 'lat': 45.4642, 'lon': 9.1900, 'country_code_override': 'IT'},
    "brussels_be":   {'code': "BRU", 'lat': 50.8503, 'lon': 4.3517, 'country_code_override': 'BE'},
    
    # Asia, Oceania, SA, Africa
    "guigang_cn":    {'code': "GUI", 'lat': 23.0964, 'lon': 109.6072, 'country_code_override': 'CN'}, 
    "madang_pg":     {'code': "MAG", 'lat': -5.2227, 'lon': 145.7947, 'country_code_override': 'PG'}, 
    "buenos_aires_ar":{'code': "EZE", 'lat': -34.6037, 'lon': -58.3816, 'country_code_override': 'AR'},
    "osasco_br":     {'code': "OSA", 'lat': -23.5312, 'lon': -46.7901, 'country_code_override': 'BR'}, 
    "jerusalem_il":  {'code': "JRS", 'lat': 31.7683, 'lon': 35.2137, 'country_code_override': 'IL'}, 
    "bogotá_co":     {'code': "BOG", 'lat': 4.6115, 'lon': -74.0833, 'country_code_override': 'CO'}, 
    "santiago_cl":   {'code': "SCL", 'lat': -33.4489, 'lon': -70.6693, 'country_code_override': 'CL'}, 
    "durbanville_za":{'code': "DRB", 'lat': -33.8409, 'lon': 18.6566, 'country_code_override': 'ZA'}, 
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
    "cayman_ky":     {'code': "GCM", 'lat': 19.3133, 'lon': -81.2546, 'country_code_override': 'KY'},
    "unknowncity_xx": {'code': "UNK", 'lat': 0.0, 'lon': 0.0, 'country_code_override': 'XX' }, 
}

# --- API Data Fetching and Caching (no changes) ---
def fetch_validator_data_from_api(api_key):
    if not api_key: print("ERROR: VALIDATORS_APP_API_KEY not found..."); return None
    headers = {"Token": api_key}; print(f"Fetching fresh data from {VALIDATORS_API_ENDPOINT}...")
    try:
        response = requests.get(VALIDATORS_API_ENDPOINT, headers=headers, params={'limit': 9999}, timeout=60) 
        response.raise_for_status(); print("Successfully fetched data from API.")
        return response.json()
    except requests.exceptions.RequestException as e: print(f"Error fetching data from API: {e}"); return None

def load_or_fetch_validator_data(force_refresh=False):
    cached_data = None
    if CACHE_FILE_PATH.exists() and not force_refresh:
        try:
            with open(CACHE_FILE_PATH, 'r', encoding='utf-8') as f: cached_data = json.load(f) 
            cache_timestamp = datetime.fromisoformat(cached_data['timestamp'])
            if datetime.now() - cache_timestamp > timedelta(days=CACHE_STALE_DAYS):
                print(f"Cache is older than {CACHE_STALE_DAYS} days.")
                if input("Fetch fresh data from API? (y/n): ").lower() != 'y':
                    print("Using stale cached data."); return cached_data['data']
                else: cached_data = None 
            else: print("Using recent cached validator data."); return cached_data['data']
        except Exception as e: print(f"Error reading cache: {e}. Fetching fresh."); cached_data = None
    if VALIDATORS_APP_API_KEY:
        fresh_data = fetch_validator_data_from_api(VALIDATORS_APP_API_KEY)
        if fresh_data:
            try:
                with open(CACHE_FILE_PATH, 'w', encoding='utf-8') as f: json.dump({'timestamp': datetime.now().isoformat(), 'data': fresh_data}, f) 
                print(f"Saved fresh API data to {CACHE_FILE_PATH}")
            except Exception as e: print(f"Error writing cache: {e}")
            return fresh_data
    else: print("No API key and no valid cache. Cannot fetch API data."); return None

# --- Function to save validator summary to file (no changes) ---
def save_validator_api_summary_to_file(api_data, filename=HUMAN_READABLE_VALIDATOR_SUMMARY_FILE):
    if not api_data: print("No API data provided for summary."); return
    print(f"Saving human-readable validator API summary to '{filename}'...")
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(f"--- Validator API Data Summary (Total: {len(api_data)} entries) ---\n")
            for i, validator in enumerate(api_data):
                name = validator.get("name", "N/A"); account = validator.get("account", "N/A")
                stake_lamports = validator.get("active_stake", 0)
                stake_sol = stake_lamports / LAMPORTS_PER_SOL if stake_lamports else 0.0
                dc_key = validator.get("data_center_key", "N/A")
                lat = validator.get("latitude", "N/A"); lon = validator.get("longitude", "N/A")
                ip = validator.get("ip", "N/A"); asn = validator.get("autonomous_system_number", "N/A")
                f.write(f"\nValidator #{i+1}:\n  Name: {name}\n  Account: {account}\n  Active Stake: {stake_sol:,.2f} SOL ({stake_lamports:,} Lamports)\n")
                f.write(f"  Data Center Key: {dc_key}\n  Location: Lat={lat}, Lon={lon}\n")
                if ip != "N/A": f.write(f"  IP: {ip}\n")
                if asn != "N/A": f.write(f"  ASN: {asn}\n")
            f.write("\n--- End of Validator API Data Summary ---")
        print(f"Successfully saved validator summary to '{filename}'")
        print(f"\n--- Console Validator API Data Summary (First 5 entries of {len(api_data)}) ---")
        for i, validator in enumerate(api_data[:5]):
             stake_lamports_console = validator.get('active_stake',0)
             stake_sol_console = stake_lamports_console / LAMPORTS_PER_SOL if stake_lamports_console else 0.0
             print(f"  Name: {validator.get('name', 'N/A')}, DC Key: {validator.get('data_center_key', 'N/A')}, Stake: {stake_sol_console:,.2f} SOL")
        if len(api_data) > 5: print("  ... (see full list in validator_api_summary.txt)")
    except Exception as e: print(f"Error writing validator summary to file: {e}")

# --- Data Processing & Definitions ---
parsing_issues_count = 0 

def parse_api_validator_data(api_data):
    global parsing_issues_count
    city_aggregates = {}
    if not api_data: print("No API data to parse."); return city_aggregates
    for validator in api_data: 
        dc_key = validator.get("data_center_key"); stake = validator.get("active_stake", 0) 
        lat_str = validator.get("latitude"); lon_str = validator.get("longitude")
        account_for_log = validator.get('account', 'Unknown Account')
        
        if not dc_key or not isinstance(dc_key, str) or dc_key.strip() == "0--Unknown" or not dc_key.strip():
            parsing_issues_count +=1; continue 
        
        country_code = "XX"; city_name_raw = "UnknownCity" 
        
        # Normalize dc_key: remove 'Europe/', 'America/', 'Asia/' prefixes from city part
        dc_key_normalized = dc_key
        for prefix in ["Europe/", "America/", "Asia/"]:
            if prefix in dc_key_normalized: # Check if prefix is a substring
                # Split carefully to handle cases like "ASN-Europe/City" or "Europe/City"
                if dc_key_normalized.startswith(prefix): # Prefix is at the beginning
                    city_name_raw = dc_key_normalized[len(prefix):]
                    # Try to infer country code from EXISTING_CITIES_TEMPLATE if city_name_raw is now just a city name
                    # This is a bit heuristic and might need refinement
                    for template_key, template_val in EXISTING_CITIES_TEMPLATE.items():
                        if city_name_raw.lower() in template_key.lower() and template_val.get('country_code_override'):
                            country_code = template_val['country_code_override']
                            break
                        elif city_name_raw.lower() in template_key.lower() and len(template_key.split('_')[-1]) == 2:
                            country_code = template_key.split('_')[-1].upper()
                            break
                    break # Found and processed prefix
                else: # Prefix is in the middle, e.g., ASN-PREFIX-CITY
                    parts = dc_key_normalized.split(prefix, 1)
                    if len(parts) > 1:
                        # Potentially ASN or CC-ASN before prefix
                        first_part_before_prefix = parts[0]
                        city_name_raw = parts[1]
                        # Try to get CC from first_part_before_prefix if it's like "ASN-CC-"
                        if '-' in first_part_before_prefix:
                            sub_parts = first_part_of_double.rsplit('-',1)
                            if len(sub_parts[-1])==2 and sub_parts[-1].isalpha():
                                country_code = sub_parts[-1].upper()
                        break
        else: # No "Europe/", "America/", "Asia/" prefix, proceed with normal parsing
            if "--" in dc_key_normalized: 
                parts_double_hyphen = dc_key_normalized.split('--', 1)
                first_part_of_double = parts_double_hyphen[0]
                if len(parts_double_hyphen) > 1:
                    city_name_raw = parts_double_hyphen[1] 
                    if not (first_part_of_double and first_part_of_double.isdigit()): 
                        print(f"Warning (parse_api): Unusual double-hyphen format (non-ASN prefix): {dc_key}. Validator: {account_for_log}")
                else: 
                     print(f"Warning (parse_api): Malformed double-hyphen dc_key: {dc_key}. Validator: {account_for_log}.")
                     parsing_issues_count +=1; continue 
            else: 
                parts = dc_key_normalized.split('-', 2)
                if len(parts) >= 3: 
                    if parts[0].isdigit(): country_code = parts[1]; city_name_raw = parts[2]
                    else: country_code = parts[0]; city_name_raw = "-".join(parts[1:])
                elif len(parts) == 2: country_code = parts[0]; city_name_raw = parts[1]
                elif len(parts) == 1 : 
                    if len(dc_key_normalized) > 2 and dc_key_normalized[:2].isalpha() and dc_key_normalized[:2].isupper() and not dc_key_normalized[2:].isdigit():
                        country_code = dc_key_normalized[:2]; city_name_raw = dc_key_normalized[2:]
                    else: city_name_raw = dc_key_normalized 
                else: 
                    print(f"Warning (parse_api): Could not parse data_center_key: {dc_key}. Validator: {account_for_log}.")
                    parsing_issues_count +=1; continue

        # Clean city_name_raw
        city_name_cleaned = city_name_raw.replace(" am Main", "").replace(" (Oder)", "").strip()
        city_name_cleaned = "".join(char for char in city_name_cleaned if char.isalnum() or char.isspace() or char == '-').strip() # Allow Alphanumeric, space, hyphen
        city_name_cleaned = re.sub(r'\s+', ' ', city_name_cleaned) # Consolidate multiple spaces
        
        if not city_name_cleaned: city_name_cleaned = "UnknownCity"; parsing_issues_count +=1
        
        # Validate country_code
        if not (country_code and len(country_code) == 2 and country_code.isalpha() and country_code.isupper()):
            if country_code != "XX": 
                parsing_issues_count +=1
            country_code = "XX" 
        if country_code == "EN": country_code = "GB" # Normalize
        
        city_key = f"{city_name_cleaned}, {country_code.upper()}" 
        if city_key not in city_aggregates:
            city_aggregates[city_key] = {
                'city_name': city_name_cleaned, 'country_code': country_code.upper(), 'stake': 0, 'population': 0, 
                'lat': float(lat_str) if lat_str and isinstance(lat_str, (str, float, int)) and re.match(r"^-?(\d+|\d+\.\d+|\.\d+)([eE][-+]?\d+)?$", str(lat_str)) else 0.0,
                'lon': float(lon_str) if lon_str and isinstance(lon_str, (str, float, int)) and re.match(r"^-?\d+(\.\d+)?$", str(lon_str)) else 0.0, 
                'raw_dc_keys': set()
            }
        city_aggregates[city_key]['stake'] += int(stake) if stake else 0 
        city_aggregates[city_key]['population'] += 1
        city_aggregates[city_key]['raw_dc_keys'].add(dc_key)
        if city_aggregates[city_key]['lat'] == 0.0 and lat_str and isinstance(lat_str, (str, float, int)) and re.match(r"^-?\d+(\.\d+)?$", str(lat_str)): 
            city_aggregates[city_key]['lat'] = float(lat_str)
        if city_aggregates[city_key]['lon'] == 0.0 and lon_str and isinstance(lon_str, (str, float, int)) and re.match(r"^-?\d+(\.\d+)?$", str(lon_str)): 
            city_aggregates[city_key]['lon'] = float(lon_str)
    return city_aggregates

def get_or_assign_code(city_name, country_code, used_codes_session):
    generated_code = None
    lat_from_template, lon_from_template = 0.0, 0.0 # Initialize lat/lon from template
    
    # Normalize API city name and country code for matching with template keys
    # Replace spaces and hyphens with underscores, convert to lowercase, strip leading/trailing underscores
    norm_api_city_name_key = re.sub(r'[\s-]+', '_', city_name.lower()).strip('_')
    norm_api_country_code_key = country_code.lower().strip()
    
    # 1. Try to match with EXISTING_CITIES_TEMPLATE
    api_lookup_key_normalized = f"{norm_api_city_name_key}_{norm_api_country_code_key}"
    template_data = EXISTING_CITIES_TEMPLATE.get(api_lookup_key_normalized)

    if not template_data: 
        # If direct match fails, iterate for more flexible matching
        for template_key_iter_raw, template_data_iter in EXISTING_CITIES_TEMPLATE.items():
            # Normalize template key for matching
            template_key_iter_norm = template_key_iter_raw.lower().replace(" ", "_").replace("-", "_").strip('_')
            
            parts = template_key_iter_norm.rsplit('_', 1)
            template_city_part = parts[0]
            template_cc_part = parts[1] if len(parts) > 1 else ""

            city_name_match = (norm_api_city_name == template_city_part or
                               (len(norm_api_city_name) >=3 and norm_api_city_name in template_city_part) or
                               (len(template_city_part) >=3 and template_city_part in norm_api_city_name))
            
            country_match_direct = norm_api_country_code == template_cc_part
            country_match_override = norm_api_country_code == template_data_iter.get('country_code_override','').lower()

            if city_name_match and (country_match_direct or (template_data_iter.get('country_code_override') and country_match_override)):
                candidate_code = template_data_iter.get('code')
                if candidate_code:
                    generated_code = candidate_code
                    lat_from_template = template_data_iter.get('lat', 0.0)
                    lon_from_template = template_data_iter.get('lon', 0.0)
                    # print(f"Debug: Matched API city '{city_name},{country_code}' to template '{template_key_iter_raw}' -> Using template code '{generated_code}'")
                    break 
    elif template_data: 
         generated_code = template_data.get('code')
         lat_from_template = template_data.get('lat', 0.0)
         lon_from_template = template_data.get('lon', 0.0)
            
    if not generated_code: # Fallback programmatic generation
        base = "".join(filter(str.isalpha, city_name.upper()))[:2] 
        if not base and city_name: base = "".join(filter(str.isalnum, city_name.upper()))[:2] 
        if not base: base = "XX" 
        
        cc_part = "".join(filter(str.isalpha, country_code.upper() if isinstance(country_code, str) else "X"))[:1] 
        if not cc_part: cc_part = "X"
        
        generated_code = (base + cc_part).upper() 
        generated_code = "".join(filter(str.isalpha, generated_code)) 
        if len(generated_code) > 3: generated_code = generated_code[:3]
        if len(generated_code) == 0: generated_code = "XXX"
        elif len(generated_code) == 1: generated_code += "XX"
        elif len(generated_code) == 2: generated_code += "X"
            
    final_code = generated_code if generated_code else "UNK" 
    counter = 0; original_final_code = final_code; alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    current_try_code = final_code
    while current_try_code in used_codes_session:
        counter +=1; 
        if counter <= 26: 
            idx_to_change = (alphabet.find(original_final_code[-1] if original_final_code and original_final_code[-1].isalpha() else 'A') + counter -1) % 26 
            final_code = (original_final_code[:2] if len(original_final_code) >=2 else "XX") + alphabet[idx_to_change]
        elif counter <= 26*2: 
            idx_to_change = (alphabet.find(original_final_code[1] if original_final_code and len(original_final_code)>1 and original_final_code[1].isalpha() else 'A') + (counter-27)) % 26
            final_code = (original_final_code[0] if original_final_code else "X") + alphabet[idx_to_change] + (original_final_code[2] if original_final_code and len(original_final_code)>2 else "X")
        elif counter <= 26*3: 
            idx_to_change = (alphabet.find(original_final_code[0] if original_final_code and original_final_code[0].isalpha() else 'A') + (counter-53)) % 26
            final_code = alphabet[idx_to_change] + (original_final_code[1:] if original_final_code and len(original_final_code)>1 else "XX")
        else: 
            final_code = "".join(random.sample(alphabet, 3))
            if final_code in used_codes_session: final_code = "".join(random.sample(alphabet, 3)) 
            if final_code in used_codes_session: 
                print(f"CRITICAL: Unique alpha code failed for {original_final_code} from {city_name},{country_code}. Using ERR.")
                return "ERR", lat_from_template, lon_from_template 
        final_code = "".join(filter(str.isalpha, final_code.upper()))[:3]
        if len(final_code) < 3: final_code = (final_code + "XXX")[:3] 
        current_try_code = final_code
        if counter > 100: 
             print(f"CRITICAL: Exhausted attempts for {original_final_code}. Using FLX as last resort.")
             final_code = "FLX"; 
             if final_code in used_codes_session: return "ERR", lat_from_template, lon_from_template
             break
    return final_code, lat_from_template, lon_from_template


def initialize_cities_database(api_parsed_data): 
    final_db = {}; used_codes_for_session = set() 
    if not api_parsed_data:
        print("Warning: API parsed data is empty for initialize_cities_database.")
        return final_db

    for city_key_desc, data_from_api in api_parsed_data.items():
        city_name = data_from_api['city_name']; country_code = data_from_api['country_code']
        
        # Attempt to correct country_code if it's "XX" using EXISTING_CITIES_TEMPLATE
        if country_code == "XX" and city_name != "UnknownCity":
            normalized_city_name_for_template = city_name.lower().replace(" ", "_").replace("-","_")
            for template_key, template_val in EXISTING_CITIES_TEMPLATE.items():
                template_city_part_normalized = template_key.lower().replace(" ", "_").replace("-", "_").rsplit('_',1)[0]
                
                if normalized_city_name_for_template == template_city_part_normalized and template_val.get('country_code_override'):
                    inferred_cc = template_val['country_code_override']
                    country_code = inferred_cc.upper() 
                    city_key_desc = f"{city_name}, {country_code}" 
                    data_from_api['country_code'] = country_code 
                    break 

        assigned_code, template_lat, template_lon = get_or_assign_code(city_name, country_code, used_codes_for_session) 
        
        if assigned_code == "ERR": 
            print(f"Skipping city {city_name}, {country_code} due to code generation error.")
            continue
        used_codes_for_session.add(assigned_code) 
        
        lat = data_from_api.get('lat', 0.0)
        lon = data_from_api.get('lon', 0.0)

        # If API didn't provide lat/lon, try to get it from EXISTING_CITIES_TEMPLATE using the *assigned_code*
        if (lat == 0.0 and lon == 0.0) and assigned_code not in ["UNK", "ERR"]:
            for _desc, template_item in EXISTING_CITIES_TEMPLATE.items(): 
                if template_item.get('code') == assigned_code: 
                    if template_item.get('lat', 0.0) != 0.0 or template_item.get('lon', 0.0) != 0.0:
                        lat = template_item.get('lat', 0.0)
                        lon = template_item.get('lon', 0.0)
                    break 
        
        if (lat == 0.0 and lon == 0.0) and (assigned_code not in ["UNK", "ERR"]):
             print(f"ACTION NEEDED: City '{city_name}, {country_code}' (Code: {assigned_code}) has NO Lat/Lon from API or Template. Using (0,0).")
        
        final_db[assigned_code] = {
            'descriptive_name': city_key_desc, 
            'lat': lat, 'lon': lon, 
            'stake': data_from_api['stake'], 'population': data_from_api['population'],
            'raw_dc_keys': list(data_from_api.get('raw_dc_keys', [])) 
        }
    print(f"Initialized CITIES_DATABASE with {len(final_db)} cities from API data.")
    return final_db

# --- Main Initialization Block ---
CITIES_DATABASE = {}
city_codes = []
api_data_content = load_or_fetch_validator_data() 
if api_data_content:
    save_validator_api_summary_to_file(api_data_content) 
    parsed_api_cities = parse_api_validator_data(api_data_content)
    CITIES_DATABASE = initialize_cities_database(parsed_api_cities) 
else: print("CRITICAL ERROR: Could not obtain validator data. CITIES_DATABASE will be empty.")
city_codes = list(CITIES_DATABASE.keys()) if CITIES_DATABASE else []


# --- Haversine, Latency Constants, Region Mapping ---
def haversine(lat1, lon1, lat2, lon2): 
    R = 3958.8; dLat = math.radians(lat2 - lat1); dLon = math.radians(lon2 - lon1)
    lat1 = math.radians(lat1); lat2 = math.radians(lat2)
    a = math.sin(dLat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dLon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a)); distance = R * c
    return distance
LATENCY_PER_MILE_CONTINENTAL = 0.018; LATENCY_PER_MILE_INTERCONTINENTAL_OVERLAND = 0.020 
BASE_LATENCY_CONTINENTAL = 2; BASE_LATENCY_INTERCONTINENTAL = 15 
MAJOR_REGIONS = { 
    'NA': ["ASH", "NYC", "CHI", "DAL", "LAX", "SEA", "MIA", "SLC", "YYZ", "YUL", "ATL", "DEN", "PHX", "YVR", "EWR", "PIT", "BTR", "PIC", "BDM", "OGD", "ANC", "LAS", "MRN", "BGM", "TPA", "BOS", "CBF", "MRC", "KNT", "AUS", "SIY", "FTW", "SEC", "SJC", "EGV", "COU", "CAK", "QRO", "RAW"], 
    'EU': ["FRA", "AMS", "LON", "PAR", "MAD", "NUR", "WAW", "ZRH", "FAL", "DUB", "VIE", "ARN", "HEL", "MIL", "BRU", "OSL", "VNO", "LUX", "SXB", "FEC", "BER", "OFF", "GVL", "RBX", "VDZ", "BTS", "SWN", "PRG", "GRQ", "SVO", "HAT", "CLJ", "WOR", "SAL", "CVT", "RIX", "LEI", "OTP", "LIS", "MAN", "ESP", "CEK", "PEE", "KEJ", "KBP", "URS", "REM", "FRY", "ROM", "VIS", "WCL", "SPA", "COG", "THM", "AUB"], 
    'AS': ["TYO", "SIN", "SEL", "HKG", "BOM", "DXB", "BKK", "TPE", "JKT", "DEL", "JRS", "GUI"], 
    'OC': ["SYD", "MAG"], 'SA': ["GRU", "EZE", "LIM", "SCL", "BOG", "OSA"], 'AF': ["JNB", "NBO", "LOS", "DRB"], 
    'KY': ["GCM"], 'SC': ["SEZ"], 'AL': ["TIA"], 'SK': ["SAL"], 'LI': ["VDZ"], 
    'UNKNOWN_REGION_TEMP': [] 
}
def get_region(city_code, current_cities_db, current_major_regions): 
    if not current_cities_db or city_code not in current_cities_db: return "UNKNOWN" 
    for region, codes_in_region in current_major_regions.items():
        if city_code in codes_in_region: return region
    city_data = current_cities_db.get(city_code)
    if city_data: 
        cc = city_data.get('country_code','').upper()
        if cc in ["US", "CA", "MX"]: return 'NA'
        if cc in ["GB", "DE", "FR", "NL", "ES", "PL", "CH", "IE", "AT", "SE", "FI", "IT", "BE", "NO", "LT", "LU", "CZ", "PT", "SK", "AL", "RO", "LV", "RU", "LI"]: return 'EU'
        if cc in ["JP", "SG", "HK", "KR", "IN", "AE", "TH", "TW", "ID", "IL", "CN"]: return 'AS'
        if cc in ["AU", "PG"]: return 'OC'
        if cc in ["BR", "AR", "PE", "CL", "CO"]: return 'SA'
        if cc in ["ZA", "KE", "NG"]: return 'AF'
    
    # Only add to UNKNOWN_REGION_TEMP if not already there to avoid duplicates during multiple calls
    # This list is for reporting purposes.
    if city_code not in current_major_regions.get('UNKNOWN_REGION_TEMP', []):
         current_major_regions.get('UNKNOWN_REGION_TEMP', []).append(city_code) 
    return "UNKNOWN" 

# --- Public Link Generation ---
new_public_links_data = []
if city_codes: 
    for i in range(len(city_codes)):
        for j in range(i + 1, len(city_codes)):
            c1_code = city_codes[i]; c2_code = city_codes[j]
            if not (c1_code and c2_code) or c1_code == c2_code or c1_code == "UNK" or c2_code == "UNK" or c1_code == "ERR" or c2_code == "ERR": continue
            estimated_cost = -1; estimation_note = ""; distance_miles = -1
            c1_data = CITIES_DATABASE.get(c1_code); c2_data = CITIES_DATABASE.get(c2_code)
            if c1_data and c2_data and c1_data.get('lat',0.0) != 0.0 and c1_data.get('lon',0.0) != 0.0 and \
               c2_data.get('lat',0.0) != 0.0 and c2_data.get('lon',0.0) != 0.0 :
                distance_miles = haversine(c1_data['lat'], c1_data['lon'], c2_data['lat'], c2_data['lon'])
                r1 = get_region(c1_code, CITIES_DATABASE, MAJOR_REGIONS); r2 = get_region(c2_code, CITIES_DATABASE, MAJOR_REGIONS)
                if r1 and r2 and r1 != r2 and r1 != "UNKNOWN" and r2 != "UNKNOWN": 
                    estimated_cost = BASE_LATENCY_INTERCONTINENTAL + distance_miles * LATENCY_PER_MILE_INTERCONTINENTAL_OVERLAND
                    estimation_note = f"Est. InterCont ({r1}-{r2}): ~{distance_miles:.0f}mi"
                elif r1 and r2 and r1 == r2 and r1 != "UNKNOWN": 
                    estimated_cost = BASE_LATENCY_CONTINENTAL + distance_miles * LATENCY_PER_MILE_CONTINENTAL
                    estimation_note = f"Est. IntraCont ({r1}): ~{distance_miles:.0f}mi"
                else: 
                    estimated_cost = BASE_LATENCY_INTERCONTINENTAL + distance_miles * LATENCY_PER_MILE_INTERCONTINENTAL_OVERLAND 
                    estimation_note = f"Est. Default (region {r1}/{r2} unknown): ~{distance_miles:.0f}mi"
                estimated_cost = max(1, int(round(estimated_cost)))
            else:
                estimated_cost = 150 if c1_code != c2_code else 0 
                estimation_note = f"Fallback (Lat/Lon missing for {c1_code} or {c2_code})" if c1_code != c2_code else "Self-link"
            capacity_abstract = 1000 + int(distance_miles/10) if distance_miles > 0 else 1000
            if estimated_cost > 0 : 
                 new_public_links_data.append( ((c1_code, c2_code), estimated_cost, capacity_abstract, estimation_note) )

# Define raw_demand_definitions before it's used in prioritized_routes or participant data generation
raw_demand_definitions = [ 
    {'name': 'P_US_EU',       'source_desc': ("New York", "US"), 'destination_desc': ("Frankfurt", "DE"), 'base_traffic_weight': 20, 'value': 500, 'stake_influence': 0.7},
    {'name': 'P_ASIA_US',     'source_desc': ("Tokyo", "JP"), 'destination_desc': ("Los Angeles", "US"), 'base_traffic_weight': 15, 'value': 400, 'stake_influence': 0.8},
    {'name': 'P_EU_Regional', 'source_desc': ("Paris", "FR"), 'destination_desc': ("Amsterdam", "NL"), 'base_traffic_weight': 10, 'value': 150, 'stake_influence': 0.5},
    {'name': 'P_US_Internal', 'source_desc': ("Chicago", "US"), 'destination_desc': ("Dallas", "US"), 'base_traffic_weight': 12, 'value': 200, 'stake_influence': 0.6},
    {'name': 'P_EU_Nordic',   'source_desc': ("Stockholm", "SE"), 'destination_desc': ("Helsinki", "FI"), 'base_traffic_weight': 5,  'value': 90,  'stake_influence': 0.4},
    {'name': 'P_SIN_HK',      'source_desc': ("Singapore", "SG"), 'destination_desc': ("Hong Kong", "HK"), 'base_traffic_weight': 18, 'value': 300, 'stake_influence': 0.7},
    {'name': 'P_SLC_CHI',     'source_desc': ("Salt Lake City", "US"), 'destination_desc': ("Chicago", "US"), 'base_traffic_weight': 8,  'value': 180, 'stake_influence': 0.9},
    {'name': 'P_MajorStakeSource', 'source_desc': ("Frankfurt", "DE"), 'destination_desc': ("New York", "US"), 'base_traffic_weight': 25, 'value': 550, 'stake_influence': 1.0},
    {'name': 'P_AnotherStakeRoute', 'source_desc': ("Amsterdam", "NL"), 'destination_desc': ("London", "GB"), 'base_traffic_weight': 20, 'value': 250, 'stake_influence': 0.8},
    {'name': 'P_DZ_SIN_TYO', 'source_desc': ("Singapore", "SG"), 'destination_desc': ("Tokyo", "JP"), 'base_traffic_weight': 20, 'value': 400, 'stake_influence': 0.8},
    {'name': 'P_DZ_AMS_LON', 'source_desc': ("Amsterdam", "NL"), 'destination_desc': ("London", "GB"), 'base_traffic_weight': 15, 'value': 300, 'stake_influence': 0.7},
    {'name': 'P_DZ_FRA_PRG', 'source_desc': ("Frankfurt", "DE"), 'destination_desc': ("Prague", "CZ"), 'base_traffic_weight': 10, 'value': 200, 'stake_influence': 0.6},
    # Expanded demand pairs
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
    {'name': 'P_NYC_LAX', 'source_desc': ("New York", "US"), 'destination_desc': ("Los Angeles", "US"), 'base_traffic_weight': 25, 'value': 450, 'stake_influence': 0.7},
    {'name': 'P_LON_TYO', 'source_desc': ("London", "GB"), 'destination_desc': ("Tokyo", "JP"), 'base_traffic_weight': 15, 'value': 600, 'stake_influence': 0.6},
    {'name': 'P_FRA_SIN', 'source_desc': ("Frankfurt", "DE"), 'destination_desc': ("Singapore", "SG"), 'base_traffic_weight': 18, 'value': 500, 'stake_influence': 0.65},
    {'name': 'P_CHI_AMS', 'source_desc': ("Chicago", "US"), 'destination_desc': ("Amsterdam", "NL"), 'base_traffic_weight': 10, 'value': 350, 'stake_influence': 0.5},
    {'name': 'P_SYD_LAX', 'source_desc': ("Sydney", "AU"), 'destination_desc': ("Los Angeles", "US"), 'base_traffic_weight': 12, 'value': 550, 'stake_influence': 0.55},
    {'name': 'P_HKG_LON', 'source_desc': ("Hong Kong", "HK"), 'destination_desc': ("London", "GB"), 'base_traffic_weight': 16, 'value': 650, 'stake_influence': 0.6},
    {'name': 'P_MIA_GRU', 'source_desc': ("Miami", "US"), 'destination_desc': ("Sao Paulo", "BR"), 'base_traffic_weight': 14, 'value': 200, 'stake_influence': 0.7},
    {'name': 'P_SEA_TYO', 'source_desc': ("Seattle", "US"), 'destination_desc': ("Tokyo", "JP"), 'base_traffic_weight': 10, 'value': 450, 'stake_influence': 0.5},
    {'name': 'P_PAR_NYC', 'source_desc': ("Paris", "FR"), 'destination_desc': ("New York", "US"), 'base_traffic_weight': 18, 'value': 520, 'stake_influence': 0.65},
    {'name': 'P_AMS_CHI', 'source_desc': ("Amsterdam", "NL"), 'destination_desc': ("Chicago", "US"), 'base_traffic_weight': 10, 'value': 330, 'stake_influence': 0.5},
    {'name': 'P_WAW_NYC', 'source_desc': ("Warsaw", "PL"), 'destination_desc': ("New York", "US"), 'base_traffic_weight': 8, 'value': 480, 'stake_influence': 0.4},
    {'name': 'P_DUB_BOS', 'source_desc': ("Dublin", "IE"), 'destination_desc': ("Boston", "US"), 'base_traffic_weight': 7, 'value': 300, 'stake_influence': 0.4},
    {'name': 'P_BER_SFO', 'source_desc': ("Berlin", "DE"), 'destination_desc': ("Los Angeles", "US"), 'base_traffic_weight': 10, 'value': 580, 'stake_influence': 0.5}, 
    {'name': 'P_MAD_MIA', 'source_desc': ("Madrid", "ES"), 'destination_desc': ("Miami", "US"), 'base_traffic_weight': 12, 'value': 400, 'stake_influence': 0.55},
    {'name': 'P_ZRH_SIN', 'source_desc': ("Zurich", "CH"), 'destination_desc': ("Singapore", "SG"), 'base_traffic_weight': 9, 'value': 550, 'stake_influence': 0.4},
    {'name': 'P_ATL_LON', 'source_desc': ("Atlanta", "US"), 'destination_desc': ("London", "GB"), 'base_traffic_weight': 14, 'value': 480, 'stake_influence': 0.6},
    {'name': 'P_DEN_FRA', 'source_desc': ("Denver", "US"), 'destination_desc': ("Frankfurt", "DE"), 'base_traffic_weight': 10, 'value': 500, 'stake_influence': 0.5},
    {'name': 'P_PHX_TYO', 'source_desc': ("Phoenix", "US"), 'destination_desc': ("Tokyo", "JP"), 'base_traffic_weight': 8, 'value': 520, 'stake_influence': 0.45},
    {'name': 'P_YVR_HKG', 'source_desc': ("Vancouver", "CA"), 'destination_desc': ("Hong Kong", "HK"), 'base_traffic_weight': 12, 'value': 480, 'stake_influence': 0.55},
    {'name': 'P_YYZ_PAR', 'source_desc': ("Toronto", "CA"), 'destination_desc': ("Paris", "FR"), 'base_traffic_weight': 10, 'value': 450, 'stake_influence': 0.5},
]

# --- DoubleZero TestNet Link Data & Operator Definitions ---
DZ_TESTNET_LINKS_RAW_DESCRIPTIVE = [
    {'cities': (('Singapore', 'SG'), ('Tokyo', 'JP')),       'latency_rtt_ms': 67.20, 'owner_pubkey': "66yfemxTAjCL686R4FFpGugx1myQ7X6m274MzWB82xBy"},
    {'cities': (('Amsterdam', 'NL'), ('London', 'GB')),       'latency_rtt_ms': 5.76,  'owner_pubkey': "66yfemxTAjCL686R4FFpGugx1myQ7X6m274MzWB82xBy"},
    {'cities': (('Frankfurt', 'DE'), ('Prague', 'CZ')),       'latency_rtt_ms': 7.01,  'owner_pubkey': "RoXFXFQAqBxYx6QZYG9AmGMWpSyr7xJPPqAy3FCafpv"},
    {'cities': (('New York', 'US'), ('London', 'GB')),        'latency_rtt_ms': 66.93, 'owner_pubkey': "66yfemxTAjCL686R4FFpGugx1myQ7X6m274MzWB82xBy"},
    {'cities': (('London', 'GB'), ('Singapore', 'SG')),       'latency_rtt_ms': 152.54,'owner_pubkey': "66yfemxTAjCL686R4FFpGugx1myQ7X6m274MzWB82xBy"},
    {'cities': (('Los Angeles', 'US'), ('New York', 'US')),   'latency_rtt_ms': 69.71, 'owner_pubkey': "66yfemxTAjCL686R4FFpGugx1myQ7X6m274MzWB82xBy"},
    {'cities': (('Tokyo', 'JP'), ('Los Angeles', 'US')),      'latency_rtt_ms': 98.71, 'owner_pubkey': "66yfemxTAjCL686R4FFpGugx1myQ7X6m274MzWB82xBy"},
    {'cities': (('London', 'GB'), ('Frankfurt', 'DE')),       'latency_rtt_ms': 11.09, 'owner_pubkey': "66yfemxTAjCL686R4FFpGugx1myQ7X6m274MzWB82xBy"},
]
DZ_TESTNET_BANDWIDTH = 10000 
OPERATOR_Z = "OperatorZ"; NUM_TOTAL_OPERATORS = 20; NUM_TOP_OPERATORS = 5 
TOTAL_PRIVATE_LINKS_TARGET = 200 

descriptive_name_to_code_map = {data['descriptive_name']: code for code, data in CITIES_DATABASE.items() if CITIES_DATABASE}

dz_operator_map = {}
for link_info in DZ_TESTNET_LINKS_RAW_DESCRIPTIVE: 
    pubkey = link_info['owner_pubkey']
    if pubkey not in dz_operator_map: dz_operator_map[pubkey] = f"DZ_Op_{pubkey[:4]}"
all_operator_names_base = [OPERATOR_Z] + [f"Contributor{i}" for i in range(1, NUM_TOTAL_OPERATORS)] 
unique_dz_ops = list(dz_operator_map.values())
all_operator_names = [OPERATOR_Z] 
for dz_op in unique_dz_ops:
    if dz_op not in all_operator_names: all_operator_names.append(dz_op)
contributor_idx = 1
while len(all_operator_names) < NUM_TOTAL_OPERATORS:
    contributor_name = f"Contributor{contributor_idx}"
    if contributor_name not in all_operator_names: all_operator_names.append(contributor_name)
    contributor_idx += 1;       
    if contributor_idx > (NUM_TOTAL_OPERATORS * 2): break 
all_operator_names = all_operator_names[:NUM_TOTAL_OPERATORS] 

top_operators = [OPERATOR_Z] 
for i in range(1, NUM_TOP_OPERATORS): 
    contributor_candidate = f"Contributor{i}"
    if contributor_candidate in all_operator_names and contributor_candidate not in top_operators:
        top_operators.append(contributor_candidate)
    if len(top_operators) == NUM_TOP_OPERATORS: break
idx = 0 
while len(top_operators) < NUM_TOP_OPERATORS and idx < len(all_operator_names):
    if all_operator_names[idx] not in top_operators:
        top_operators.append(all_operator_names[idx])
    idx +=1
other_operators = [name for name in all_operator_names if name not in top_operators]
new_private_links_data = []
public_link_cost_lookup = {} 

# Populate public_link_cost_lookup from the globally generated new_public_links_data
# This should happen AFTER new_public_links_data is generated by the public link generation logic
for (city_pair_tuple, cost, _capacity, _note) in new_public_links_data: 
    start_city, end_city = city_pair_tuple
    public_link_cost_lookup[(start_city, end_city)] = cost
    public_link_cost_lookup[(end_city, start_city)] = cost

for link_info in DZ_TESTNET_LINKS_RAW_DESCRIPTIVE:
    (city1_name, city1_cc), (city2_name, city2_cc) = link_info['cities']
    desc_name1 = f"{city1_name}, {city1_cc.upper()}"; desc_name2 = f"{city2_name}, {city2_cc.upper()}" 
    start_city_code = descriptive_name_to_code_map.get(desc_name1); 
    end_city_code = descriptive_name_to_code_map.get(desc_name2)
    if start_city_code and end_city_code:
        one_way_cost = max(1, int(round(link_info['latency_rtt_ms'] / 2.0)))
        operator_name = dz_operator_map[link_info['owner_pubkey']]
        new_private_links_data.append({'operator': operator_name, 'start': start_city_code, 'end': end_city_code,'cost': one_way_cost, 'bandwidth': DZ_TESTNET_BANDWIDTH,'shared_tag': f"dz_{start_city_code}_{end_city_code}"})
    else: print(f"Warning: Could not map DZ TestNet cities '{desc_name1}' (->{start_city_code}) or '{desc_name2}' (->{end_city_code}) to 3L codes. Skipping link.")

chi_template_key_norm = "chicago_us" 
slc_template_key_norm = "salt_lake_city_us"     

chi_code_final = EXISTING_CITIES_TEMPLATE.get(chi_template_key_norm, {}).get('code')
slc_code_final = EXISTING_CITIES_TEMPLATE.get(slc_template_key_norm, {}).get('code')

if slc_code_final and chi_code_final and slc_code_final in CITIES_DATABASE and chi_code_final in CITIES_DATABASE :
    new_private_links_data.append({'operator': OPERATOR_Z, 'start': slc_code_final, 'end': chi_code_final, 'cost': 14, 'bandwidth': HIGH_BANDWIDTH_VALUE, 'shared_tag': "slc_chi_link"}) 
    contributor1_name = "Contributor1" 
    if contributor1_name in top_operators and contributor1_name != OPERATOR_Z: 
         new_private_links_data.append({'operator': contributor1_name, 'start': slc_code_final, 'end': chi_code_final, 'cost': 13, 'bandwidth': 150, 'shared_tag': "slc_chi_link"})
    elif top_operators and len(top_operators) > 1 and top_operators[1] != OPERATOR_Z and top_operators[1] in all_operator_names : 
         new_private_links_data.append({'operator': top_operators[1], 'start': slc_code_final, 'end': chi_code_final, 'cost': 13, 'bandwidth': 150, 'shared_tag': "slc_chi_link"})
else: 
    print(f"Warning: CHI ('{chi_code_final}') or SLC ('{slc_code_final}') not found in CITIES_DATABASE. OperatorZ/C1 links not added.")


num_fixed_links = len(new_private_links_data)
random_links_to_generate = max(0, TOTAL_PRIVATE_LINKS_TARGET - num_fixed_links)

num_random_links_for_top_ops = int(random_links_to_generate * 0.80) 
num_random_links_for_other_ops = random_links_to_generate - num_random_links_for_top_ops


if random_links_to_generate > 0 and city_codes and len(city_codes) >= 2:
    prioritized_routes = []
    if CITIES_DATABASE and descriptive_name_to_code_map : 
        temp_demand_pairs = []
        for entry in raw_demand_definitions: 
            (source_city_name, source_cc) = entry['source_desc']
            (dest_city_name, dest_cc) = entry['destination_desc']
            source_desc_key = f"{source_city_name}, {source_cc.upper()}"
            dest_desc_key = f"{dest_city_name}, {dest_cc.upper()}"
            
            source_code = descriptive_name_to_code_map.get(source_desc_key)
            dest_code = descriptive_name_to_code_map.get(dest_desc_key)

            if source_code and dest_code and source_code in CITIES_DATABASE and dest_code in CITIES_DATABASE:
                s_stake = CITIES_DATABASE[source_code].get('stake', 0)
                d_stake = CITIES_DATABASE[dest_code].get('stake', 0)
                temp_demand_pairs.append(((source_code, dest_code), s_stake + d_stake))
        prioritized_routes = [pair for pair, score in sorted(temp_demand_pairs, key=lambda x: x[1], reverse=True)]

    for i in range(num_random_links_for_top_ops): 
        if not top_operators: break 
        op = random.choice(top_operators)
        c1, c2 = None, None
        if prioritized_routes and i < len(prioritized_routes): 
            c1, c2 = prioritized_routes[i % len(prioritized_routes)]
        else: 
            try: c1, c2 = random.sample(city_codes, 2)
            except ValueError: print(f"Warning: Not enough cities ({len(city_codes)}) to sample for top op private links."); break
        if not (c1 and c2) or c1 == c2 or c1 == "UNK" or c2 == "UNK" or c1 == "ERR" or c2 == "ERR": continue

        public_cost = public_link_cost_lookup.get((c1, c2), public_link_cost_lookup.get((c2,c1),100)) 
        improvement = random.uniform(0.03, 0.20)
        private_cost = max(1, int(round(public_cost * (1 - improvement))))
        bandwidth = HIGH_BANDWIDTH_VALUE if random.random() < HIGH_BANDWIDTH_RATIO_FOR_TOP_OPS else STANDARD_BANDWIDTH_VALUE
        new_private_links_data.append({'operator': op, 'start': c1, 'end': c2, 'cost': private_cost, 'bandwidth': bandwidth, 'shared_tag': None})
    
    if other_operators and num_random_links_for_other_ops > 0:
        ops_to_get_links = []
        if len(other_operators) > 0:
            for _ in range(num_random_links_for_other_ops):
                ops_to_get_links.append(random.choice(other_operators)) 
        
        for i_other_op, op in enumerate(ops_to_get_links): 
            c1, c2 = None, None
            if prioritized_routes and len(prioritized_routes) > 0: # Check if prioritized_routes is not empty
                route_idx = (i_other_op + num_random_links_for_top_ops) % len(prioritized_routes) 
                c1, c2 = prioritized_routes[route_idx]
            else:
                try: c1, c2 = random.sample(city_codes, 2)
                except ValueError: print(f"Warning: Not enough cities ({len(city_codes)}) to sample for other op private links."); break
            if not (c1 and c2) or c1 == c2 or c1 == "UNK" or c2 == "UNK" or c1 == "ERR" or c2 == "ERR": continue

            public_cost = public_link_cost_lookup.get((c1,c2), public_link_cost_lookup.get((c2,c1),100))
            improvement = random.uniform(0.03, 0.20)
            private_cost = max(1, int(round(public_cost * (1 - improvement))))
            bandwidth = STANDARD_BANDWIDTH_VALUE 
            new_private_links_data.append({'operator': op, 'start': c1, 'end': c2, 'cost': private_cost, 'bandwidth': bandwidth, 'shared_tag': None})


# --- Participants Data (Demand) ---
STAKE_TO_DEMAND_FACTOR = 0.000005 
MIN_DEMAND_PER_ROUTE = 1 
# raw_demand_definitions is now defined earlier
new_participants_data = []
max_observed_stake = 1.0 
if CITIES_DATABASE: 
    all_stakes = [data.get('stake',0) for data in CITIES_DATABASE.values() if data.get('stake',0) > 0]
    if all_stakes: max_observed_stake = max(all_stakes) 
if max_observed_stake == 0: max_observed_stake = 30000000 # Fallback if no stake data
for entry in raw_demand_definitions: 
    (source_city_name, source_cc) = entry['source_desc']
    (dest_city_name, dest_cc) = entry['destination_desc']
    source_desc_key = f"{source_city_name}, {source_cc.upper()}"
    dest_desc_key = f"{dest_city_name}, {dest_cc.upper()}"
    
    source_code = descriptive_name_to_code_map.get(source_desc_key)
    dest_code = descriptive_name_to_code_map.get(dest_desc_key)

    if source_code and dest_code and source_code in CITIES_DATABASE and dest_code in CITIES_DATABASE:
        source_city_data = CITIES_DATABASE[source_code]
        source_stake = source_city_data.get('stake', 0) 
        stake_multiplier_effect = (source_stake / max_observed_stake) * 10 * entry['stake_influence'] if max_observed_stake > 0 else 0
        calculated_demand_volume = entry['base_traffic_weight'] * (1 + stake_multiplier_effect)
        calculated_demand_volume = max(MIN_DEMAND_PER_ROUTE, int(round(calculated_demand_volume)))
        new_participants_data.append({
            'name': entry['name'], 'source': source_code, 'destination': dest_code,
            'demand': calculated_demand_volume, 'value': entry['value']
        })
    else: print(f"Warning: Demand pair (Source: {source_desc_key} -> {source_code}, Dest: {dest_desc_key} -> {dest_code}) could not be fully mapped. Skipping.")

# --- Helper to convert 3-letter city codes to switch names ---
def to_switch_name(city_3_letter_code): 
    if isinstance(city_3_letter_code, str) and len(city_3_letter_code) == 3 and city_3_letter_code.isalpha(): # Ensure it's a 3-letter alpha code
        return city_3_letter_code + "1"
    return city_3_letter_code 

# --- CSV Generation Functions ---
def generate_public_links_csv(data, filename="public_links.csv"): 
    df_data = []
    for item in data:
        (start_node_city_3_letter, end_node_city_3_letter), cost, _capacity, _note = item
        df_data.append({ "Start": to_switch_name(start_node_city_3_letter), "End": to_switch_name(end_node_city_3_letter), "Cost": cost })
    df = pd.DataFrame(df_data); df.to_csv(filename, index=False)
    print(f"Successfully generated '{filename}' with {len(df_data)} public links.")
def generate_private_links_csv(data, filename="private_links.csv"): 
    df_data = []; default_uptime = 0.99; default_operator2 = "NA"; shared_tag_to_id_map = {}; next_shared_id = 1
    for item in data: 
        shared_value_for_csv = "NA"
        if item.get('shared_tag') is not None:
            if item['shared_tag'] not in shared_tag_to_id_map: shared_tag_to_id_map[item['shared_tag']] = next_shared_id; next_shared_id += 1
            shared_value_for_csv = shared_tag_to_id_map[item['shared_tag']]
        df_data.append({
            "Start": to_switch_name(item['start']), "End": to_switch_name(item['end']), "Cost": item['cost'],
            "Bandwidth": item['bandwidth'], "Operator1": item['operator'], "Operator2": default_operator2,
            "Uptime": default_uptime, "Shared": shared_value_for_csv
        })
    df = pd.DataFrame(df_data); df.to_csv(filename, index=False)
    print(f"Successfully generated '{filename}' with {len(df_data)} private links.")
def generate_demand_csv(data, filename="demand.csv"): 
    df_data = []
    for item in data:
        df_data.append({ "Start": item['source'], "End": item['destination'], "Traffic": item['demand'], "Type": item['name'] })
    df = pd.DataFrame(df_data); df.to_csv(filename, index=False)
    print(f"Successfully generated '{filename}' with {len(df_data)} demand pairs (traffic volume now stake-influenced).")

if __name__ == "__main__":
    print("Starting CSV generation for comprehensive network...")
    if parsing_issues_count > 0: 
        print(f"\nCritical Warning: Encountered {parsing_issues_count} issues during initial API data parsing. Review console output and '{HUMAN_READABLE_VALIDATOR_SUMMARY_FILE}'.")
        print("This may lead to an incomplete or inaccurate CITIES_DATABASE.")
    
    if not CITIES_DATABASE: 
        print("ERROR: CITIES_DATABASE is empty. This might be due to API key issue or parsing failure.")
        print("Please check your .env file for VALIDATORS_APP_API_KEY and ensure API data can be fetched or cache is valid.")
    else:
        print(f"CITIES_DATABASE initialized with {len(CITIES_DATABASE)} entries.")
        city_codes = list(CITIES_DATABASE.keys()) 
        
        public_link_cost_lookup.clear() 
        for (city_pair_tuple, cost, _capacity, _note) in new_public_links_data: 
            start_city, end_city = city_pair_tuple
            public_link_cost_lookup[(start_city, end_city)] = cost
            public_link_cost_lookup[(end_city, start_city)] = cost
            
        generate_public_links_csv(new_public_links_data)
        generate_private_links_csv(new_private_links_data) 
        generate_demand_csv(new_participants_data)
        print(f"\nCSV file generation complete. Number of unique cities processed: {len(CITIES_DATABASE)}")
        
        # Operator Link Count Summary
        if new_private_links_data:
            operator_counts = Counter(link['operator'] for link in new_private_links_data)
            print("\n--- Operator Link Counts (in generated private_links.csv) ---")
            for operator, count in sorted(operator_counts.items()):
                print(f"  {operator}: {count} link(s)")
            print(f"Total unique operators with links: {len(operator_counts)}")
        else:
            print("\nNo private links were generated.")

        cities_for_review_data = []
        if CITIES_DATABASE: 
            for code, data_dict in CITIES_DATABASE.items():
                region = get_region(code, CITIES_DATABASE, MAJOR_REGIONS) 
                if region == "UNKNOWN" or region is None or code in MAJOR_REGIONS.get('UNKNOWN_REGION_TEMP', []):
                    stake_sol_review = data_dict.get('stake', 0) / LAMPORTS_PER_SOL 
                    cities_for_review_data.append({
                        "GeneratedCode": code,
                        "DescriptiveName": data_dict.get('descriptive_name', 'N/A'),
                        "OriginalDataCenterKeys": "; ".join(data_dict.get('raw_dc_keys', [])), 
                        "Stake_SOL": f"{stake_sol_review:,.2f}",
                        "Population_Validators": data_dict.get('population', 0),
                        "Latitude": data_dict.get('lat', 0.0),
                        "Longitude": data_dict.get('lon', 0.0),
                        "AssignedRegion": region
                    })
        
        if cities_for_review_data: 
            review_df = pd.DataFrame(cities_for_review_data)
            review_df.to_csv(CITIES_NEEDING_REVIEW_FILE, index=False)
            print(f"\nACTION REQUIRED: {len(cities_for_review_data)} cities need region/data review. See '{CITIES_NEEDING_REVIEW_FILE}'.")
            print("Please verify these cities, ensure correct lat/lon, and add their 3-letter codes to the appropriate list in MAJOR_REGIONS.")
        else:
            print("\nAll processed cities were successfully mapped to a major region (or were not UNKNOWN).")
    
    if parsing_issues_count > 0: 
        print(f"\nFinal Warning: Encountered {parsing_issues_count} issues during API data parsing. This likely affected CITIES_DATABASE construction.")


