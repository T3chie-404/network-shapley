# Network Shapley (Enhanced Simulation & Data Generation Fork)

This repository is an enhanced version of the original `network-shapley` tool. While the core economic modeling using Shapley values remains the same, this fork introduces a significantly more powerful and flexible data generation pipeline (`generate_csv_data.py`) and improvements to the core simulation script (`network_shapley.py`).

The primary goal is to enable more realistic, dynamic, and large-scale network simulations by leveraging live data and offering greater control over simulation parameters.

## Core Concept (Unchanged from Original)

The fundamental goal is to compute Shapley values for network contributors. This provides a fair measure of their marginal contribution to overall network performance based on cooperative game theory, moving beyond simpler metrics like mere traffic volume. It helps in understanding the true economic value of different network links and operator contributions within a complex, interconnected system.

## Key Differences & Enhancements in This Fork

This version significantly expands upon the original's capabilities, primarily through the `generate_csv_data.py` script and modifications to `network_shapley.py`:

1.  **API-Driven City & Validator Data (via `generate_csv_data.py`):**
    * **Live Data Source:** Instead of relying solely on manually created static CSVs for network locations and participant data, this version can fetch live validator information (including data center locations, active stake, and validator population counts) directly from the `validators.app` API.
    * **Local Caching:** API responses are cached locally (e.g., in `validators_app_cache.json`) to minimize API calls and speed up subsequent runs. The script checks the cache age and can prompt for a refresh.
    * **`.env` for API Key:** Securely manages the `VALIDATORS_APP_API_KEY` using a `.env` file.

2.  **Dynamic & Comprehensive Public Link Generation (in `generate_csv_data.py`):**
    * Public link latencies are now primarily calculated **dynamically based on geographical distance** (using the Haversine formula) between all city pairs identified from the API data (or supplemented by the `EXISTING_CITIES_TEMPLATE`).
    * This reduces the need for a manually curated, exhaustive `public_links.csv`.

3.  **Advanced City Data Processing & Standardization (in `generate_csv_data.py`):**
    * **`EXISTING_CITIES_TEMPLATE`:** This user-maintained dictionary within `generate_csv_data.py` is crucial for:
        * **Standardizing 3-Letter Codes:** Mapping city names parsed from potentially varied API `data_center_key` formats (e.g., "Frankfurt am Main, DE") to your preferred, consistent 3-letter codes (e.g., "FRA").
        * **Latitude/Longitude Override/Fallback:** Providing verified latitude and longitude for cities, which will be prioritized over or used as a fallback for API-provided coordinates. This is essential for accurate distance calculations.
        * **Country Code Correction:** Assisting in inferring or correcting country codes when API data is ambiguous (e.g., using `country_code_override`).
    * **Robust Parsing:** Improved logic to parse and sanitize city names and country codes from API data, including handling of special characters and varied `data_center_key` formats.

4.  **Flexible and Scalable Private Link Generation (in `generate_csv_data.py`):**
    * **Configurable Operator Count:** Supports a configurable number of network operators (e.g., `NUM_TOTAL_OPERATORS = 20`).
    * **Operator Roles:** Introduces `OPERATOR_Z_NAME` (primary user/operator) and `OPERATOR_A_NAME` (competitor) for specific link assignments.
    * **Dedicated Operator Links:** Allows defining specific fixed links for `OPERATOR_Z_LINKS` and `OPERATOR_A_LINKS` with custom costs and bandwidths. OperatorZ can also be assigned a configurable number of additional random links (`NUM_RANDOM_LINKS_FOR_OPERATOR_Z`).
    * **Granular Link Distribution:**
        * Defines `NUM_TOP_OPERATORS` (e.g., 5) who receive a larger share of the remaining random links.
        * The `TOTAL_PRIVATE_LINKS_TARGET` (e.g., 200) is distributed after accounting for fixed links (DZ TestNet, OperatorZ, OperatorA).
        * Top operators receive ~80% of the *remaining random links*, while the other operators share the remaining ~20%.
    * **Stake-Influenced Link Placement:** Randomly generated private links can be preferentially placed on routes between high-stake city pairs, derived from demand definitions and aggregated city stakes.
    * **Tiered Bandwidth:** Supports assigning different bandwidth tiers (e.g., 100G via `HIGH_BANDWIDTH_VALUE`, 10G via `STANDARD_BANDWIDTH_VALUE`) to private links. 90% of random links for top operators get high bandwidth, while other operators' random links get standard bandwidth. OperatorZ's specific links can also be configured for high bandwidth.

5.  **Expanded & Stake-Influenced Demand Generation (in `generate_csv_data.py`):**
    * The `raw_demand_definitions` list has been significantly expanded to provide a richer set of traffic scenarios.
    * Traffic volumes in `demand.csv` are influenced by the `active_stake` of the source cities, making demand patterns more dynamic.
    * Demand pairs are defined using descriptive city names and country codes (e.g., `('New York', 'US')`) which are then mapped to the final 3-letter codes.

6.  **Parallelized Shapley Calculation (in `network_shapley.py`):**
    * The core `network_shapley.py` script utilizes Python's `multiprocessing` module to parallelize the coalition valuation loop, reducing runtime for simulations with more operators.

7.  **Improved Reporting & Debugging (from `generate_csv_data.py`):**
    * `validator_api_summary.txt`: Comprehensive dump of raw API data.
    * `cities_needing_region_review.csv`: Lists cities needing manual review for codes, lat/lon, or region mapping.
    * **Operator Link Count Summary:** The script now prints a summary of how many links each operator was assigned, helping to verify the distribution.
    * Enhanced warning messages for parsing and mapping issues.

## Get Started (Updated Workflow)

1.  **Clone the Repository:**
    ```bash
    git clone <your-fork-url>
    cd network-shapley
    ```

2.  **Set up Python Environment & Install Requirements:**
    ```bash
    python -m venv .venv
    source .venv/bin/activate  # On Windows: .venv\Scripts\activate
    pip install -r requirements.txt
    ```
    *(Ensure `pandas`, `numpy`, `scipy`, `requests`, `python-dotenv` are in `requirements.txt`)*

3.  **Create `.env` File for API Key:**
    * In the project root, create `.env` with: `VALIDATORS_APP_API_KEY=your_actual_secret_api_token_here`
    * **Add `.env` to your `.gitignore` file.**

4.  **Curate Data Generation Parameters (in `generate_csv_data.py`):**
    * Open `generate_csv_data.py`.
    * **`EXISTING_CITIES_TEMPLATE`:** This is crucial. Populate this dictionary with cities you expect from the API or want to ensure are included with specific codes and verified lat/lon. Use normalized keys (e.g., `cityname_cc` like `frankfurt_de`).
    * **`MAJOR_REGIONS`:** Ensure this dictionary maps all your final 3-letter city codes to the correct geographical regions.
    * **Operator Configuration:**
        * Set `OPERATOR_Z_NAME` and `OPERATOR_A_NAME`. Operator Z is the script user and Operator A is for adding specific links to a route to test as a competitor on your route.
        * Define `OPERATOR_Z_LINKS` and `OPERATOR_A_LINKS` with their specific city pairs, costs, and bandwidths.
        * Configure `NUM_RANDOM_LINKS_FOR_OPERATOR_Z`. This is if you want your user to be assigned and number of random links in addition to the defined ones.
    * **Network Configuration:**
        * Adjust `NUM_TOTAL_OPERATORS` (e.g., 20).
        * Adjust `NUM_TOP_OPERATORS` (e.g., 5).
        * Set `TOTAL_PRIVATE_LINKS_TARGET` (e.g., 200).
        * Configure `HIGH_BANDWIDTH_VALUE`, `STANDARD_BANDWIDTH_VALUE`, and `HIGH_BANDWIDTH_RATIO_FOR_TOP_OPS`.
    * **Demand:** Review and expand `raw_demand_definitions` as needed.

5.  **Generate Simulation Input CSVs:**
    ```bash
    python3 generate_csv_data.py
    ```
    * Review console output, especially the "Operator Link Counts" summary.
    * Check `validator_api_summary.txt` and `cities_needing_region_review.csv`.

6.  **Iterate on Data Quality (CRUCIAL):**
    * Based on `cities_needing_region_review.csv` and any warnings:
        1.  Refine `EXISTING_CITIES_TEMPLATE` for better city name to code/geo mapping.
        2.  Update `MAJOR_REGIONS` with any new or corrected city codes.
    * Re-run `generate_csv_data.py`. Repeat until the generated data is satisfactory.

7.  **Run the Simulation:**
    * Use `run_worldwide_simulation.py` or your custom run script.
        ```bash
        python3 run_worldwide_simulation.py
        ```
    * **Operator Limit in `network_shapley.py`:** The core `network_shapley.py` script has an assertion like `_assert(n_ops < 16, ...)`. If `generate_csv_data.py` now produces a `private_links.csv` with more unique operators than this limit, you **must** modify this assertion in `network_shapley.py` (e.g., to `_assert(n_ops < 21, ...)` if you expect up to 20 operators). Be mindful of the exponential increase in computation time with more operators.


