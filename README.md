# Network Shapley (Enhanced Simulation & Data Generation Fork)

This repository is an enhanced version of the original `network-shapley` tool. While the core economic modeling using Shapley values remains the same, this fork introduces a significantly more powerful and flexible data generation pipeline (`generate_csv_data.py`) and improvements to the core simulation script (`network_shapley.py`).

The primary goal is to enable more realistic, dynamic, and large-scale network simulations by leveraging live data and offering greater control over simulation parameters.

## Core Concept (Unchanged from Original)

The fundamental goal is to compute Shapley values for network contributors. This provides a fair measure of their marginal contribution to overall network performance based on cooperative game theory, moving beyond simpler metrics like mere traffic volume. It helps in understanding the true economic value of different network links and operator contributions within a complex, interconnected system.

## Original Project

This project forks and enhances the original `network-shapley` repository. You can find the original project here: **[Link to Original Network Shapley Repository - TODO: Add URL]**

---

## Key Features & Enhancements in This Fork

This version significantly expands upon the original's capabilities, primarily through the `generate_csv_data.py` script and modifications to `network_shapley.py`:

* NOTE: TIME WAS NOT TAKEN TO COMPLETLY VETTE ACCURACY OF DATA, LOCATIONS, OR VALIDATOR INFO *
*        THIS WAS A PROOF-OF-CONCEPT TO TRY TO UNDERSTAND THE LARGER SYSTEM AS A WHOLE *

1.  ✨ **API-Driven City & Validator Data (via `generate_csv_data.py`):**
    * **Live Data Source:** Fetches live validator information (data center locations, active stake, validator counts) directly from the `validators.app` API.
    * **Local Caching:** Caches API responses (e.g., `validators_app_cache.json`) with configurable stale days.
    * **Secure API Key Management:** Uses a `.env` file for `VALIDATORS_APP_API_KEY`.

2.  🌍 **Dynamic & Comprehensive Public Link Generation (in `generate_csv_data.py`):**
    * Public link latencies are primarily calculated **dynamically based on geographical distance** (Haversine formula) between city pairs.
    * Utilizes `MAJOR_REGIONS` mapping for more accurate inter/intra-continental latency calculations.

3.  🏙️ **Advanced City Data Processing & Standardization (in `generate_csv_data.py`):**
    * **`EXISTING_CITIES_TEMPLATE`:** Crucial for standardizing city names to 3-letter codes, providing verified lat/lon, and overriding country codes.
    * **Robust Parsing:** Improved logic for sanitizing city names and country codes from API `data_center_key`s.

4.  🔗 **Flexible & Scalable Private Link Generation (in `generate_csv_data.py`):**
    * **Configurable Operator Count & Link Targets:** (See "Key Configuration Parameters" below).
    * **Granular Link Distribution:** Proportional distribution of random links among "top" and "other" operators.
    * **Stake-Influenced Link Placement:** Randomly generated private links can be biased towards high-stake city pairs (derived from demand definitions).
    * **Tiered Bandwidth:** Assigns different bandwidth tiers (e.g., 100G, 10G) to private links, with configurable ratios for top operators.
    * **Fixed Links:** Retains support for defining specific fixed links (e.g., DZ TestNet, OperatorZ specific routes).

5.  📊 **Stake-Influenced Demand Generation (in `generate_csv_data.py`):**
    * Traffic volumes in `demand.csv` are influenced by the `active_stake` of source cities, modulated by `STAKE_INFLUENCE_AGGREGATE_FACTOR` and per-route `stake_influence` settings. (See "Key Configuration Parameters").
    * Demand pairs are defined using descriptive names and mapped to 3-letter codes.

6.  🚀 **Parallelized Shapley Calculation (in `network_shapley.py`):**
    * Utilizes Python's `multiprocessing` to parallelize the coalition valuation loop.

7.  📝 **Improved Reporting & Debugging (from `generate_csv_data.py`):**
    * `validator_api_summary.txt`: Human-readable API data dump.
    * `cities_needing_region_review.csv`: Lists cities needing manual review.
    * Console summaries of operator link counts and parsing issues.

---

## Prerequisites

* Python (3.7+ recommended)
* pip (Python package installer)
* Git
* A `validators.app` API key.

---

## Installation

1.  **Clone the Repository:**
    ```bash
    git clone https://github.com/T3chie-404/network-shapley.git # TODO: Replace with your fork's actual URL
    cd network-shapley # Or your repository's directory name
    ```

2.  **Set up Python Environment & Install Requirements:**
    ```bash
    python -m venv .venv
    source .venv/bin/activate  # On Windows: .venv\Scripts\activate
    pip install -r requirements.txt
    ```
    *(Ensure `pandas`, `numpy`, `scipy`, `requests`, `python-dotenv` are in `requirements.txt`)*

3.  **Create `.env` File for API Key:**
    * In the project root, create `.env`: `VALIDATORS_APP_API_KEY=your_api_token_here`
    * **Add `.env` to your `.gitignore` file.**

---

## Configuration (`generate_csv_data.py`)

Before running `generate_csv_data.py`, review and adjust parameters at the top of the script:

1.  **API & Cache Settings:**
    * `CACHE_STALE_DAYS`: How old API cache can be before prompting for refresh.

2.  **`EXISTING_CITIES_TEMPLATE` (CRUCIAL):**
    * Your primary tool for standardizing city data from the API.
    * Map varied API city names/country codes to your preferred 3-letter `code`, verified `lat`/`lon`, and `country_code_override`.
    * Keys should be lowercase, underscore-separated: `cityname_cc` (e.g., `frankfurt_de`).

3.  **`MAJOR_REGIONS`:**
    * Map your final 3-letter city codes to geographical regions (NA, EU, AS, etc.) for accurate latency calculations. Cities not mapped here or inferred by country code will be flagged for review.

4.  **Key Simulation Parameters (User-Adjusted Values from your selection):**
    * **`HIGH_BANDWIDTH_VALUE` / `STANDARD_BANDWIDTH_VALUE`**: Define capacities for different link tiers (e.g., 100G, 10G).
    * **`NUM_TOTAL_OPERATORS = 1`**: Sets the total number of distinct network operators in the simulation. This includes your main operator (e.g., `OperatorZ`), operators derived from fixed links (like `DZ_Op_XXXX`), and any procedurally generated `Contributor` operators needed to reach this total. *Your current setting of `1` will likely result in only `OperatorZ` or a single DZ operator if no other fixed links define more.*
    * **`NUM_TOP_OPERATORS = 5`**: Designates how many operators are considered "top-tier." These operators can receive a higher proportion of high-bandwidth links if random links are generated for them.
    * **`TOTAL_PRIVATE_LINKS_TARGET = 60`**: The script will attempt to generate enough random private links to reach this total after accounting for predefined fixed links (like DZ TestNet or OperatorZ links). *With your current setting of 60, and considering the number of fixed links, this will determine how many (if any) random links are created.*
    * **`HIGH_BANDWIDTH_RATIO_FOR_TOP_OPS = 0.80`**: For any *randomly generated* private links assigned to "top-tier" operators, this ratio (80%) will be high-bandwidth (e.g., 100G). The remainder (20%) will be standard bandwidth.
    * **`STAKE_INFLUENCE_AGGREGATE_FACTOR = 10.0`**: This is a general scaling factor applied to the influence of a source city's validator stake on its generated traffic demand. It works alongside the per-route `stake_influence` multiplier defined in `raw_demand_definitions`. A higher aggregate factor means stake generally has a more pronounced impact on demand volumes across all routes.
    * **`MIN_DEMAND_PER_ROUTE = 1`**: Ensures that every defined demand pair will have at least this minimum amount of traffic, regardless of stake calculations.

5.  **Fixed Link Definitions:**
    * `DZ_TESTNET_LINKS_RAW_DESCRIPTIVE`: Define specific links with known latencies and owners, like the DoubleZero TestNet.
    * You can add other fixed links for `OPERATOR_Z` or other specific operators directly in the script.

6.  **Demand Definitions:**
    * `raw_demand_definitions`: A list of dictionaries defining traffic demand pairs. Each entry specifies:
        * `source_desc` / `destination_desc`: Tuples of (City Name, Country Code).
        * `base_traffic_weight`: Base volume for this demand.
        * `value`: Economic value of the traffic (used by Shapley).
        * `stake_influence`: A per-route multiplier (0.0 to 1.0+) indicating how strongly the source city's stake should affect this specific route's demand, used in conjunction with `STAKE_INFLUENCE_AGGREGATE_FACTOR`.

---

## Workflow: Generating Data & Running Simulations

1.  **Curate `generate_csv_data.py`:**
    * Set API key in `.env`.
    * Carefully populate `EXISTING_CITIES_TEMPLATE`.
    * Ensure `MAJOR_REGIONS` is comprehensive for your city codes.
    * Adjust simulation parameters (operator counts, link targets, bandwidths, demand factors) as described above.

2.  **Generate Simulation Input CSVs:**
    ```bash
    python3 generate_csv_data.py
    ```
    * Prompts to refresh API data if cache is stale.
    * Outputs: `public_links.csv`, `private_links.csv`, `demand.csv`, `validator_api_summary.txt`, `cities_needing_region_review.csv`.

3.  **Iterate on Data Quality (CRUCIAL):**
    * Review `validator_api_summary.txt` (how API data is parsed).
    * Examine `cities_needing_region_review.csv`. For each city:
        1.  Verify/correct its 3-letter code, lat/lon.
        2.  Update `EXISTING_CITIES_TEMPLATE` in `generate_csv_data.py`.
        3.  Add its final 3-letter code to the correct list in `MAJOR_REGIONS`.
    * Re-run `generate_csv_data.py`. Repeat until `cities_needing_region_review.csv` is satisfactory.

4.  **Run the Simulation:**
    * Use `run_worldwide_simulation.py` or your custom run script.
        ```bash
        python3 run_worldwide_simulation.py
        ```
    * **Operator Limit Note:** `network_shapley.py` has an assertion (`_assert(n_ops < 21, ...)`). If you configure `NUM_TOTAL_OPERATORS` in `generate_csv_data.py` to be higher than this limit (e.g., 20 operators means `n_ops` will be 20), you may need to adjust this assertion in `network_shapley.py`. Be mindful of exponential computation time increase with more operators.

---

## Key Scripts and Files Overview

* **`generate_csv_data.py`**: Main script for data generation. Contains key configurations.
* **`network_shapley.py`**: Core script for Shapley value calculations (parallelized).
* **`run_worldwide_simulation.py`**: Example script to run the simulation.
* **`.env`**: Stores `VALIDATORS_APP_API_KEY`.
* **`requirements.txt`**: Python dependencies.
* **Output CSVs (`*.csv`)**: `public_links.csv`, `private_links.csv`, `demand.csv`.
* **Reports & Cache**: `validators_app_cache.json`, `validator_api_summary.txt`, `cities_needing_region_review.csv`.

---

## Contributing

Contributions are welcome! Please open an issue to discuss changes, then fork and submit a pull request.

---

## License

**[TODO: Specify License Information Here]**

---
