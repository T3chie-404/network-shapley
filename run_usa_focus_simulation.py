# run_usa_focus_simulation.py
"""
Runs the network_shapley simulation using data from generated CSV files,
allowing for analysis focused on specific operators like OperatorZ and others in the USA.

Assumes that 'generate_csv_data.py' has been run and the following files exist
in the same directory or a specified path:
- private_links.csv
- public_links.csv
- demand.csv

Run from the repo root (after generating CSVs) with:
    python run_usa_focus_simulation.py 
    (or python3 run_usa_focus_simulation.py)
"""

from __future__ import annotations
import pathlib
import sys
import pandas as pd

# --- Ensure the repo root is on PYTHONPATH so we can import network_shapley.py ---
# This assumes 'run_usa_focus_simulation.py' is in the same root directory as 'network_shapley.py'
# and the CSV files. If CSVs are elsewhere, adjust CSV_PATH.
try:
    REPO_ROOT = pathlib.Path(__file__).resolve().parent
    if not (REPO_ROOT / "network_shapley.py").exists():
        # If not in root, try one level up (common for scripts in a 'scripts' subfolder)
        REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
    
    if not (REPO_ROOT / "network_shapley.py").exists():
        raise FileNotFoundError("Could not reliably find network_shapley.py. Ensure this script is run from the repo root or adjust REPO_ROOT.")

    sys.path.append(str(REPO_ROOT))
    from network_shapley import network_shapley
except ImportError as e:
    print(f"Error importing network_shapley: {e}")
    print("Please ensure that network_shapley.py is in the repository root directory,")
    print("and that this script is run from a location where it can find it (e.g., the repo root).")
    sys.exit(1)
except FileNotFoundError as e:
    print(e)
    sys.exit(1)


# --- Configuration for CSV file paths ---
# Assumes CSVs are in the same directory as this script (which should be the repo root)
CSV_DIR = REPO_ROOT 
PRIVATE_LINKS_FILE = CSV_DIR / "private_links.csv"
PUBLIC_LINKS_FILE = CSV_DIR / "public_links.csv"
DEMAND_FILE = CSV_DIR / "demand.csv" # Using the standard demand file generated

def load_inputs_from_csv() -> tuple[pd.DataFrame | None, pd.DataFrame | None, pd.DataFrame | None]:
    """Loads private_links, public_links, and demand DataFrames from CSV files."""
    private_links_df = None
    public_links_df = None
    demand_df = None

    try:
        private_links_df = pd.read_csv(PRIVATE_LINKS_FILE)
        print(f"Successfully loaded '{PRIVATE_LINKS_FILE}'")
    except FileNotFoundError:
        print(f"Error: '{PRIVATE_LINKS_FILE}' not found. Please generate it first.")
    except Exception as e:
        print(f"Error loading '{PRIVATE_LINKS_FILE}': {e}")

    try:
        public_links_df = pd.read_csv(PUBLIC_LINKS_FILE)
        print(f"Successfully loaded '{PUBLIC_LINKS_FILE}'")
    except FileNotFoundError:
        print(f"Error: '{PUBLIC_LINKS_FILE}' not found. Please generate it first.")
    except Exception as e:
        print(f"Error loading '{PUBLIC_LINKS_FILE}': {e}")
        
    try:
        demand_df = pd.read_csv(DEMAND_FILE)
        print(f"Successfully loaded '{DEMAND_FILE}'")
    except FileNotFoundError:
        print(f"Error: '{DEMAND_FILE}' not found. Please generate it first.")
    except Exception as e:
        print(f"Error loading '{DEMAND_FILE}': {e}")

    return private_links_df, public_links_df, demand_df

def main() -> None:
    print("Loading simulation inputs from CSV files...")
    private_links, public_links, demand = load_inputs_from_csv()

    if private_links is None or public_links is None or demand is None:
        print("\nOne or more input files could not be loaded. Aborting simulation.")
        return

    print("\nRunning network_shapley simulation...")
    # You can adjust these optional parameters as needed
    result_df = network_shapley(
        private_links=private_links,
        public_links=public_links,
        demand=demand,
        operator_uptime=0.98,    # Default from example_run.py
        hybrid_penalty=5.0,      # Default from example_run.py
        demand_multiplier=1.0,   # Default from example_run.py
    )

    print("\n--- Full Shapley Results ---")
    if result_df is not None and not result_df.empty:
        print(result_df.to_string(index=False))

        # --- Analysis for specific operators ---
        print("\n--- Analysis for Specific USA Area Operators ---")
        
        # Operators of interest (based on your request and data in generate_csv_data.py)
        # OperatorZ: Your link (SLC-CHI)
        # ContributorA: Shares SLC-CHI link
        # ContributorC: Ashburn-NY, Dallas-LA
        # ContributorD: Chicago-Dallas
        # ContributorE: NewYork-Chicago
        # ContributorF: SLC-Seattle
        # ContributorG, H, I also have US-based endpoints for their international links.
        
        operators_to_highlight = ["OperatorZ", "ContributorA", "ContributorC", "ContributorD", "ContributorE", "ContributorF"]
        
        highlighted_results = result_df[result_df['Operator'].isin(operators_to_highlight)]
        
        if not highlighted_results.empty:
            print("\nData for OperatorZ and selected USA-area contributors:")
            print(highlighted_results.to_string(index=False))
        else:
            print(f"\nCould not find data for the specified operators: {', '.join(operators_to_highlight)}")
            print("Check if these operators were present in the 'private_links.csv' and participated.")
        
        operator_z_data = result_df[result_df['Operator'] == "OperatorZ"]
        if not operator_z_data.empty:
            print(f"\nSpecific data for OperatorZ:")
            print(operator_z_data.to_string(index=False))
        else:
            print(f"\nOperatorZ not found in results.")

    else:
        print("Simulation did not return results or results were empty.")


if __name__ == "__main__":
    main()

