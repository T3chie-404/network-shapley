# network_shapley.py (Parallelized Version)

# Packages
from __future__ import annotations
import math
from typing import List, Dict, Tuple # Added Tuple
import numpy as np
import pandas as pd
from numpy.typing import NDArray
from scipy.optimize import linprog
from scipy.sparse import (
    csr_matrix,
    block_diag,
    diags,
    hstack as sp_hstack,
    vstack as sp_vstack,
)
import multiprocessing # Added for parallelization
import os # Added for cpu_count

# --- Constants for Parallelization ---
# Minimum number of operators to trigger parallel processing.
# Tune this based on observed performance; for very small n_ops, serial might be faster.
MIN_OPS_FOR_PARALLEL = 8 # Example threshold

# Helper utilities (no changes from original)
def _assert(cond: bool, msg: str) -> None:
    if not cond:
        raise ValueError(msg)

def _has_digit(s: pd.Series) -> pd.Series:
    return s.str.contains(r"[0-9]")

def _unique_int(s: pd.Series) -> pd.Series:
    return s.map({u: i + 1 for i, u in enumerate(pd.unique(s))})

def _rep(arr: NDArray, times: int) -> NDArray:
    return np.tile(arr, times)

def _bits(n_bits: int) -> NDArray:
    cols = np.arange(2**n_bits, dtype=np.uint32)
    return ((cols[None] >> np.arange(n_bits)[:, None]) & 1).astype(np.uint8)

def _fact(v: NDArray) -> NDArray:
    return np.vectorize(math.factorial, otypes=[float])(v)

def consolidate_map(
    private_links: pd.DataFrame,
    demand: pd.DataFrame,
    public_links: pd.DataFrame,
    hybrid_penalty: float,
) -> pd.DataFrame:
    # (Function body remains IDENTICAL to your original version)
    # Work on copies to avoid mutating caller data
    private_df = private_links.copy()
    public_df = public_links.copy()
    demand_df = demand.copy()

    # Perform basic sanity checks on map length, switch names, and node names
    _assert(private_df.shape[0] > 0,
            "There must be at least one private link for this simulation.")
    _assert(_has_digit(private_df["Start"]).all() & _has_digit(private_df["End"]).all(),
            "Switches are not labeled correctly in private links; they should be denoted with an integer.")
    _assert(_has_digit(public_df["Start"]).all() & _has_digit(public_df["End"]).all(),
            "Switches are not labeled correctly in public links; they should be denoted with an integer.") # Corrected msg
    _assert((~_has_digit(demand_df["Start"])).all() & (~_has_digit(demand_df["End"])).all(),
            "Endpoints are not labeled correctly in the demand matrix; they should not have an integer.")

    # Cast operators as strings and fill in any missing secondary operators
    private_df["Operator1"] = private_df["Operator1"].astype(str)
    private_df["Operator2"] = (private_df["Operator2"].fillna(private_df["Operator1"])).astype(str)

    # Duplicate private links, so matrix represents one-way flows only between switches
    max_shared = int(private_df["Shared"].max(skipna=True)) if pd.notna(private_df["Shared"].max(skipna=True)) else 0
    rev = private_df.copy()
    rev[["Start", "End"]] = rev[["End", "Start"]]
    # Ensure 'Shared' column is numeric before addition if it's not already guaranteed
    rev["Shared"] = pd.to_numeric(rev['Shared'], errors='coerce').fillna(0) + max_shared 
    private_df = pd.concat([private_df, rev], ignore_index=True)

    # Adjust private bandwidth for uptime and make private links available to all traffic types
    private_df["Bandwidth"] *= private_df["Uptime"]
    private_df["Type"] = 0

    # Compact down shared IDs (if gaps in series) for private links
    max_shared = int(private_df["Shared"].max(skipna=True)) if pd.notna(private_df["Shared"].max(skipna=True)) else 0
    na_shared = private_df["Shared"].isna()
    if na_shared.any():
        private_df.loc[na_shared, "Shared"] = np.arange(max_shared + 1, max_shared + 1 + na_shared.sum())
    private_df["Shared"] = _unique_int(private_df["Shared"].astype(float)) # Ensure numeric before unique_int

    # Perform sanity check on traffic type in demand matrix
    _assert((demand_df.groupby("Type")["Start"].nunique() == 1).all(),
            "All traffic of a single type must have a single source.")

    # Duplicate public links, so matrix represents one-way flows only
    rev_public = public_df.copy()
    rev_public[["Start", "End"]] = rev_public[["End", "Start"]]
    public_df = pd.concat([public_df, rev_public], ignore_index=True)
    public_df["Type"] = 0

    # Perform sanity checks on the public links spanning private link routes and demand nodes
    _assert(pd.merge(private_df[["Start", "End"]].drop_duplicates(), 
                     public_df[["Start", "End"]].drop_duplicates(), 
                     on = ['Start', 'End']).shape[0] == private_df[["Start", "End"]].drop_duplicates().shape[0],
            "The public pathway is not fully specified for all the unique private link switch pairs.")
            
    city_pairs = public_df.assign(Start_City=public_df["Start"].str[:3], End_City=public_df["End"].str[:3]) \
                          [["Start_City", "End_City"]].rename(columns={"Start_City": "Start", "End_City": "End"})
    _assert(pd.merge(demand_df[["Start", "End"]].drop_duplicates(), 
                     city_pairs.drop_duplicates(), 
                     on=['Start', 'End']).shape[0] == demand_df[["Start", "End"]].drop_duplicates().shape[0],
            "The public pathway is not fully specified for the demand points.")


    # Build both helper links (node to switch) and direct public paths (node to node), per traffic type
    helper_frames: List[pd.DataFrame] = []
    for t in demand_df["Type"].unique():
        src_city = demand_df.loc[demand_df["Type"] == t, "Start"].iat[0]
        dst_cities = demand_df.loc[demand_df["Type"] == t, "End"].unique()

        helper_dir = public_df[(public_df["Start"].str[:3] == src_city) & (public_df["End"].str[:3].isin(dst_cities))]
        if not helper_dir.empty:
            helper_dir = helper_dir.assign(Start=helper_dir["Start"].str[:3], End=helper_dir["End"].str[:3])
            helper_dir = helper_dir.groupby(["Start", "End"], as_index=False)["Cost"].min()
            helper_dir["Type"] = t
        else: # Create an empty df with correct columns if no direct paths
            helper_dir = pd.DataFrame(columns=["Start", "End", "Cost", "Type"])


        src_switches = public_df.loc[public_df["Start"].str[:3] == src_city, "Start"].unique()
        helper_src = pd.DataFrame({"Start": src_city, "End": src_switches, "Cost": 0, "Type": t})
        
        dst_switches_series = public_df.loc[public_df["End"].str[:3].isin(dst_cities), "End"]
        if not dst_switches_series.empty:
            helper_dst = pd.DataFrame({"Start": dst_switches_series.unique(), 
                                   "End": [s[:3] for s in dst_switches_series.unique()], 
                                   "Cost": 0, "Type": t})
        else:
            helper_dst = pd.DataFrame(columns=["Start", "End", "Cost", "Type"])


        helper_frames.append(pd.concat([helper_dir, helper_src, helper_dst], ignore_index=True))
    
    public_df["Cost"] += hybrid_penalty
    if helper_frames: # Only concat if helper_frames is not empty
        public_df = pd.concat([public_df, pd.concat(helper_frames, ignore_index=True)], ignore_index=True)
    
    public_df = public_df.assign(Bandwidth=0, Operator1='0', Operator2='0', Uptime=1, Shared=0)
    # Ensure columns match private_df for consistent concatenation
    # This might require selecting common columns or aligning them carefully.
    # For simplicity, let's assume private_df.columns is the target set from original code.
    # However, 'Type' in public_df might be an issue if private_df 'Type' is always 0.
    # The original code did: public_df.assign(...)[private_df.columns]
    # This assumes all columns in private_df also exist or are created in public_df
    
    # Re-align columns to match private_df, adding missing ones with default values
    for col in private_df.columns:
        if col not in public_df.columns:
            if col == "Bandwidth": public_df[col] = 0 # Default for public links
            elif col == "Uptime": public_df[col] = 1.0
            elif col == "Operator1" or col == "Operator2": public_df[col] = "0"
            elif col == "Shared": public_df[col] = 0 # Public links aren't 'shared' in the private sense
            else: public_df[col] = pd.NA # Or appropriate default
    public_df = public_df[private_df.columns] # Ensure same column order and set

    return pd.concat([private_df, public_df], ignore_index=True)


def lp_primitives(
    link_map: pd.DataFrame,
    demand: pd.DataFrame,
    demand_multiplier: float,
) -> Dict[str, object]:
    # (Function body remains IDENTICAL to your original version)
    n_private = int((link_map["Operator1"] != "0").sum())
    n_links = len(link_map)
    nodes = np.sort(pd.unique(np.concatenate([link_map["Start"], link_map["End"], demand["Start"], demand["End"]])))
    node_idx = {n: i for i, n in enumerate(nodes)}
    rows, cols, data = [], [], []
    for j, (s, e) in link_map[["Start", "End"]].iterrows():
        rows += [node_idx[s], node_idx[e]]
        cols += [j, j]
        data += [1, -1]
    A_single = csr_matrix((data, (rows, cols)), shape=(len(nodes), n_links))
    commodities = np.sort(demand["Type"].unique())
    A = block_diag([A_single] * len(commodities), format="csr")
    keep: List[int] = []
    for k, t in enumerate(commodities):
        valid_type_mask = (link_map["Type"] == t) | (link_map["Type"] == 0)
        if not valid_type_mask.any(): # If a commodity type has no valid links at all
             print(f"Warning: Commodity type {t} has no valid links (Type column in link_map). This might lead to issues.")
        valid = np.where(valid_type_mask)[0]
        keep.extend(valid + k * n_links) 
    keep = np.asarray(keep)
    if keep.size == 0 and A.shape[1] > 0 : # If keep is empty but A had columns
        print("Warning: 'keep' array is empty, meaning no links are valid for any commodity type. LP will likely be trivial or fail.")
        # A will become an empty matrix if keep is empty.
        # We might need to return early or handle this gracefully if it implies an invalid setup.
        # For now, allow A to become empty if 'keep' is empty.
    A = A[:, keep] if keep.size > 0 else csr_matrix((A.shape[0], 0))


    b_flows: List[NDArray] = []
    for t in commodities:
        vec = np.zeros(len(nodes))
        sub = demand[demand["Type"] == t]
        for _, r in sub.iterrows():
            vec[node_idx[r["Start"]]] += r["Traffic"] * demand_multiplier
            vec[node_idx[r["End"]]]   -= r["Traffic"] * demand_multiplier
        b_flows.append(vec)
    b = np.concatenate(b_flows) if b_flows else np.array([])

    # Ensure shared_ids are integers and handle potential NaN before max()
    link_map_private_shared = pd.to_numeric(link_map.loc[: n_private - 1, "Shared"], errors='coerce').fillna(0)
    shared_ids = link_map_private_shared.astype(int).to_numpy()
    
    I_single_shape_0 = shared_ids.max() if shared_ids.size > 0 else 0
    if I_single_shape_0 == 0 and n_private > 0: # If max shared_id is 0 but there are private links
        # This can happen if all Shared IDs are 0 (e.g. after fillna(0) if all were NA)
        # and _unique_int made them all 1. Max would be 1.
        # If shared_ids is empty (n_private=0), I_single_shape_0 is 0.
        # Let's ensure shape is at least 1 if n_private > 0 to avoid empty matrix error if all shared IDs were 0.
        # The original _unique_int maps to 1-based indexing.
        # If all shared IDs become 1 (e.g. from all NA), shared_ids.max() would be 1.
        pass # Max logic seems okay.

    if n_private > 0 and I_single_shape_0 > 0: # Only build I_single if there are private links and valid shared IDs
        I_single = csr_matrix(
            (np.ones(n_private), (shared_ids - 1, np.arange(n_private))), # -1 assumes 1-based shared_ids
            shape=(I_single_shape_0, n_links),
        )
        I = sp_hstack([I_single] * len(commodities), format="csr")[:, keep] if keep.size > 0 else csr_matrix((I_single.shape[0] * len(commodities), 0))
        
        sorted_dupes = link_map.iloc[:n_private].drop_duplicates("Shared") # original had sort_values but not strictly needed for cap
        cap = sorted_dupes["Bandwidth"].to_numpy()
        row_op1 = sorted_dupes["Operator1"].to_numpy()
        row_op2 = sorted_dupes["Operator2"].to_numpy()
    else: # No private links or no shared ID structure
        I = csr_matrix((0, A.shape[1])) # Empty inequality constraints
        cap = np.array([])
        row_op1 = np.array([])
        row_op2 = np.array([])


    col_op1 = _rep(link_map["Operator1"].to_numpy(), len(commodities))[keep] if keep.size > 0 else np.array([])
    col_op2 = _rep(link_map["Operator2"].to_numpy(), len(commodities))[keep] if keep.size > 0 else np.array([])
    cost = _rep(link_map["Cost"].to_numpy(), len(commodities))[keep] if keep.size > 0 else np.array([])

    return dict(A_eq=A, A_ub=I, b_eq=b, b_ub=cap, cost=cost,
                row_index1=row_op1, row_index2=row_op2,
                col_index1=col_op1, col_index2=col_op2)

# --- Worker function for parallel processing ---
def _solve_coalition_lp_worker(args: Tuple[int, NDArray, NDArray, Dict, float]) -> float:
    """
    Solves the linear program for a single coalition.
    Args:
        idx: Coalition index (for debugging or tracking, not used in calculation here).
        bitmap_col: The column from the bitmap representing the current coalition (members are 1).
        operators: Array of all operator names.
        prim: Dictionary of LP primitives.
        default_svalue: Value to return if LP fails.
    Returns:
        The calculated svalue for this coalition.
    """
    _idx, current_bitmap_col, all_operators, lp_primitives_dict, default_val = args
    
    subset_ops = all_operators[current_bitmap_col == 1]

    # Masks used to access relevant coalition sets (and public operator "0")
    # Ensure np.concatenate always has ["0"] even if subset_ops is empty
    active_entities_for_mask = np.concatenate((["0"], subset_ops))

    row_mask = (np.isin(lp_primitives_dict["row_index1"], active_entities_for_mask) &
                np.isin(lp_primitives_dict["row_index2"], active_entities_for_mask))
    
    col_mask = (np.isin(lp_primitives_dict["col_index1"], active_entities_for_mask) &
                np.isin(lp_primitives_dict["col_index2"], active_entities_for_mask))

    cost_vector = lp_primitives_dict["cost"][col_mask]
    
    # Handle A_ub correctly if it's empty due to no private links in coalition
    A_ub_matrix_full = lp_primitives_dict["A_ub"]
    if A_ub_matrix_full.shape[0] > 0 and A_ub_matrix_full.shape[1] > 0 : # If A_ub was constructed
        A_ub_coalition = A_ub_matrix_full[row_mask][:, col_mask]
        b_ub_coalition = lp_primitives_dict["b_ub"][row_mask]
        if A_ub_coalition.shape[0] == 0: # No relevant constraints for this coalition
            A_ub_to_pass = None
            b_ub_to_pass = None
        else:
            A_ub_to_pass = A_ub_coalition
            b_ub_to_pass = b_ub_coalition
    else: # A_ub was initially empty (e.g., no private links at all in the simulation)
        A_ub_to_pass = None
        b_ub_to_pass = None

    # Handle A_eq if col_mask makes it have zero columns (no usable links for any commodity)
    A_eq_coalition = lp_primitives_dict["A_eq"][:, col_mask] if lp_primitives_dict["A_eq"].shape[1] > 0 else lp_primitives_dict["A_eq"]


    if cost_vector.size == 0 and A_eq_coalition.shape[1] == 0 : # No variables in the problem for this coalition
        # This typically means the coalition (plus public) cannot satisfy any part of the demand, or no demand exists.
        # Or, more simply, this coalition contributes no usable links.
        # The objective value for such a scenario is often taken as -infinity (or a very large negative if minimizing cost)
        # if demand is non-zero. If demand is zero, cost is zero.
        # For this model, if linprog can't run, it implies failure or trivial solution.
        # The original serial code would attempt to run linprog which would likely error or return non-success.
        # We want to achieve -res.fun. If fun is 0 for trivial success, then 0. If error, default_val.
        # If no cost vector, implies no flow variables. If b_eq is all zero, fun=0. If b_eq non-zero, infeasible.
        if not np.any(lp_primitives_dict["b_eq"]): # if all demand is zero
            return 0.0 # No cost, no flow
        else:
            return default_val # Cannot satisfy demand, effectively infinite cost / -infinity value

    res = linprog(
        cost_vector,
        A_ub=A_ub_to_pass,
        b_ub=b_ub_to_pass,
        A_eq=A_eq_coalition,
        b_eq=lp_primitives_dict["b_eq"],
        bounds=(0, None),
        method="highs"
    )
    if res.success:
        return -res.fun  # Negative to turn min objective into max objective
    return default_val


def network_shapley(
    private_links: pd.DataFrame,
    demand: pd.DataFrame,
    public_links: pd.DataFrame,
    operator_uptime: float = 1.0,
    hybrid_penalty: float = 5.0,
    demand_multiplier: float = 1.0,
) -> pd.DataFrame:
    # Enumerate all operators
    operators = np.sort(pd.unique(np.concatenate([private_links["Operator1"].dropna().astype(str),
                                                  private_links["Operator2"].dropna().astype(str)])))
    operators = operators[operators != "0"] # Remove public operator "0" if present
    n_ops = len(operators)
    _assert("0" not in operators, "0 is a protected keyword for operator names; choose another.")
    _assert(n_ops < 21, "There are too many operators; we limit to 15 to prevent the program from crashing.")

    bitmap = _bits(n_ops)
    full_map = consolidate_map(private_links, demand, public_links, hybrid_penalty)
    prim = lp_primitives(full_map, demand, demand_multiplier)

    n_coal = 2 ** n_ops
    svalue = np.full(n_coal, -np.inf) # Default value for failed LP solves
    size = np.zeros(n_coal, dtype=int) # To store size of each coalition

    # --- Coalition Valuation: Parallel or Serial ---
    if n_ops >= MIN_OPS_FOR_PARALLEL and os.cpu_count() and os.cpu_count() > 1:
        print(f"Running coalition valuation in parallel with {os.cpu_count()} cores for {n_ops} operators...")
        tasks = []
        for idx in range(n_coal):
            size[idx] = np.sum(bitmap[:, idx]) # Calculate size here for consistency
            # Pass a copy of the bitmap column to ensure no shared state issues if workers modify it (they shouldn't)
            tasks.append((idx, bitmap[:, idx].copy(), operators, prim, -np.inf))
        
        try:
            # Ensure the pool is properly managed, especially on Windows/macOS with 'spawn' or 'forkserver'
            # The 'spawn' start method (default on Windows, available on others) requires the worker 
            # function and its arguments to be picklable and defined at the top level of a module.
            # Our _solve_coalition_lp_worker is top-level.
            # Using context manager for the pool is good practice.
            num_processes = min(max(1, os.cpu_count() -1 ), n_coal) # Use n-1 cores or max n_coal workers
            print(f"Using {num_processes} worker processes.")

            with multiprocessing.Pool(processes=num_processes) as pool:
                svalue_results = pool.map(_solve_coalition_lp_worker, tasks)
            svalue = np.array(svalue_results)
        except Exception as e:
            print(f"Error during parallel processing: {e}")
            print("Falling back to serial execution for coalition valuation.")
            # Fallback to serial execution (copy of the original serial loop)
            for idx in range(n_coal):
                current_bitmap_col = bitmap[:, idx]
                size[idx] = np.sum(current_bitmap_col)
                svalue[idx] = _solve_coalition_lp_worker((idx, current_bitmap_col, operators, prim, -np.inf))

    else: # Serial execution
        print(f"Running coalition valuation serially for {n_ops} operators...")
        for idx in range(n_coal):
            current_bitmap_col = bitmap[:, idx]
            size[idx] = np.sum(current_bitmap_col)
            svalue[idx] = _solve_coalition_lp_worker((idx, current_bitmap_col, operators, prim, -np.inf))


    # (Rest of the Shapley calculation logic remains IDENTICAL to your original version)
    submask = (bitmap[:, None, :] <= bitmap[:, :, None]).all(axis=0)
    submask &= np.tri(n_coal, dtype=bool)
    base_p = operator_uptime ** size
    bp_masked = base_p * submask
    coef = csr_matrix((1, 1), dtype=int)
    for i in range(n_ops):
        sz = 2 ** i
        top = sp_hstack([coef, csr_matrix((sz, sz), dtype=int)])
        bottom = sp_hstack([-coef - diags([1]*sz, format="csr"), coef])
        coef = sp_vstack([top, bottom], format="csr").astype(int)
        coef.eliminate_zeros()
    coef_dense = coef.toarray()
    term = bp_masked @ (coef_dense * submask)
    part = (bp_masked + term) * submask
    evalue = (svalue * part).sum(axis=1)
    if n_coal > 0: # svalue[0] exists only if n_coal > 0
        evalue[0] = svalue[0] 

    shapley = np.zeros(n_ops)
    if n_ops > 0: # Avoid division by zero if n_ops is 0
        fact_n = math.factorial(n_ops)
        for k, op_k in enumerate(operators): # Use op_k to avoid confusion if 'op' is used elsewhere
            with_op = np.where(bitmap[k] == 1)[0] 
            without_op = with_op - (1 << k) 
            w = _fact(size[with_op] - 1) * _fact(n_ops - size[with_op]) / fact_n 
            shapley[k] = np.sum(w * (evalue[with_op] - evalue[without_op]))
    
    percent = np.maximum(shapley, 0)
    percent_sum = percent.sum()
    percent = percent / percent_sum if percent_sum > 0 else percent

    return pd.DataFrame({
        "Operator": operators if n_ops > 0 else ["NONE"], # Handle case of no operators
        "Value": np.round(shapley, 4) if n_ops > 0 else [0.0],
        "Percent": np.round(percent, 4) if n_ops > 0 else [0.0],
    })


