from fixed_data.sizes import SIZE_RANGES
from urllib.parse import urlparse
import re
import math
import json
from fixed_data.levels import all_job_levels, job_level_seniority, equal_levels_map


def remove_duplicates(values):
    """
    Remove duplicates from a list and return:
    - unique: first occurrences only
    - duplicates: all extra repeated items (not deduplicated)
    """
    seen = set()
    unique = []
    duplicates = []

    for item in values:
        if item in seen:
            duplicates.append(item)  
        else:
            seen.add(item)
            unique.append(item)

    return unique, duplicates


def ranges_overlap(r1, r2):
    """Check if two ranges overlap."""
    return r1[0] <= r2[1] and r2[0] <= r1[1]


def is_real_range_matching(user_ranges, real_range_str):
    real_range_str = real_range_str.strip()

    # Case 1: "10000+" → [10000, inf]
    if real_range_str.endswith("+"):
        try:
            start = int(real_range_str.rstrip("+"))
        except ValueError:
            return False
        real_range = [start, math.inf]

    # Case 2: "start-end"
    elif "-" in real_range_str:
        try:
            real_start, real_end = map(int, real_range_str.split("-"))
        except ValueError:
            return False
        real_range = [real_start, real_end]

    # Case 3: exact number "45"
    else:
        try:
            num = int(real_range_str)
        except ValueError:
            return False
        real_range = [num, num]

    for user_range in user_ranges:
        # Single number in user input (rare, but keep)
        if isinstance(user_range, (int, float)):
            if real_range[0] <= user_range <= real_range[1]:
                return True

        # If user input is [45, 45] (exact match)
        elif user_range[0] == user_range[1]:
            if real_range[0] <= user_range[0] <= real_range[1]:
                return True

        # Normal overlap
        elif ranges_overlap(user_range, real_range):
            return True

    return False


def normalize_number(value):
    """
    Convert strings like '3 million', '3mln', '3mil.', '3k' to int.
    """
    if isinstance(value, str):
        value = value.strip().lower()
        value = re.sub(r"[,\s]+", "", value)  # remove commas/spaces

        # Map messy words to k/m/b
        replacements = {
            r"(thousand|k$)": "k",
            r"(mil+ion?|mil+|mln|m$)": "m",
            r"(bil+ion?|bil+|bln|b$)": "b"
        }
        for pattern, suffix in replacements.items():
            value = re.sub(pattern, suffix, value)

        # Extract numeric and suffix
        match = re.match(r"([0-9]*\.?[0-9]+)([kmb]?)", value)
        if not match:
            raise ValueError(f"Cannot parse: {value}")

        num, suffix = match.groups()
        num = float(num)

        multiplier = {"": 1, "k": 1_000, "m": 1_000_000, "b": 1_000_000_000}
        return int(num * multiplier[suffix])

    return int(value)


def values_match(user_value, real_value):
    """
    Check if real_value matches or is inside a user-provided range.
    """
    cleaned_real_value = re.sub(r"\bless\b|\$", "", real_value, flags=re.I).strip()  # remove "$" symbol and "less" word

    if isinstance(user_value, str) and "-" in user_value:
        # Normalize dash variants and split
        user_value = user_value.replace("–", "-").replace("—", "-")
        start_str, end_str = user_value.split("-", 1)
        start = normalize_number(start_str)
        end = normalize_number(end_str)
        real = normalize_number(cleaned_real_value)

        if "less" not in real_value:
            return start <= real <= end
        else:
            return start <= real >= end
    else:
        return normalize_number(user_value) == normalize_number(cleaned_real_value)


def compare_data(input_data, scraped_data):
    """
    Compare user input data requirements and real data from API.
    """    
    failed_reasons = []

    # INDUSTRY /////////////////////////////////////////////////////////////////////////////////////////
    input_industry = input_data["input_industry"]
    scraped_industry = scraped_data["industry"]

    if "any" not in input_industry:
        if scraped_industry == "-" or scraped_industry not in input_industry:
            failed_reasons.append("h industry")

    # REVENUE /////////////////////////////////////////////////////////////////////////////////////////
    input_revenue = input_data["input_revenue"].strip().lower()
    scraped_revenue = scraped_data["revenue"].strip().lower()
    
    if "any" not in input_revenue:
        if scraped_revenue == "-":
            failed_reasons.append("h revenue")
        else:
            try:
                if not values_match(input_revenue, scraped_revenue):
                    failed_reasons.append("h revenue")
            except ValueError:
                failed_reasons.append("h revenue")

    # SIZE ///////////////////////////////////////////////////////////////////////////////////////////
    input_size = input_data["input_size"].strip().lower().replace(" ", "")  
    scraped_size = scraped_data["employees"].strip().lower()

    if "any" not in input_size.lower():
        # Prepare user ranges list
        user_ranges = []

        if input_size.endswith("+"):
            # Example: "50+"
            try:
                min_value = int(input_size.rstrip("+"))
            except ValueError:
                return []

            # All SIZE_RANGES that end >= min_value
            user_ranges = [[start, end] for start, end in SIZE_RANGES if end >= min_value]

        elif "-" in input_size:
            # Example: "11-50"
            try:
                start, end = map(int, input_size.split("-"))
            except ValueError:
                return []
            user_ranges = [[start, end]]

        else:
            # Example: "45"
            try:
                num = int(input_size)
            except ValueError:
                return []
            user_ranges = [[num, num]]

        # Now check match for all cases
        if not is_real_range_matching(user_ranges, scraped_size):
            failed_reasons.append("h size")

    # FINAL RESULT
    compare_data_info = {
        "is_valid": len(failed_reasons) == 0,
        "status": ", ".join(failed_reasons) if failed_reasons else "valid"
    }

    return compare_data_info


def compare_revenue_data(input_data, scraped_data):
    """
    Compare user input revenue and real revenue.
    """    
    failed_reasons = []

    # REVENUE /////////////////////////////////////////////////////////////////////////////////////////
    input_revenue = input_data["input_revenue"].strip().lower()
    scraped_revenue = scraped_data["revenue"].strip().lower()
    
    if "any" not in input_revenue:
        if scraped_revenue == "-":
            failed_reasons.append("h revenue")
        else:
            try:
                if not values_match(input_revenue, scraped_revenue):
                    failed_reasons.append("h revenue")
            except ValueError:
                failed_reasons.append("h revenue")

    # FINAL RESULT
    compare_data_info = {
        "is_valid": len(failed_reasons) == 0,
        "status": ", ".join(failed_reasons) if failed_reasons else "valid"
    }

    return compare_data_info


def get_clean_domain(raw):
    """
    Clean domain from additional symbols, letters.
    """
    # Replace backslashes with slashes for safety
    raw = raw.replace("\\", "/")
    
    # If missing scheme, add http:// so urlparse can work correctly
    if not raw.startswith(("http://", "https://")):
        raw = "http://" + raw

    parsed = urlparse(raw)
    domain = parsed.netloc or parsed.path  # Handles inputs without scheme
    
    # Remove "www."
    if domain.startswith("www."):
        domain = domain[4:]

    return domain


# /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
# //////////////////////////////////////////////////// Define levels //////////////////////////////////////////////////////////////
# /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

def get_job_levels(level1, level2, level3):
    """
    Modify job levels.
    """
    if level1:
        return level1
    else:
        if level2:
            lev_2 = level2[0]
        else:
            lev_2 = ""

        if level3:
            lev_3 = level3[0]
        else:
            lev_3 = ""

        return get_sublevels_in_range_by_sublevel(lev_2, lev_3)


def get_sublevels_in_range_by_sublevel(start_sub, end_sub=None):
    # 1️⃣ Flatten sublevels in correct order
    ordered_sublevels = []
    for level in job_level_seniority:
        ordered_sublevels.extend(all_job_levels[level])

    # 2️⃣ Ensure start_sub exists
    if start_sub not in ordered_sublevels:
        raise ValueError(f"Invalid start sublevel: {start_sub}")
    start_idx = ordered_sublevels.index(start_sub)

    # 3️⃣ Handle end_sub (if not provided → take until last)
    if not end_sub:
        end_idx = len(ordered_sublevels) - 1
    else:
        if end_sub not in ordered_sublevels:
            raise ValueError(f"Invalid end sublevel: {end_sub}")
        end_idx = ordered_sublevels.index(end_sub)

    # 4️⃣ Ensure correct order
    if start_idx > end_idx:
        start_idx, end_idx = end_idx, start_idx

    return ordered_sublevels[start_idx:end_idx + 1]


def _normalize(text: str) -> str:
    """Normalize text: lowercase, remove punctuation, expand abbreviations."""
    if text is None:
        return ""
    text = text.lower().strip()
    # normalize common abbreviations
    text = re.sub(r'\b(sr|sr\.)\b', 'senior', text)
    text = re.sub(r'\b(avp)\b', 'assistant vice president', text)
    # remove punctuation (replace with space so word boundaries remain)
    text = re.sub(r'[^\w\s]', ' ', text)
    # collapse multiple spaces
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def map_to_sublevel(title: str, ordered_sublevels):
    """
    Map a title string to the best-matching sublevel.
    Handles modifiers (associate, deputy, general, senior, etc.).
    Returns candidate sublevel(s) (normalized).
    """
    norm_title = _normalize(title)
    ordered_norm = [_normalize(s) for s in ordered_sublevels]

    # find matches inside title
    matches = []
    for sub_norm in ordered_norm:
        if re.search(r'\b' + re.escape(sub_norm) + r'\b', norm_title):
            matches.append(sub_norm)

    if not matches:
        return []

    # pick longest match (e.g., "associate director" > "director")
    best = max(matches, key=len)

    # base candidate: remove modifiers
    modifiers = {"general", "assistant", "associate", "deputy", "chief", "lead", "senior", "sr"}
    best_words = best.split()
    base_words = [w for w in best_words if w not in modifiers]
    base = " ".join(base_words).strip()

    candidates = [best]
    if base and base != best:
        candidates.append(base)

    # special rule: "general manager" → "director"
    if "general" in best_words and base == "manager":
        candidates.append("director")

    # also allow removing *only* first modifier
    if best_words[0] in modifiers and len(best_words) > 1:
        candidates.append(" ".join(best_words[1:]))

    # return normalized unique candidates
    return list({ _normalize(c) for c in candidates })


# Old version for only check without equal matching levels
# def check_lead_title(title, levels):
#     """
#     Check if API title matches any of the user-selected sublevels (levels).
#     """
#     # normalize levels into a set
#     levels_norm = { _normalize(l) for l in levels }

#     # quick direct match
#     if _normalize(title) in levels_norm:
#         return True

#     # build ordered sublevels list
#     ordered_sublevels = []
#     for lvl in job_level_seniority:
#         ordered_sublevels.extend(all_job_levels.get(lvl, []))

#     # try to map title to sublevel candidates
#     candidates = map_to_sublevel(title, ordered_sublevels)

#     for cand in candidates:
#         if cand in levels_norm:
#             return True

#     # fallback: substring check
#     norm_title = _normalize(title)
#     for lvl_norm in levels_norm:
#         if re.search(r'\b' + re.escape(lvl_norm) + r'\b', norm_title):
#             return True

#     return False


# -------------------------------
# NEW: expand equivalents helper
# -------------------------------
def expand_levels_with_equivalents(levels, equal_map):
    """
    Expand user-selected levels with equivalents from equal_map.
    This adds:
      - values for any selected key (key -> values)
      - the key + all its values if a selected level matches a value (value -> key + values)
    Returns a list (unique) of expanded level strings (original forms from input and map).
    """
    if not levels:
        return []

    expanded = set(levels)  

    for sel in list(expanded):
        sel_norm = _normalize(sel)
        for k, vals in equal_map.items():
            k_norm = _normalize(k)
            # If selected equals the key -> add its values
            if sel_norm == k_norm:
                expanded.update(vals)
            else:
                # if selected equals one of values -> add the key and all values
                for v in vals:
                    if sel_norm == _normalize(v):
                        expanded.add(k)
                        expanded.update(vals)
                        break

    newly_added = set(expanded)
    for sel in list(newly_added):
        sel_norm = _normalize(sel)
        for k, vals in equal_map.items():
            if sel_norm == _normalize(k):
                expanded.update(vals)
            else:
                for v in vals:
                    if sel_norm == _normalize(v):
                        expanded.add(k)
                        expanded.update(vals)
                        break

    return list(expanded)


# -----------------------------
# Main level matching function 
# -----------------------------
def check_lead_title(title, levels):
    """
    Check if API title matches any of the user-selected sublevels (levels).
    This now expands levels with equivalents from equal_levels_map before matching.
    """
    # 1) Expand the user-selected levels with equivalents
    expanded_levels = expand_levels_with_equivalents(levels, equal_levels_map)

    # 2) normalize levels into a set
    levels_norm = { _normalize(l) for l in expanded_levels }

    # 3) quick direct match
    if _normalize(title) in levels_norm:
        return True

    # 4) build ordered sublevels list
    ordered_sublevels = []
    for lvl in job_level_seniority:
        ordered_sublevels.extend(all_job_levels.get(lvl, []))

    # 5) try to map title to sublevel candidates
    candidates = map_to_sublevel(title, ordered_sublevels)
    for cand in candidates:
        if cand in levels_norm:
            return True

    # 6) fallback: substring check
    norm_title = _normalize(title)

    for lvl_norm in levels_norm:
        if re.search(r'\b' + re.escape(lvl_norm) + r'\b', norm_title):
            return True

    return False


def same_word(a: str, b: str) -> bool:
    normalize = lambda s: s.lower().replace(".", "").replace(" ", "")
    return normalize(a) == normalize(b)

