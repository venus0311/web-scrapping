from fixed_data.countries_code import country_codes
import json
from fixed_data.industries import find_matching_industry
from api_calls import get_search_results_by_serper
from rapidfuzz import fuzz, process
import re


def get_company_info_from_names(company_names, country_names):
    """
    Get company domains using company name.
    """
    result = []
    links = []

    for company_name, country_name in zip(company_names, country_names):

        country_code = country_codes.get(country_name, "us") if country_name else "us"   
        country_code = get_country_code(country_name)    
        response = get_search_results_by_serper(company_name, location=country_code)
        response_data = json.loads(response)

        if "organic" in response_data:
            best_match = None
            best_match_score = 0

            for item in response_data["organic"]:
                title = item.get("title", "").lower()
                snippet = item.get("snippet", "").lower()
                link = item.get("link", "")

                links.append(link)

                match_score = sum(1 for word in company_name.lower().split() if word in title or word in snippet)
                if match_score > best_match_score:
                    best_match_score = match_score
                    best_match = {
                        "title": title,
                        "snippet": snippet,
                        "link": link
                    }

            if best_match:

                find_link = company_best_link(company_name, links)

                company_info = extract_company_info(best_match["snippet"])

                # print(f"TTTTT {company_info}")
                # print(f"........ {best_match["snippet"]}")

                formatted_subindustry = title_case_except_and(company_info["industry"])
                matched_industry = find_matching_industry(formatted_subindustry)

                industries = matched_industry if matched_industry != "Unknown" else None

                collected_data = {
                    "industry": industries,  
                    "subindustry": formatted_subindustry,          
                    "employees": company_info["company_size"],
                    "link": find_link["url"],
                    "company": find_link["company"]
                }

                result.append(collected_data)
    
    return result


def company_best_link(company_name, links):
    """
    Find best matched linkedin link for company.
    """
    def extract_slug(url):
        match = re.search(r"/company/([^/?]+)", url)
        return match.group(1).lower() if match else None

    slugs = [extract_slug(url) for url in links]
    best_match, score, index = process.extractOne(
        company_name.lower(),
        slugs,
        scorer=fuzz.partial_ratio
    )
    return {
        "company": company_name,
        "url": links[index]
    }


def get_country_code(country_name: str) -> str:
    """
    Return the 2-letter code for a given country name.
    """
    return country_codes.get(country_name.strip(), "us")


def extract_employee_range(text: str) -> str:
    """
    Extract employee range (e.g., '1-10', '11-50', '100+') from a given text.
    """
    match = re.search(r'\b\d{1,3}(?:-\d{1,3}|\+)\b', text)
    return match.group(0) if match else "-"


def extract_company_info(text):
    """
    Extract company industry and size (from both 'company size:' and 'employs ...' patterns).
    Removes commas from size but keeps '+'.
    """
    info = {}

    # --- Extract industry ---
    industry = re.search(r"industry:\s*([^;.\n]+)", text, re.IGNORECASE)
    info["industry"] = industry.group(1).strip() if industry else None

    # --- Extract company size (2 possible patterns) ---
    size = re.search(r"company size:\s*([^;.\n]+)", text, re.IGNORECASE)

    if not size:
        size = re.search(
            r"employs\s+(?:about|approximately)?\s*([\d,]+)\+?\s*(?:people|employees)?",
            text,
            flags=re.IGNORECASE
        )

    info["company_size"] = size.group(1).strip() if size else "-"

    # --- Clean up company size ---
    if info["company_size"]:
        # Remove the word 'employees' but keep '+', remove commas
        info["company_size"] = re.sub(
            r"employees?$", "", info["company_size"], flags=re.IGNORECASE
        )
        info["company_size"] = info["company_size"].replace(",", "").strip()

    return info


def title_case_except_and(text):
    if not text:
        return text
    return " ".join(
        w if w == "and" else w.capitalize()
        for w in text.lower().split()
    )





