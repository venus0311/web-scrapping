import requests
import time
import re
from dotenv import load_dotenv
import os
import json


# Load .env variables
load_dotenv()

# 🔐 ENV VARS
NINJA_API_KEY = os.getenv("EMAIL_VERIFY_NINJA_KEY")
EMAIL_FINDER_KEY = os.getenv("EMAIL_FINDER_KEY")
FIND_AND_VALIDATE_EMAIL_KEY = os.getenv("EMAIL_FINDER_AND_VALIDATE_KEY")
GOOGLE_SEARCH_API_KEY = os.getenv("GOOGLE_SEARCH_API_KEY")
DETECT_ACTIVITY_API_KEY = os.getenv("DETECT_ACTIVITY_API_KEY")


class ApiError(Exception):
    pass


def get_company_by_domain(rapidapi_key, domain):
    """
    Get company info from API using company domain.
    """
    url = "https://web-scraping-api2.p.rapidapi.com/get-company-by-domain"
    querystring = {'domain': domain}
    headers = {
        'x-rapidapi-key': rapidapi_key,
        'x-rapidapi-host': 'web-scraping-api2.p.rapidapi.com'
    }

    try:
        response = requests.get(url, headers=headers, params=querystring)
               
        if response.status_code == 429:
            return "No more credits for domain API"
        if response.status_code == 403:
            return "Subscription is suspended"
        if response.status_code == 200:
            return response.json().get('data', {})
        else:
            print(f"Domain API request failed: {response.status_code} {response.text}", flush=True)
            return {}
    except Exception as e:
        print(f"Error fetching company data by domain: {e}", flush=True)
        return {}


def search_leads(rapidapi_key, payload):

    url = "https://web-scraping-api2.p.rapidapi.com/search-leads"

    headers = {
        "x-rapidapi-key": rapidapi_key,
        "x-rapidapi-host": "web-scraping-api2.p.rapidapi.com",
        "Content-Type": "application/json"
    }

    try:
        response = requests.post(url, json=payload, headers=headers)
        if response.status_code == 429:
            print(response.status_code, response.text, flush=True)
            raise ApiError("No request id for API")
        if response.status_code == 200:
            return response.json().get('request_id', {})
        else:
            print(f"Get request id API request failed: {response.status_code} {response.text}", flush=True)
            return {}
    except Exception as e:
        print(f"Error fetching request id: {e}", flush=True)
        return {}


def check_search_status(rapidapi_key, request_id):

    url = "https://web-scraping-api2.p.rapidapi.com/check-search-status"

    querystring = {"request_id": request_id}

    headers = { 
        "x-rapidapi-key": rapidapi_key,
        "x-rapidapi-host": "web-scraping-api2.p.rapidapi.com"
    }

    try:
        response = requests.get(url, headers=headers, params=querystring)
        if response.status_code == 429:
            print(response.status_code, response.text, flush=True)
            raise ApiError("No request id for API")
        if response.status_code == 200:
            return response.json()
            # return response.json().get('request_id', {})
        else:
            print(f"Get request id API request failed: {response.status_code} {response.text}", flush=True)
            return {}
    except Exception as e:
        print(f"Error fetching request id: {e}", flush=True)
        return {}
    

def wait_for_results(rapidapi_key, request_id, delay, max_retries):

    for attempt in range(max_retries):
        status_response = check_search_status(rapidapi_key, request_id)

        status = status_response.get("status")
        print(f"Attempt {attempt + 1}: Status = {status}", flush=True)

        if status == "done":
            print("✅ Search completed.")
            return status_response
        elif status == "failed":
            raise Exception("❌ Search failed.")
        elif status == "pending" or status == "processing":
            # print(f"⏳ Processing. Waiting {delay}s before next check...", flush=True)
            time.sleep(delay)
        else:
            print(f"⚠️ Unexpected status: {status}")
            break

    timeout_result = {
        "status": "Search did not complete in time." 
    }

    return timeout_result


def get_search_results(rapidapi_key, request_id):

    url = "https://web-scraping-api2.p.rapidapi.com/get-search-results"

    querystring = {"request_id": request_id,"page": "1"}

    headers = { 
        "x-rapidapi-key": rapidapi_key,
        "x-rapidapi-host": "web-scraping-api2.p.rapidapi.com"
    }

    try:
        response = requests.get(url, headers=headers, params=querystring)
        if response.status_code == 429:
            print(response.status_code, response.text, flush=True)
            raise ApiError("No results for API")
        if response.status_code == 200:
            return response.json()
            # return response.json().get('request_id', {})
        else:
            print(f"Get request id API request failed: {response.status_code} {response.text}", flush=True)
            return {}
    except Exception as e:
        print(f"Error fetching request id: {e}", flush=True)
        return {}
    

# def find_email(person_data):
#     """
#     Find email for lead.
#     """
#     url = "https://email-finder7.p.rapidapi.com/email-address/find-one/"

#     first_name = person_data["first_name"].strip()
#     last_name = person_data["last_name"].strip()
#     domain = person_data["domain"].strip()

#     suffixes = [
#         "CPA", "MD", "PhD", "JD", "RN", "CEO", "CFO", "COO", "CTO", "PMP",
#         "CFA", "CFE", "CFI", "CISA", "CMA", "CSM", "Esq.", "DDS", "DO", "DVM",
#         "MBA", "BSc", "MSc", "Eng.", "LLM", "ACCA", "CA", "NP", "PA", "RPh",
#         "CRC", "CHRP", "MCSE", "AWS", "GCP", "CISSP"
#     ]

#     suffix_pattern = r'(\s*,\s*|\s+)?(' + '|'.join(r'\b' + re.escape(s) + r'\b' for s in suffixes) + r')(\s*,\s*|\s+)?'
#     domain = re.sub(r'^(https?://)?(www\.)?', '', domain).rstrip('/')

#     if first_name and not re.fullmatch(r'\b(' + '|'.join(re.escape(s) for s in suffixes) + r')\b', first_name, flags=re.IGNORECASE):
#         first_name = re.sub(suffix_pattern, '', first_name, flags=re.IGNORECASE).strip()
#     if last_name and not re.fullmatch(r'\b(' + '|'.join(re.escape(s) for s in suffixes) + r')\b', last_name, flags=re.IGNORECASE):
#         last_name = re.sub(suffix_pattern, '', last_name, flags=re.IGNORECASE).strip()

#     payload = {
#         "personFirstName": first_name,
#         "personLastName": last_name,
#         "domain": domain
#     }

#     headers = {
#         "x-rapidapi-key": EMAIL_FINDER_KEY,
#         "x-rapidapi-host": "email-finder7.p.rapidapi.com"
#     }

#     try:
#         response = requests.get(url, headers=headers, params=payload, timeout=5)

#         print(f"&&&&&&& {response}")

#         if response.status_code == 429:
#             print("❌ 429 Too Many Requests")
#             return 429
        
#         if response.status_code == 522:
#             print("❌ 522 Server error find email")
#             return 522
        
#         if response.status_code == 200:
#             json_response = response.json()
#             print(f"5555555555555555555555555555 {json_response}")
#             return json_response.get("payload", None).get("data", None)

#         # Safe to raise other HTTP errors
#         # response.raise_for_status()

#         # if not response.text:
#         #     print(f"API Error: Empty response. Status: {response.status_code}, Payload: {payload}")
#         #     return {}

#         # try:
#         #     json_response = response.json()
#         #     if not isinstance(json_response, dict):
#         #         print(f"⚠️ Unexpected JSON format: {type(json_response)}: {response.text}")
#         #         return {}
#         #     return json_response.get("payload", {}).get("data", {})
#         # except ValueError:
#         #     print(f"❌ Invalid JSON")
#         #     return None

#     except requests.RequestException as e:
#         print(f"🌐 Request failed: {e}")
#         return None


def find_email(person_data):
    """
    Safely find email for a lead using the RapidAPI Email Finder service.
    Handles invalid responses, timeouts, and inconsistent API structures.
    """

    url = "https://email-finder7.p.rapidapi.com/email-address/find-one/"

    first_name = person_data.get("first_name", "").strip()
    last_name = person_data.get("last_name", "").strip()
    domain = person_data.get("domain", "").strip()

    # --- Clean name and domain ---
    suffixes = [
        "CPA", "MD", "PhD", "JD", "RN", "CEO", "CFO", "COO", "CTO", "PMP",
        "CFA", "CFE", "CFI", "CISA", "CMA", "CSM", "Esq.", "DDS", "DO", "DVM",
        "MBA", "BSc", "MSc", "Eng.", "LLM", "ACCA", "CA", "NP", "PA", "RPh",
        "CRC", "CHRP", "MCSE", "AWS", "GCP", "CISSP"
    ]
    suffix_pattern = r'(\s*,\s*|\s+)?(' + '|'.join(r'\b' + re.escape(s) + r'\b' for s in suffixes) + r')(\s*,\s*|\s+)?'

    domain = re.sub(r'^(https?://)?(www\.)?', '', domain).rstrip('/')

    if first_name and not re.fullmatch(r'\b(' + '|'.join(re.escape(s) for s in suffixes) + r')\b', first_name, flags=re.IGNORECASE):
        first_name = re.sub(suffix_pattern, '', first_name, flags=re.IGNORECASE).strip()
    if last_name and not re.fullmatch(r'\b(' + '|'.join(re.escape(s) for s in suffixes) + r')\b', last_name, flags=re.IGNORECASE):
        last_name = re.sub(suffix_pattern, '', last_name, flags=re.IGNORECASE).strip()

    payload = {
        "personFirstName": first_name,
        "personLastName": last_name,
        "domain": domain
    }

    headers = {
        "x-rapidapi-key": EMAIL_FINDER_KEY,
        "x-rapidapi-host": "email-finder7.p.rapidapi.com"
    }

    try:
        response = requests.get(url, headers=headers, params=payload, timeout=10)
        # print(f"🌐 Response status: {response.status_code}")

        # --- Handle known HTTP errors ---
        if response.status_code == 429:
            print("❌ Too Many Requests (429)")
            return 429

        if response.status_code == 522:
            print("❌ Server error (522)")
            return 522

        # --- Only parse JSON if 200 ---
        if response.status_code == 200:
            try:
                json_response = response.json()
            except ValueError:
                print("❌ Invalid JSON in response")
                return None

            # print(f"📩 JSON response: {json_response}")

            # Handle all safe cases
            if not isinstance(json_response, dict):
                # print("⚠️ Unexpected JSON format")
                return None

            payload = json_response.get("payload", {})
            if not isinstance(payload, dict):
                # print("⚠️ Invalid 'payload' structure")
                return None

            data = payload.get("data")
            if not data:
                # print("⚠️ Empty or missing data in response")
                return None

            return data  

        # print(f"⚠️ Unexpected status code: {response.status_code}")
        return None

    except requests.Timeout:
        print("⏰ Request timed out")
        return None

    except requests.RequestException as e:
        print(f"🌐 Request failed: {e}")
        return None


def verify_email(rapidapi_key, email):
    
    url = "https://validect-email-verification-v1.p.rapidapi.com/v1/verify"

    querystring = {"email": email}
    headers = { 
        "x-rapidapi-key": rapidapi_key,
        "x-rapidapi-host": "validect-email-verification-v1.p.rapidapi.com"
    }

    try:
        response = requests.get(url, headers=headers, params=querystring)
        if response.status_code == 429:
            print(response.status_code, response.text, flush=True)
            raise ApiError("No results for API")
        if response.status_code == 200:
            return response.json()
            # return response.json().get('request_id', {})
        else:
            print(f"Get request id API request failed: {response.status_code} {response.text}", flush=True)
            return {}
    except Exception as e:
        print(f"Error fetching request id: {e}", flush=True)
        return {}
    

def find_and_validate_email(person_data):
    """
    Find and validate email for lead.
    """
    url = "https://email-finder11.p.rapidapi.com/v2/email/finder"
    api_key = FIND_AND_VALIDATE_EMAIL_KEY

    person_name = person_data["query"].strip()
    company_name = person_data["company_name"].strip()
    company_domain = person_data["company_domain"].strip()

    payload = {
        "query": person_name,
        "company_name": company_name,
        "company_domain": company_domain
    }

    headers = {
        "x-rapidapi-key": api_key,
        "x-rapidapi-host": "email-finder11.p.rapidapi.com"
    }

    try:
        response = requests.get(url, headers=headers, params=payload)

        # 👉 Check for 429 BEFORE raise_for_status
        if response.status_code == 429:
            print("❌ 429 Too Many Requests")
            return 429
        if response.status_code == 403:
            print("❌ 403 Too Many Requests")
            return 403

        # Safe to raise other HTTP errors
        # response.raise_for_status()

        if not response.text:
            print(f"API Error: Empty response. Status: {response.status_code}, Payload: {payload}")
            return {}

        # try:
        #     json_response = response.json()
        #     if not isinstance(json_response, dict):
        #         print(f"⚠️ Unexpected JSON format: {type(json_response)}: {response.text}")
        #         return {}
        #     return json_response
        # except ValueError:
        #     print(f"❌ Invalid JSON")
        #     return None

    except requests.RequestException as e:
        print(f"🌐 Request failed: {e}")
        return None
    

# Email generator alternative method
# def get_token_for_ninja(api_key: str) -> str:
#     """Fetch a 24-hour token from MailTester.Ninja"""
#     url = f"https://token.mailtester.ninja/token?key={api_key}"
#     response = requests.get(url)
#     response.raise_for_status()
#     data = response.json()

#     if "token" not in data:
#         raise Exception(f"Failed to get token: {data}")
#     return data["token"]


# def verify_email_after_manual(email: str, token: str) -> dict:
#     """Verify a single email address"""
#     url = f"https://happy.mailtester.ninja/ninja?email={email}&token={token}"
#     response = requests.get(url)
#     response.raise_for_status()
#     return response.json()


# def generate_emails_alternative(first_name: str, last_name: str, domain: str):
#     """Generate possible email patterns"""
#     first = first_name.lower()
#     last = last_name.lower()
#     initials = first[0]

#     patterns = [
#         f"{first}@{domain}",
#         f"{last}@{domain}",
#         f"{first}{last}@{domain}",
#         f"{first}.{last}@{domain}",
#         f"{initials}{last}@{domain}",
#         f"{first}{initials}@{domain}",
#     ]

#     return list(set(patterns))  # ensure unique


# def generate_emails_alternative(first_name: str, last_name: str, domain: str):
#     """Generate possible email patterns (advanced version)"""
#     fn = first_name.lower()
#     ln = last_name.lower()
#     fi = fn[0]
#     li = ln[0]

#     patterns = [
#         f"{fn}@{domain}",
#         f"{ln}@{domain}",
#         f"{fn}{ln}@{domain}",
#         f"{fn}.{ln}@{domain}",
#         f"{fi}{ln}@{domain}",
#         f"{fi}.{ln}@{domain}",
#         f"{fn}{li}@{domain}",
#         f"{fn}.{li}@{domain}",
#         f"{fi}{li}@{domain}",
#         f"{fi}.{li}@{domain}",
#         f"{ln}{fn}@{domain}",
#         f"{ln}.{fn}@{domain}",
#         f"{ln}{fi}@{domain}",
#         f"{ln}.{fi}@{domain}",
#         f"{li}{fn}@{domain}",
#         f"{li}.{fn}@{domain}",
#         f"{li}{fi}@{domain}",
#         f"{li}.{fi}@{domain}",
#         f"{fn}-{ln}@{domain}",
#         f"{fi}-{ln}@{domain}",
#         f"{fn}-{li}@{domain}",
#         f"{fi}-{li}@{domain}",
#         f"{ln}-{fn}@{domain}",
#         f"{ln}-{fi}@{domain}",
#         f"{li}-{fn}@{domain}",
#         f"{li}-{fi}@{domain}",
#         f"{fn}_{ln}@{domain}",
#         f"{fi}_{ln}@{domain}",
#         f"{fn}_{li}@{domain}",
#         f"{fi}_{li}@{domain}",
#         f"{ln}_{fn}@{domain}",
#         f"{ln}_{fi}@{domain}",
#         f"{li}_{fn}@{domain}",
#         f"{li}_{fi}@{domain}",
#     ]

#     return list(set(patterns))  # ensure unique


# def find_email_manual(person_data: dict):
#     """
#     Find a valid email for a given person using MailTester API.

#     person_data must contain:
#       - first_name
#       - last_name
#       - domain
#     """
#     api_key = NINJA_API_KEY
#     token = get_token_for_ninja(api_key)

#     candidates = generate_emails_alternative(
#         person_data["first_name"], person_data["last_name"], person_data["domain"]
#     )

#     for email in candidates:
#         result = verify_email_after_manual(email, token)
#         if result.get("code") == "ok":
#             return {
#                 "found_email": email,
#                 "details": result
#             }

#     return {
#         "found_email": "email not found",
#         "details": None
#     }


def get_revenue(company):
    """
    Get revenue of company via google search (scrape from zoominfo.com/).
    """
    headers = {"X-API-KEY": GOOGLE_SEARCH_API_KEY, "Content-Type": "application/json"}

    query = f"site:zoominfo.com/c {company} revenue"

    res = requests.post(
        "https://google.serper.dev/search",
        headers=headers,
        json={"q": query}
    )

    data = res.json()

    if "organic" not in data:
        return None

    for item in data["organic"]:
        snippet = item.get("snippet", "")

        # Match "revenue <5M", "revenue < $5 million", or "revenue less than $5M"
        less_match = re.search(
            r"revenue[^$0-9<]{0,10}(?:<|less\s+than)\s*(\$?\s*[0-9,.]+(?:\s?(?:million|billion|trillion|m|bn|b))?)",
            snippet,
            re.I
        )
        if less_match:
            value = less_match.group(1).strip()
            return f"less {value}", item.get("link")

        # Normal case like "revenue $5 million"
        match = re.search(
            r"revenue[^$0-9]{0,10}(\$?\s*[0-9,.]+(?:\s?(?:million|billion|trillion|m|bn|b))?)",
            snippet,
            re.I
        )
        if match:
            return match.group(1).strip(), item.get("link")

    return None


def get_profile_activity(linkedin_url):
    """
    Get profile activity from API using url.
    """
    url = "https://web-scraping-api2.p.rapidapi.com/get-profile-recent-activity-time"
    
    querystring = {
        'linkedin_url': linkedin_url
    }
    
    headers = {
        'x-rapidapi-key': DETECT_ACTIVITY_API_KEY,
        "X-RapidAPI-Host": "web-scraping-api2.p.rapidapi.com"
    }

    try:
        response = requests.get(url, headers=headers, params=querystring)
        
        if response.status_code == 429:
            return "No more credits for domain API"
        if response.status_code == 403:
            return "Subscription is suspended"
        if response.status_code == 200:
            return response.json().get('data', {})
        else:
            print(f"Domain API request failed: {response.status_code} {response.text}", flush=True)
            return {}
    except Exception as e:
        print(f"Error fetching company data by domain: {e}", flush=True)
        return {}


def get_search_results_by_serper(company_name, location="us"):

    url = "https://google.serper.dev/search"

    payload = json.dumps({
        "q": f"(site:linkedin.com/company OR site:linkedin.com/school OR site:linkedin.com/showcase) {company_name} industry size",
        "gl": location,
        })
    headers = {
        'X-API-KEY': GOOGLE_SEARCH_API_KEY,
        'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    return response.text


def get_company_domain(api_key, linkedin_url):
    """
    Get company domain using linkedin url.
    """
    url = "https://web-scraping-api2.p.rapidapi.com/get-company-by-url"
    
    querystring = {
        'linkedin_url': linkedin_url
    }
    
    headers = {
        "x-rapidapi-key": api_key,
        "x-rapidapi-host": "web-scraping-api2.p.rapidapi.com"
    }

    try:
        response = requests.get(url, headers=headers, params=querystring)
        
        if response.status_code == 429:
            return "No more credits for domain API"
        if response.status_code == 403:
            return "Subscription is suspended"
        if response.status_code == 200:
            return response.json().get('data', {})
        else:
            print(f"Domain API request failed: {response.status_code} {response.text}", flush=True)
            return {}
    except Exception as e:
        print(f"Error fetching company data by domain: {e}", flush=True)
        return {}
