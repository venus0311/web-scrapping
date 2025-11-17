from models import SessionLocal, ProcessEntry
from google_service.sheet_processor import write_results_in_tab, flush_all_buffers, register_flush_on_exit
from utils.utils import *
from utils.search_by_name import get_company_info_from_names
from openai_service.ai_cleaner import translate_title
import json
import random
from api_calls import *
from fixed_data.industries import find_matching_industry
from fixed_data.list_of_industries import INDUSTRIES as all_industries
from fixed_data.countries_id import country_ids
from google_service.utils import *
from typing import Dict, Any
from cache_manager import store_processed_data, delete_processed_data
from models import SessionLocal, ProcessEntry, ProcessItem
from fastapi.responses import JSONResponse


# Load .env variables
load_dotenv()

# ENV VARS
API_KEY = os.getenv("RAPIDAPI_KEY")
API_KEY_VERIFY = os.getenv("RAPIDAPI_KEY_VERIFY")


def process_entry_logic(entry_id: str):
    """
    Core processing logic for an entry.
    Can be used by both /process (new) and /resume (stopped).
    """
    db = SessionLocal()

    try:
        # Get main entry
        entry = db.query(ProcessEntry).get(entry_id)
        if not entry:
            print(f"Entry {entry_id} not found")
            return

        # Optional: mark entry as in progress
        entry.status = "In Progress"
        db.commit()

        # Load user input from entry.input_data
        raw_data = entry.input_data or {}
        if isinstance(raw_data, str):
            data = json.loads(raw_data)
        else:
            data = raw_data

        is_company_geo_required = data.get("company_geo", False) 

        sheet_url = data.get("sheet_url")
        process_type = data.get("process_type")

        print(f"ENTRY ID FIRST :::: {entry_id}")
        print(f"$$$$$$$$$ {entry.items}")

        suitable_results = []
        unsuitable_results = []

        sup_names_sheet_url = data["sup_names_sheet_url"]

        if sup_names_sheet_url:
            sup_names = read_sup_names(sup_names_sheet_url)

            if sup_names == "The 'sup name' column isn't found.":
                entry.status = "Failed"
                entry.error_message = "The 'sup name' column isn't found."
                db.commit()
                db.refresh(entry)

                raise Exception("The 'sup name' column isn't found.")
        else:
            sup_names = None

        # Insert ProcessItem rows first time if none exist (--Project runs first time)
        if not entry.items:

            # Mixed
            if process_type == "search_mixed": 
            
                try:
                    read_sheet_response = read_company_data_mixed(sheet_url)
                except RuntimeError:
                    return JSONResponse(
                        status_code=400,
                        content={"error": "no_edit"}
                    )

                if read_sheet_response["message"]:
                    entry.status = "Failed"
                    entry.error_message = read_sheet_response["message"]
                    db.commit()
                    db.refresh(entry)

                    raise Exception(read_sheet_response["message"])
                
                existing_columns = read_sheet_response["existing_columns"]

                if "domain" in existing_columns:
                    process_type = "search_by_domain"
                else:
                    process_type = "search_by_name"
            
            # Search by company domain
            if process_type == "search_by_domain": 
            
                try:
                    read_sheet_response = read_company_domains(sheet_url, is_company_geo_required)
                except RuntimeError:
                    return JSONResponse(
                        status_code=400,
                        content={"error": "no_edit"}
                    )
                
                if not read_sheet_response["message"]: 
                    values = read_sheet_response["domains"]
                    for val in values:
                        db.add(ProcessItem(entry_id=entry_id, value=val, status="unprocessed"))
                    db.commit()
                    db.refresh(entry)  

                else:
                    entry.status = "Failed"
                    entry.error_message = read_sheet_response["message"]
                    db.commit()
                    db.refresh(entry)

                    raise Exception(read_sheet_response["message"])
            
            # Search by company name
            if process_type == "search_by_name": 
            
                try:
                    read_sheet_response_by_name = read_company_names(sheet_url, is_company_geo_required)
                except RuntimeError:
                    return JSONResponse(
                        status_code=400,
                        content={"error": "no_edit"}
                    )
                
                sheet_for_names = read_sheet_response_by_name["sheet"]
                
                if not read_sheet_response_by_name["message"]: 
                    values = read_sheet_response_by_name["names"]

                    locations = read_sheet_response_by_name["locations"]

                    if len(locations) == 0:
                        locations = ["usa"] * len(values)

                    company_short_data = get_company_info_from_names(values, locations)
                    
                    all_names = []
                    for y in company_short_data:
                        all_names.append(y["company"])

                    valid_domains_from_names = []

                    unique_values, duplicate_values = remove_duplicates(all_names)

                    uniquq_links_and_names = {}
                    for d in company_short_data:
                        if d["company"] in unique_values:
                            uniquq_links_and_names[d["company"]] = d["link"]  

                    if duplicate_values:
                        for dup_val in duplicate_values:
                            unsuitable_data = {
                                "Company Name": dup_val,
                                "domain": "-",
                                "employees": "-",
                                "employees_prooflink": "-",
                                "subindustry": "-",
                                "industry": "-",
                                "revenue": "-",
                                "revenue_prooflink": "-",
                                "first_name": "-",
                                "last_name": "-",
                                "title": "-",
                                "prooflink": "-",
                                "location": "-",
                                "status": "duplicate",
                                "email": "-",
                                "email_status": "-",
                                "last_activity": "-"
                            }

                            unsuitable_results.append(unsuitable_data)
                            write_results_in_tab(sheet_for_names, suitable_results, unsuitable_results, "unsuitable", unsuitable_data)

                    for indx in range(len(unique_values)):

                        # Check if company name in sup list
                        if sup_names:
                            lower_list = [item.lower() for item in sup_names]
                            lower_name = unique_values[indx].lower()
                            if lower_name in lower_list:
                                print(f"The company ('{unique_values[indx]}') in sup list.")
                                
                                unsuitable_data = {
                                    "Company Name": unique_values[indx],
                                    "domain": "-",
                                    "employees": "-",
                                    "employees_prooflink": "-",
                                    "subindustry": "",
                                    "industry": "",
                                    "revenue": "",
                                    "revenue_prooflink": "",
                                    "first_name": "-",
                                    "last_name": "-",
                                    "title": "-",
                                    "prooflink": "-",
                                    "location": "-",
                                    "status": "sup name",
                                    "email": "-",
                                    "email_status": "-",
                                    "last_activity": "-"
                                }

                                unsuitable_results.append(unsuitable_data)
                                write_results_in_tab(sheet_for_names, suitable_results, unsuitable_results, "unsuitable", unsuitable_data) 

                                continue    

                        # Compare scraped industry, size to user input data
                        input_size = data["size"]
                        input_industry = data["industry"]
                        input_revenue = data["revenue"]

                        # Recheck industries "any" case
                        if len(input_industry) == len(all_industries):
                            input_industry = ["any"]

                        input_data = {
                            "input_industry": input_industry,
                            "input_size": input_size,
                            "input_revenue": "any",
                        }

                        # industry = company_short_data[indx]["industry"]
                        # subindustry = company_short_data[indx]["subindustry"]

                        if not company_short_data[indx]["industry"]:
                            subindustry = "-"
                            industries = "-"
                        else:
                            industries = company_short_data[indx]["industry"]
                            subindustry = company_short_data[indx]["subindustry"]

                        employees_size = company_short_data[indx]["employees"]
                        
                        scraped_company_data = {
                            "industry": industries,
                            "employees": employees_size,
                            "revenue": "-",
                        }

                        compared_data_info = compare_data(input_data, scraped_company_data)

                        if not compared_data_info["is_valid"]:
                            unsuitable_data = {
                                "Company Name": unique_values[indx],
                                "domain": "-",
                                "employees": employees_size,
                                "employees_prooflink": "-",
                                "subindustry": subindustry,
                                "industry": industries,
                                "revenue": "-",
                                "revenue_prooflink": "-",
                                "first_name": "-",
                                "last_name": "-",
                                "title": "-",
                                "prooflink": "-",
                                "location": "-",
                                "status": compared_data_info["status"],
                                "email": "-",
                                "email_status": "-",
                                "last_activity": "-"
                            }

                            unsuitable_results.append(unsuitable_data)
                            write_results_in_tab(sheet_for_names, suitable_results, unsuitable_results, "unsuitable", unsuitable_data)

                            continue

                        else:
                            revenue_data = get_revenue(company_short_data[indx]["company"])

                            if revenue_data:
                                revenue = revenue_data[0]
                                revenue_prooflink = revenue_data[1]
                            else:
                                revenue = "-"
                                revenue_prooflink = "-"

                        input_data = {
                            "input_revenue": input_revenue,
                        }
                        scraped_company_data = {
                            "revenue": revenue,
                        }

                        compared_data_info = compare_revenue_data(input_data, scraped_company_data)

                        if not compared_data_info["is_valid"]:
                            unsuitable_data = {
                                "Company Name": unique_values[indx],
                                "domain": "-",
                                "employees": employees_size,
                                "employees_prooflink": "-",
                                "subindustry": subindustry,
                                "industry": industries,
                                "revenue": revenue,
                                "revenue_prooflink": revenue_prooflink,
                                "first_name": "-",
                                "last_name": "-",
                                "title": "-",
                                "prooflink": "-",
                                "location": "-",
                                "status": compared_data_info["status"],
                                "email": "-",
                                "email_status": "-",
                                "last_activity": "-"
                            }

                            unsuitable_results.append(unsuitable_data)
                            write_results_in_tab(sheet_for_names, suitable_results, unsuitable_results, "unsuitable", unsuitable_data)

                            continue
                        
                        else:
                            linkedin_url = uniquq_links_and_names[unique_values[indx]]

                            get_domain = get_company_domain(API_KEY, linkedin_url)
                            
                            domain_from_name = get_domain.get("domain", None)

                            if domain_from_name:
                                valid_domains_from_names.append(domain_from_name)
                            else:
                                print(f"The API was not able to find the domain on Linkedin ('{company_short_data[indx]["company"]}').You will not be charged for this request.")
                                
                                unsuitable_data = {
                                    "Company Name": unique_values[indx],
                                    "domain": "-",
                                    "employees": "-",
                                    "employees_prooflink": "-",
                                    "subindustry": "-",
                                    "industry": "-",
                                    "revenue": "-",
                                    "revenue_prooflink": "-",
                                    "first_name": "-",
                                    "last_name": "-",
                                    "title": "-",
                                    "prooflink": "-",
                                    "location": "-",
                                    "status": "domain not found in linkedin",
                                    "email": "-",
                                    "email_status": "-",
                                    "last_activity": "-"
                                }

                                unsuitable_results.append(unsuitable_data)
                                write_results_in_tab(sheet_for_names, suitable_results, unsuitable_results, "unsuitable", unsuitable_data) 
                    
                    for val in valid_domains_from_names:
                        db.add(ProcessItem(entry_id=entry_id, value=val, status="unprocessed"))
                    db.commit()
                    db.refresh(entry)  

                else:
                    entry.status = "Failed"
                    entry.error_message = read_sheet_response_by_name["message"]
                    db.commit()
                    db.refresh(entry)

                    raise Exception(read_sheet_response_by_name["message"])    

        if process_type == "search_by_domain":
            try:
                read_sheet_response = read_company_domains(sheet_url, is_company_geo_required)
            except RuntimeError:
                return JSONResponse(
                    status_code=400,
                    content={"error": "no_edit"}
                )
            
            if read_sheet_response["message"]:
                entry.status = "Failed"
                entry.error_message = read_sheet_response["message"]
                db.commit()
                db.refresh(entry)

                raise Exception(read_sheet_response["message"])
            else:
                values = read_sheet_response["domains"]  # not used
                country_names = read_sheet_response["country_names"]
                sheet = read_sheet_response["sheet"]

        if process_type == "search_by_name":
            try:
                read_sheet_response_by_name = read_company_names(sheet_url, is_company_geo_required)
            except RuntimeError:
                return JSONResponse(
                    status_code=400,
                    content={"error": "no_edit"}
                )
            
            if read_sheet_response_by_name["message"]:
                entry.status = "Failed"
                entry.error_message = read_sheet_response_by_name["message"]
                db.commit()
                db.refresh(entry)

                raise Exception(read_sheet_response_by_name["message"])
            else:
                # values = read_sheet_response_by_name["name"]  # not used
                country_names = read_sheet_response_by_name["country_names"]
                sheet = read_sheet_response_by_name["sheet"]

        values_to_process = []

        items = db.query(ProcessItem).filter_by(entry_id=entry_id, status="unprocessed").all()
        for item in items:
            values_to_process.append(item.value)

        print(f"UNPROCESSED VALUES::: {values_to_process}")

        items_2 = db.query(ProcessItem).filter_by(entry_id=entry_id).all()
        for y in items_2:
            print(f"################ {y.value} -- {y.status}")

        requirements_count = len(data["requirements"])
        
        values_to_process, duplicate_values = remove_duplicates(values_to_process)

        if duplicate_values:
            for dup_val in duplicate_values:
                unsuitable_data = {
                    "Company Name": dup_val,
                    "domain": "-",
                    "employees": "-",
                    "employees_prooflink": "-",
                    "subindustry": "-",
                    "industry": "-",
                    "revenue": "-",
                    "revenue_prooflink": "-",
                    "first_name": "-",
                    "last_name": "-",
                    "title": "-",
                    "prooflink": "-",
                    "location": "-",
                    "status": "duplicate",
                    "email": "-",
                    "email_status": "-",
                    "last_activity": "-"
                }

                unsuitable_results.append(unsuitable_data)
                write_results_in_tab(sheet, suitable_results, unsuitable_results, "unsuitable", unsuitable_data)

        if not values_to_process and process_type == "search_by_domain":
            entry = db.query(ProcessEntry).get(entry_id)
            entry.status = "Failed"
            entry.error_message = "No domains"
            db.commit()
            db.refresh(entry)

            raise Exception("No domains")

        if is_company_geo_required and country_names:
            domains_and_countries = dict(zip(values_to_process, country_names))

        # suitable_results = []
        # unsuitable_results = []

        # Define exclude keywords *******************
        exclude_keywords = []

        if data.get("exclude_keywords", ""):
            modifed_exclude_keywords = data.get("exclude_keywords", "").replace("exclude_keywords", "").replace("/", ",").replace(":", "")
            cleaned_modifed_exclude_keywords = [x.strip() for x in modifed_exclude_keywords.split(",") if x.strip()]
            exclude_keywords = []
            for i in cleaned_modifed_exclude_keywords:
                exclude_keywords.append(i)
        else:
            exclude_keywords = []

        chunk = []
        company_data_map = {}  # Store company data by ID for later reference
        
        register_flush_on_exit(sheet)

        sup_domains_sheet_url = data["sup_domains_sheet_url"]
        sup_emails_sheet_url = data["sup_emails_sheet_url"]

        if sup_domains_sheet_url:
            sup_domains = read_sup_domains(sup_domains_sheet_url)

            if sup_domains == "The 'sup domain' column isn't found.":
                entry.status = "Failed"
                entry.error_message = "The 'sup domain' column isn't found."
                db.commit()
                db.refresh(entry)

                raise Exception("The 'sup domain' column isn't found.")
        else:
            sup_domains = None

        if sup_emails_sheet_url:
            sup_emails = read_sup_emails(sup_emails_sheet_url)
            
            if sup_emails == "The 'email' column isn't found":
                entry.status = "Failed"
                entry.error_message = "The 'email' column isn't found"
                db.commit()
                db.refresh(entry)

                raise Exception("The 'email' column isn't found")
        else:
           sup_emails = None 

        keywords = []
        job_functions = []
        levels = []
        geo_codes = []
        lpc = 1
        goal = 1

        for idx, domain in enumerate(random.sample(values_to_process, len(values_to_process)), start=1):
            
            db.refresh(entry)
            if entry.is_stopped:
                print(f"⏹️ Stopped at row {idx}")
                entry.last_processed_row = idx
                db.commit()

                break

            print(f"\n⏳ Processing company {idx} \n")
            print(f"Domain:: {domain}")
            
            if not domain or "." not in domain or domain.startswith("linkedin.com"):
                                
                unsuitable_data = {
                    "Company Name": "-",
                    "domain": domain,
                    "employees": "-",
                    "employees_prooflink": "-",
                    "subindustry": "-",
                    "industry": "-",
                    "revenue": "-",
                    "revenue_prooflink": "-",
                    "first_name": "-",
                    "last_name": "-",
                    "title": "-",
                    "prooflink": "-",
                    "location": "-",
                    "status": "bad domain",
                    "email": "-",
                    "email_status": "-",
                    "last_activity": "-"
                }

                unsuitable_results.append(unsuitable_data)
                write_results_in_tab(sheet, suitable_results, unsuitable_results, "unsuitable", unsuitable_data) 

                # continue   

            else:      

                clean_domain = get_clean_domain(domain)

                # Check if domain in sup list
                if sup_domains and clean_domain in sup_domains:
                        print(f"The domain ('{clean_domain}') in sup list.")
                        
                        unsuitable_data = {
                            "Company Name": "-",
                            "domain": clean_domain,
                            "employees": "-",
                            "employees_prooflink": "-",
                            "subindustry": "-",
                            "industry": "-",
                            "revenue": "-",
                            "revenue_prooflink": "-",
                            "first_name": "-",
                            "last_name": "-",
                            "title": "-",
                            "prooflink": "-",
                            "location": "-",
                            "status": "sup domain",
                            "email": "-",
                            "email_status": "-",
                            "last_activity": "-"
                        }

                        unsuitable_results.append(unsuitable_data)
                        write_results_in_tab(sheet, suitable_results, unsuitable_results, "unsuitable", unsuitable_data) 

                        # continue                   

                else:

                    company_data = get_company_by_domain(API_KEY, clean_domain)

                    if company_data == "No more credits for domain API":
                        entry.status = "Failed"
                        entry.error_message = "No more credits"
                        db.commit()
                        db.refresh(entry)

                        raise Exception("No more credits")

                    if company_data == "Subscription is suspended":
                        entry.status = "Failed"
                        entry.error_message = "Subscription is suspended. Contact admin for renewal"
                        db.commit()
                        db.refresh(entry)

                        raise Exception("Subscription is suspended. Contact admin for renewal")

                    if not company_data:

                        print(f"No results for domain '{clean_domain}', simplifying domain and retrying...")
                        simplified_domain = ".".join(clean_domain.split(".")[-2:])
                        print(f"Retrying with simplified domain: {simplified_domain}")
                        company_data = get_company_by_domain(API_KEY, simplified_domain)
                                
                    if not company_data:

                        print(f"The API was not able to find the domain on Linkedin ('{clean_domain}').You will not be charged for this request.")
                        
                        unsuitable_data = {
                            "Company Name": "-",
                            "domain": clean_domain,
                            "employees": "-",
                            "employees_prooflink": "-",
                            "subindustry": "-",
                            "industry": "-",
                            "revenue": "-",
                            "revenue_prooflink": "-",
                            "first_name": "-",
                            "last_name": "-",
                            "title": "-",
                            "prooflink": "-",
                            "location": "-",
                            "status": "domain not found in linkedin",
                            "email": "-",
                            "email_status": "-",
                            "last_activity": "-"
                        }

                        unsuitable_results.append(unsuitable_data)
                        write_results_in_tab(sheet, suitable_results, unsuitable_results, "unsuitable", unsuitable_data) 

                        # continue       

                    else:            
                                    
                        company_name = company_data.get('company_name', 'no info')
                        employee_range = company_data.get('employee_range', 'no info')
                        employee_count = company_data.get('employee_count', 'no info')
                        employees_prooflink = company_data.get('linkedin_url', 'no info')
                        company_id = company_data.get('company_id', 'no info')

                        # Check if company name in sup list
                        if sup_names:
                            lower_list = [item.lower() for item in sup_names]
                            lower_name = company_name.lower()
                            if lower_name in lower_list:
                                print(f"The company ('{company_name}') in sup list.")
                                
                                unsuitable_data = {
                                    "Company Name": company_name,
                                    "domain": clean_domain,
                                    "employees": employee_range,
                                    "employees_prooflink": employees_prooflink,
                                    "subindustry": "",
                                    "industry": "",
                                    "revenue": "",
                                    "revenue_prooflink": "",
                                    "first_name": "-",
                                    "last_name": "-",
                                    "title": "-",
                                    "prooflink": "-",
                                    "location": "-",
                                    "status": "sup name",
                                    "email": "-",
                                    "email_status": "-",
                                    "last_activity": "-"
                                }

                                unsuitable_results.append(unsuitable_data)
                                write_results_in_tab(sheet, suitable_results, unsuitable_results, "unsuitable", unsuitable_data) 

                                continue    
                        
                        if len(company_data.get('industries', ['no info'])) > 0:
                            subindustry = company_data.get('industries', ['no info'])[0]
                            industries = find_matching_industry(subindustry) if subindustry != 'no info' else 'no info'
                        else:
                            subindustry = None
                            industries = None

                        revenue_data = get_revenue(clean_domain)

                        if revenue_data:
                            revenue = revenue_data[0]
                            revenue_prooflink = revenue_data[1]
                        else:
                            revenue = None
                            revenue_prooflink = None

                        print(f"✅ Done Step 2 (Get company revenue: '/search')")

                        if isinstance(data, str):
                            data = json.loads(data) 

                        lpc = data["lpc"]
                        goal = data["goal"]
                        input_size = data["size"]
                        input_industry = data["industry"]
                        input_revenue = data["revenue"]

                        input_data = {
                            "input_industry": input_industry,
                            "input_size": input_size,
                            "input_revenue": input_revenue,
                        }

                        if not revenue:
                            revenue = "-"
                            revenue_prooflink = "-"

                        if not subindustry:
                            subindustry = "-"
                            industries = "-"

                        if not employee_count or employee_count == 0 or employee_count == "0":
                            employee_range = "-"

                        scraped_company_data = {
                            "industry": industries,
                            "employees": employee_range,
                            "revenue": revenue,
                        }

                        compared_data_info = compare_data(input_data, scraped_company_data)

                        if not compared_data_info["is_valid"]:
                            unsuitable_data = {
                                "Company Name": company_name,
                                "domain": clean_domain,
                                "employees": employee_range,
                                "employees_prooflink": employees_prooflink,
                                "subindustry": subindustry,
                                "industry": industries,
                                "revenue": revenue,
                                "revenue_prooflink": revenue_prooflink,
                                "first_name": "-",
                                "last_name": "-",
                                "title": "-",
                                "prooflink": "-",
                                "location": "-",
                                "status": compared_data_info["status"],
                                "email": "-",
                                "email_status": "-",
                                "last_activity": "-"
                            }

                            unsuitable_results.append(unsuitable_data)
                            write_results_in_tab(sheet, suitable_results, unsuitable_results, "unsuitable", unsuitable_data)
                            
                            # continue

                        else:

                            # Define geo codes
                            selected_countries = data.get("geo")
                            if "select_all_countries" in selected_countries:
                                geolocations = list(country_ids.keys())
                            else:
                                geolocations = selected_countries
                            
                            geo_codes = [country_ids[country] for country in geolocations][:20]

                            first_requirements = data.get("requirements")[0]

                            # Define keywords ***********************
                            modifed_keywords = first_requirements.get("keywords", "").replace("keywords", "").replace("/", ",").replace(":", "")
                            cleaned_keywords = [x.strip() for x in modifed_keywords.split(",") if x.strip()]
                            keywords = []
                            for i in cleaned_keywords:
                                keywords.append(i)

                            # Define functions **********************
                            job_functions = first_requirements.get("job_function")
                            if job_functions == ['any']:
                                job_functions = []

                            job_functions = ["Operations" if item == "Business operations" else item for item in job_functions]

                            # Define levels **************************
                            level1 = first_requirements.get("level1", [])
                            level2 = first_requirements.get("level2", [])
                            level3 = first_requirements.get("level3", [])
                            levels = get_job_levels(level1, level2, level3)

                            # Compare keywords and titles 
                            # if keywords:
                            #     compared_keywords = any(item.lower() in (x.lower() for x in keywords) for item in levels)
                            #     is_exist_keywords = compared_keywords
                            # else:
                            #     is_exist_keywords = True

                            # if not is_exist_keywords:
                            #     unsuitable_data = {
                            #         "Company Name": company_name,
                            #         "domain": clean_domain,
                            #         "employees": employee_range,
                            #         "employees_prooflink": employees_prooflink,
                            #         "subindustry": subindustry,
                            #         "industry": industries,
                            #         "revenue": revenue,
                            #         "revenue_prooflink": revenue_prooflink,
                            #         "first_name": "-",
                            #         "last_name": "-",
                            #         "title": "-",
                            #         "prooflink": "-",
                            #         "location": "-",
                            #         "status": "h title",
                            #         "email": "-",
                            #         "email_status": "-",
                            #         "last_activity": "-"
                            #     }

                            #     unsuitable_results.append(unsuitable_data)
                            #     write_results_in_tab(sheet, suitable_results, unsuitable_results, "unsuitable", unsuitable_data)

                            # else:

                            print(f"✅ Done Step 3 (Compare company data with user input)")

                            # Store company data for chunk processing
                            chunk.append(company_id)
                            company_data_map[company_id] = {
                                'company_name': company_name,
                                'domain': clean_domain,
                                'employee_range': employee_range,
                                'employees_prooflink': employees_prooflink,
                                'subindustry': subindustry,
                                'industry': industries,
                                'revenue': revenue,
                                'revenue_prooflink': revenue_prooflink,
                                'company_data': company_data
                            }

                            # Store in global cache for frontend access
                            store_processed_data(entry_id, {
                                "job_levels": levels,
                                "job_functions": job_functions,
                                "keywords": keywords,
                                "geo_locations": geolocations,
                                "entry_name": entry.name 
                            })

                            # print(f"✅ Data stored in cache for entry: {entry_id}") # ********* for display in UI

            # Process chunk when full or at the end
            if chunk:
                if len(chunk) == 10 or idx == len(values_to_process):
                    
                    data_request = {
                        "current_company_ids": chunk,
                        "title_keywords": keywords[:20],
                        "functions": job_functions, 
                        "geo_codes": geo_codes,
                        "geo_codes_exclude": [],
                        "title_keywords_exclude": exclude_keywords,
                        "past_company_ids": [],
                        "keywords": [],
                        "limit": len(chunk) * 2 * int(lpc)
                    }

                    print(f"\n{data_request}\n")

                    # Step 4 - Search leads
                    request_id = search_leads(API_KEY, data_request)

                    if not request_id:
                        print("❌ No request_id returned.")
                        # Mark all companies in chunk as failed
                        for comp_id in chunk:
                            comp_data = company_data_map[comp_id]
                            unsuitable_data = {
                                "Company Name": comp_data['company_name'],
                                "domain": comp_data['domain'],
                                "employees": comp_data['employee_range'],
                                "employees_prooflink": comp_data['employees_prooflink'],
                                "subindustry": comp_data['subindustry'],
                                "industry": comp_data['industry'],
                                "revenue": comp_data['revenue'],
                                "revenue_prooflink": comp_data['revenue_prooflink'],
                                "first_name": "-",
                                "last_name": "-",
                                "title": "-",
                                "prooflink": "-",
                                "location": "-",
                                "status": "search failed",
                                "email": "-",
                                "email_status": "-",
                                "last_activity": "-"
                            }
                            unsuitable_results.append(unsuitable_data)
                            write_results_in_tab(sheet, suitable_results, unsuitable_results, "unsuitable", unsuitable_data)
                        chunk = []
                        company_data_map = {}

                        continue

                    # Wait for results
                    check_search_data = wait_for_results(API_KEY, request_id, delay=20, max_retries=25)

                    if check_search_data["status"] != "done":
                        print("❌ Timed out waiting for result.")
                        # Mark all companies in chunk as failed
                        for comp_id in chunk:
                            comp_data = company_data_map[comp_id]
                            unsuitable_data = {
                                "Company Name": comp_data['company_name'],
                                "domain": comp_data['domain'],
                                "employees": comp_data['employee_range'],
                                "employees_prooflink": comp_data['employees_prooflink'],
                                "subindustry": comp_data['subindustry'],
                                "industry": comp_data['industry'],
                                "revenue": comp_data['revenue'],
                                "revenue_prooflink": comp_data['revenue_prooflink'],
                                "first_name": "-",
                                "last_name": "-",
                                "title": "-",
                                "prooflink": "-",
                                "location": "-",
                                "status": "search timeout",
                                "email": "-",
                                "email_status": "-",
                                "last_activity": "-"
                            }
                            unsuitable_results.append(unsuitable_data)
                            write_results_in_tab(sheet, suitable_results, unsuitable_results, "unsuitable", unsuitable_data)
                        chunk = []
                        company_data_map = {}

                        continue
                    
                    lead_count = check_search_data.get("total_count", 0)

                    print(f"Lead count = {lead_count}")

                    if lead_count:
                        last_count = lead_count if lead_count > 0 else 0
                    else:
                        lead_count = 0
                        last_count = 0
                    
                    if lead_count == 0:
                        
                        # ///////////////////////////////////////////////////////////////////////////////////////////////////
                        # //////////////////////// Try search again if there are other requirements /////////////////////////
                        # ///////////////////////////////////////////////////////////////////////////////////////////////////
                        
                        if requirements_count > 1:

                            left_requirements = data.get("requirements")[1:]

                            for req_idx in range(len(left_requirements)):

                                # Define keywords ***********************
                                modifed_keywords = left_requirements[req_idx].get("keywords", "").replace("keywords", "").replace("/", ",").replace(":", "")                                
                                cleaned_keywords = [x.strip() for x in modifed_keywords.split(",") if x.strip()]
                                keywords = []
                                for i in cleaned_keywords:
                                    keywords.append(i)

                                # Define functions **********************
                                job_functions = left_requirements[req_idx].get("job_function")
                                if job_functions == ['any']:
                                    job_functions = []

                                job_functions = ["Operations" if item == "Business operations" else item for item in job_functions]

                                # Define levels **************************
                                level1 = left_requirements[req_idx].get("level1", [])
                                level2 = left_requirements[req_idx].get("level2", [])
                                level3 = left_requirements[req_idx].get("level3", [])
                                levels = get_job_levels(level1, level2, level3)

                                # Store in global cache for frontend access
                                store_processed_data(entry_id, {
                                    "job_levels": levels,
                                    "job_functions": job_functions,
                                    "keywords": keywords,
                                    "geo_locations": geolocations,
                                    "entry_name": entry.name 
                                })

                                data_request = {
                                    "current_company_ids": chunk,
                                    "title_keywords": keywords[:20],
                                    "functions": job_functions, 
                                    "geo_codes": geo_codes,
                                    "geo_codes_exclude": [],
                                    "title_keywords_exclude": exclude_keywords,
                                    "past_company_ids": [],
                                    "keywords": [],
                                    "limit": len(chunk) * 2 * int(lpc)
                                }

                                print(f"\n{data_request}\n")

                                # Search leads
                                request_id = search_leads(API_KEY, data_request)

                                if not request_id:
                                    print("❌ No request_id returned.")
                                    # Mark all companies in chunk as failed
                                    for comp_id in chunk:
                                        comp_data = company_data_map[comp_id]
                                        unsuitable_data = {
                                            "Company Name": comp_data['company_name'],
                                            "domain": comp_data['domain'],
                                            "employees": comp_data['employee_range'],
                                            "employees_prooflink": comp_data['employees_prooflink'],
                                            "subindustry": comp_data['subindustry'],
                                            "industry": comp_data['industry'],
                                            "revenue": comp_data['revenue'],
                                            "revenue_prooflink": comp_data['revenue_prooflink'],
                                            "first_name": "-",
                                            "last_name": "-",
                                            "title": "-",
                                            "prooflink": "-",
                                            "location": "-",
                                            "status": "search failed",
                                            "email": "-",
                                            "email_status": "-",
                                            "last_activity": "-"
                                        }
                                        unsuitable_results.append(unsuitable_data)
                                        write_results_in_tab(sheet, suitable_results, unsuitable_results, "unsuitable", unsuitable_data)
                                    chunk = []
                                    company_data_map = {}
                                    
                                    continue

                                # Wait for results ////////////////////////////////////////////////////////////////////////////
                                check_search_data = wait_for_results(API_KEY, request_id, delay=20, max_retries=25)

                                if check_search_data["status"] != "done":
                                    print("❌ Timed out waiting for result.")
                                    # Mark all companies in chunk as failed
                                    for comp_id in chunk:
                                        comp_data = company_data_map[comp_id]
                                        unsuitable_data = {
                                            "Company Name": comp_data['company_name'],
                                            "domain": comp_data['domain'],
                                            "employees": comp_data['employee_range'],
                                            "employees_prooflink": comp_data['employees_prooflink'],
                                            "subindustry": comp_data['subindustry'],
                                            "industry": comp_data['industry'],
                                            "revenue": comp_data['revenue'],
                                            "revenue_prooflink": comp_data['revenue_prooflink'],
                                            "first_name": "-",
                                            "last_name": "-",
                                            "title": "-",
                                            "prooflink": "-",
                                            "location": "-",
                                            "status": "search timeout",
                                            "email": "-",
                                            "email_status": "-",
                                            "last_activity": "-"
                                        }
                                        unsuitable_results.append(unsuitable_data)
                                        write_results_in_tab(sheet, suitable_results, unsuitable_results, "unsuitable", unsuitable_data)
                                    chunk = []
                                    company_data_map = {}
                                    continue
                                
                                lead_count = check_search_data.get("total_count", 0)

                                print(f"Lead count = {lead_count}")

                                if lead_count > 0: 
                                    last_count = lead_count
                                    break

                        else:
                            # ////////////////////////////// End of search when lead count still 0 //////////////////////////////

                            for comp_id in chunk:
                                comp_data = company_data_map[comp_id]
                                unsuitable_data = {
                                    "Company Name": comp_data['company_name'],
                                    "domain": comp_data['domain'],
                                    "employees": comp_data['employee_range'],
                                    "employees_prooflink": comp_data['employees_prooflink'],
                                    "subindustry": comp_data['subindustry'],
                                    "industry": comp_data['industry'],
                                    "revenue": comp_data['revenue'],
                                    "revenue_prooflink": comp_data['revenue_prooflink'],
                                    "first_name": "-",
                                    "last_name": "-",
                                    "title": "-",
                                    "prooflink": "-",
                                    "location": "-",
                                    "status": "no leads",
                                    "email": "-",
                                    "email_status": "-",
                                    "last_activity": "-"
                                }
                                unsuitable_results.append(unsuitable_data)
                                write_results_in_tab(sheet, suitable_results, unsuitable_results, "unsuitable", unsuitable_data)
                

                    print(f"=============================== {last_count} =========================================")
                    if last_count > 0:
                        leads_data = get_search_results(API_KEY, request_id)
                        print(f"✅ Done Step 5 (Get search results: 'search-results/')")
                        
                        # Process leads for each company in the chunk
                        for comp_id in chunk:
                            comp_data = company_data_map[comp_id]
                            
                            # Filter leads for this specific company
                            company_leads = [lead for lead in leads_data.get("data", []) 
                                        if lead.get("company_id") == comp_id]
                            
                            if not company_leads:
                                # No leads for this specific company
                                unsuitable_data = {
                                    "Company Name": comp_data['company_name'],
                                    "domain": comp_data['domain'],
                                    "employees": comp_data['employee_range'],
                                    "employees_prooflink": comp_data['employees_prooflink'],
                                    "subindustry": comp_data['subindustry'],
                                    "industry": comp_data['industry'],
                                    "revenue": comp_data['revenue'],
                                    "revenue_prooflink": comp_data['revenue_prooflink'],
                                    "first_name": "-",
                                    "last_name": "-",
                                    "title": "-",
                                    "prooflink": "-",
                                    "location": "-",
                                    "status": "no leads",
                                    "email": "-",
                                    "email_status": "-",
                                    "last_activity": "-"
                                }
                                unsuitable_results.append(unsuitable_data)
                                write_results_in_tab(sheet, suitable_results, unsuitable_results, "unsuitable", unsuitable_data)
                                continue
                            
                            # Process leads for this company
                            temp_lead_valid_count = 0
                            leads = []

                            print(f"✅ Done Step 7 (Store leads data for find 'lpc' in next steps)")
                            
                            for lead in company_leads:
                                first_name = lead.get("first_name", "-")
                                last_name = lead.get("last_name", "-")
                                title = lead.get("job_title", "-")
                                linkedin_url = lead.get("linkedin_url", "-")
                                location = lead.get("location", "-")

                                # Check lead title 
                                title_states = []
                                is_valid_title = check_lead_title(title, levels)

                                # Translate title if need
                                if not is_valid_title:
                                    try:
                                        try_to_translate_title = translate_title(title)

                                        if try_to_translate_title:
                                            title = try_to_translate_title
                                            is_valid_title = check_lead_title(title, levels)
                                            print(f"TITLE AFTER :: {title}")
                                    except:
                                        pass

                                title_states.append(is_valid_title)
                                                                    
                                # Compare keywords and titles 
                                if keywords:

                                    lowercase_list_of_keywords = [item.lower() for item in keywords]

                                    compared_keywords = any(word.lower() in title.lower().split() for word in lowercase_list_of_keywords)
                                    is_valid_keyword = compared_keywords
                                    
                                    title_states.append(is_valid_keyword)

                                if False in title_states:
                                    unsuitable_data = {
                                        "Company Name": comp_data['company_name'],
                                        "domain": comp_data['domain'],
                                        "employees": comp_data['employee_range'],
                                        "employees_prooflink": comp_data['employees_prooflink'],
                                        "subindustry": comp_data['subindustry'],
                                        "industry": comp_data['industry'],
                                        "revenue": comp_data['revenue'],
                                        "revenue_prooflink": comp_data['revenue_prooflink'],
                                        "first_name": first_name,
                                        "last_name": last_name,
                                        "title": title,
                                        "prooflink": linkedin_url,
                                        "location": location,
                                        "status": "h title",
                                        "email": "-",
                                        "email_status": "-",
                                        "last_activity": "-"
                                    }

                                    unsuitable_results.append(unsuitable_data)
                                    write_results_in_tab(sheet, suitable_results, unsuitable_results, "unsuitable", unsuitable_data)
                                    
                                    continue

                                # Check lead geo if required
                                if is_company_geo_required:
                                    if domains_and_countries[comp_data['domain']].lower() not in location.lower():
                                        unsuitable_data = {
                                            "Company Name": comp_data['company_name'],
                                            "domain": comp_data['domain'],
                                            "employees": comp_data['employee_range'],
                                            "employees_prooflink": comp_data['employees_prooflink'],
                                            "subindustry": comp_data['subindustry'],
                                            "industry": comp_data['industry'],
                                            "revenue": comp_data['revenue'],
                                            "revenue_prooflink": comp_data['revenue_prooflink'],
                                            "first_name": first_name,
                                            "last_name": last_name,
                                            "title": title,
                                            "prooflink": linkedin_url,
                                            "location": location,
                                            "status": "h geo",
                                            "email": "-",
                                            "email_status": "-",
                                            "last_activity": "-"
                                        }
                                        unsuitable_results.append(unsuitable_data)
                                        write_results_in_tab(sheet, suitable_results, unsuitable_results, "unsuitable", unsuitable_data)
                                        continue
                                
                                # Check lead activity 
                                last_activity = "-"
                                if linkedin_url and linkedin_url != "-": 
                                    profile_activity = get_profile_activity(linkedin_url) 

                                    if profile_activity == "No more credits for domain API" or profile_activity == "Subscription is suspended":
                                        entry.status = "Failed"
                                        entry.error_message = "Subscription is suspended. Contact admin for renewal"
                                        db.commit()
                                        db.refresh(entry)

                                        raise Exception("Subscription is suspended. Contact admin for renewal")

                                    last_activity = profile_activity.get("recent_activity_time", "-") 
                                
                                if last_activity:
                                    if "yr" in last_activity.lower(): 
                                        unsuitable_data = { 
                                            "Company Name": comp_data['company_name'], 
                                            "domain": comp_data['domain'], 
                                            "employees": comp_data['employee_range'], 
                                            "employees_prooflink": comp_data['employees_prooflink'], 
                                            "subindustry": comp_data['subindustry'], 
                                            "industry": comp_data['industry'], 
                                            "revenue": comp_data['revenue'], 
                                            "revenue_prooflink": comp_data['revenue_prooflink'], 
                                            "first_name": first_name, 
                                            "last_name": last_name, 
                                            "title": title, 
                                            "prooflink": linkedin_url, 
                                            "location": location, 
                                            "status": "activity", 
                                            "email": "-", 
                                            "email_status": "-", 
                                            "last_activity": last_activity, 
                                        } 
                                        unsuitable_results.append(unsuitable_data) 
                                        write_results_in_tab(sheet, suitable_results, unsuitable_results, "unsuitable", unsuitable_data) 
                                        
                                        continue 

                                person_data = {
                                    "first_name": first_name,
                                    "last_name": last_name,
                                    "domain": comp_data['domain'],
                                    "linkedin_url": linkedin_url,
                                    "location": location,
                                    "title": title,
                                    "last_activity": last_activity
                                }

                                leads.append(person_data)
                            
                            # Store leads data temporarily
                            with open("leads_data.json", "w", encoding="utf-8") as f:
                                json.dump(leads, f, indent=4)
                            
                            # Read and process leads
                            with open("leads_data.json", "r", encoding="utf-8") as f:
                                people_list = json.load(f)
                            
                            for person in people_list:
                                person_data_for_call = {
                                    "first_name": person["first_name"],
                                    "last_name": person["last_name"],
                                    "domain": person["domain"],
                                }
 
                                if "?" in person["first_name"] or "?" in person["last_name"]:
                                    unsuitable_data = { 
                                        "Company Name": comp_data['company_name'], 
                                        "domain": comp_data['domain'], 
                                        "employees": comp_data['employee_range'], 
                                        "employees_prooflink": comp_data['employees_prooflink'], 
                                        "subindustry": comp_data['subindustry'], 
                                        "industry": comp_data['industry'], 
                                        "revenue": comp_data['revenue'], 
                                        "revenue_prooflink": comp_data['revenue_prooflink'], 
                                        "first_name": person["first_name"], 
                                        "last_name": person["last_name"], 
                                        "title": title, 
                                        "prooflink": linkedin_url, 
                                        "location": location, 
                                        "status": "", 
                                        "email": "-", 
                                        "email_status": "email not found", 
                                        "last_activity": "-", 
                                    } 
                                    unsuitable_results.append(unsuitable_data) 
                                    write_results_in_tab(sheet, suitable_results, unsuitable_results, "unsuitable", unsuitable_data) 
                                    
                                    continue 
                                
                                # ///////////////////////////////////////////////////////////////////////////////////////////////////
                                # ///////////////////////////////// EMAIL FIND STEP /////////////////////////////////////////////////
                                # ///////////////////////////////////////////////////////////////////////////////////////////////////
                                
                                find_email_response = find_email(person_data_for_call)

                                # Recall api with simplified domain
                                # if not find_email_response or find_email_response.get("address") == "email not found":
                                #     simplified_domain = ".".join(person["domain"].split(".")[-2:])
                                #     person_data_for_call["domain"] = simplified_domain

                                #     find_email_response = find_email(person_data_for_call)

                                # Step 1: Retry with simplified domain
                                if not find_email_response or find_email_response.get("address") == "email not found":
                                    print(f"!!!!!!!!!!!!!!!!!! ALTERNATIVE 111111111111")
                                    simplified_domain = ".".join(person["domain"].split(".")[-2:])
                                    person_data_for_call["domain"] = simplified_domain
                                    find_email_response = find_email(person_data_for_call)

                                # Step 2: Try guessed patterns
                                if not find_email_response or find_email_response.get("address") == "email not found":
                                    print(f"!!!!!!!!!!!!!!!!!! ALTERNATIVE 2222222222222")
                                    first_name = person_data_for_call.get("first_name", "").lower()
                                    last_name = person_data_for_call.get("last_name", "").lower()
                                    domain = person_data_for_call.get("domain", "").lower()

                                    if first_name and last_name and domain:
                                        fi = first_name[0]
                                        patterns = [
                                            f"{first_name}.{last_name}@{domain}",
                                            f"{fi}.{last_name}@{domain}",
                                            f"{fi}{last_name}@{domain}",
                                        ]

                                        guessed_response = None
                                        for email_guess in patterns:
                                            test_data = person_data_for_call.copy()
                                            test_data["email_guess"] = email_guess
                                            guessed_response = find_email(test_data)

                                            if guessed_response and guessed_response.get("address") != "email not found":
                                                find_email_response = guessed_response
                                                break
                                        else:
                                            find_email_response = guessed_response or {"address": "email not found"}

                                email_find_variant = ""

                                if not find_email_response:
                                    email = "email not found"
                                    email_find_variant = "find-one/"
                                    email_status = ""

                                else:
                                    email = find_email_response.get("address", "email not found")
                                    email_find_variant = "find-one/"
                                    email_status = ""

                                print(f"✅ Done Step 8 (Find lead email: {email_find_variant})")

                                if email != "email not found":

                                    if email_find_variant == "find-one/":
                                        verify_email_response = verify_email(API_KEY_VERIFY, email)

                                        email_status = verify_email_response["status"]

                                        print(f"~~~~~~~~~ {email} ----- {email_status}")

                                        if email_status == "valid" or email_status == "accept_all":
                                            is_valid_email = True
                                        else:
                                            is_valid_email = False
                                        
                                    print(f"✅ Done Step 9 (Verify lead email: {email_find_variant})")

                                    if is_valid_email:

                                        # Check email in sub list
                                        if sup_emails:
                                            print(f"✅ Done Step 10 (Check email in sup list)")
                                            if email in sup_emails:
                                                email_status = "sup"
                                            else:
                                                email_status = email_status
                                        else:
                                            email_status = email_status

                                    else:
                                        email_status = email_status
                                else:
                                    email = "-"
                                    email_status = "email not found"
                                
                                scraped_data = {
                                    "Company Name": comp_data['company_name'],
                                    "domain": comp_data['domain'],
                                    "employees": comp_data['employee_range'],
                                    "employees_prooflink": comp_data['employees_prooflink'],
                                    "subindustry": comp_data['subindustry'],
                                    "industry": comp_data['industry'],
                                    "revenue": comp_data['revenue'],
                                    "revenue_prooflink": comp_data['revenue_prooflink'],
                                    "first_name": person["first_name"],
                                    "last_name": person["last_name"], 
                                    "title": person["title"],
                                    "prooflink": person["linkedin_url"],
                                    "location": person["location"],
                                    "status": "",
                                    "email": email,
                                    "email_status": email_status,
                                    "last_activity": person["last_activity"],
                                }

                                if email_status == "valid" or email_status == "accept_all":
                                    suitable_results.append(scraped_data)
                                    temp_lead_valid_count += 1
                                    write_results_in_tab(sheet, suitable_results, unsuitable_results, "suitable", scraped_data)
                                else:
                                    scraped_data["last_activity"] = "-"
                                    unsuitable_results.append(scraped_data)
                                    write_results_in_tab(sheet, suitable_results, unsuitable_results, "unsuitable", scraped_data)

                                # Check LPC limit
                                if temp_lead_valid_count == int(lpc):
                                    break
                            
                            # Check overall goal
                            if len(suitable_results) >= int(goal):
                                entry.status = "Done"
                                db.commit()
                                break

                    # Reset for next chunk
                    chunk = []
                    company_data_map = {}
                    
                    # Check if we reached the goal after processing chunk
                    if len(suitable_results) >= int(goal):
                        break

        flush_all_buffers(sheet)

        all_processed = suitable_results + unsuitable_results
        unique_processed_values = list(set(d["domain"] for d in all_processed))

        print(f"++++++++ {unique_processed_values}")
        print(f"-------- {entry_id}")

        items = db.query(ProcessItem)\
            .filter(ProcessItem.entry_id == entry_id,
                    ProcessItem.value.in_(unique_processed_values))\
            .all()

        for item in items:
            item.status = "processed"

        db.commit()

        # Final status update
        if not entry.is_stopped:
            entry.status = "Done"
            entry.error_message = ""
            db.commit()

    except Exception as e:
        entry.status = "Failed"
        entry.error_message = str(e)
        db.commit()

        # Clean up cache on error
        delete_processed_data(entry_id)
        
    finally:
        db.close()

        # Clean up cache when processing is complete
        delete_processed_data(entry_id)