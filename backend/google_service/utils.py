import gspread
from oauth2client.service_account import ServiceAccountCredentials
import re 
from fastapi import HTTPException


scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("google_service/service_account.json", scope)
gc = gspread.authorize(creds)


def get_sheet_name(sheet_url):
    """
    Return google sheet name.
    """
    try:
        # Extract spreadsheet ID and gid from URL
        spreadsheet_id = re.search(r"/d/([a-zA-Z0-9-_]+)", sheet_url).group(1)
        gid_match = re.search(r"gid=([0-9]+)", sheet_url)
        gid = int(gid_match.group(1)) if gid_match else None

        sheet = gc.open_by_key(spreadsheet_id)
        sheet_name = sheet.title

        return sheet_name

    except Exception as e:
        print(f"❌ Error reading sheet name: {e}")
        raise RuntimeError(f"Failed to read sheet name: {e}")


def read_sup_names(sup_names_sheet_url):
    """
    Read, return names from sup sheet. 
    """
    try:
        print("✅ Google Sheets authorized (sup names)")

        # Extract spreadsheet ID and gid from URL
        spreadsheet_id = re.search(r"/d/([a-zA-Z0-9-_]+)", sup_names_sheet_url).group(1)
        gid_match = re.search(r"gid=([0-9]+)", sup_names_sheet_url)
        gid = int(gid_match.group(1)) if gid_match else None

        sheet = gc.open_by_key(spreadsheet_id)

        if gid:
            worksheet = sheet.get_worksheet_by_id(gid)
        else:
            print("⚠️ No gid found in URL, defaulting to first worksheet")
            worksheet = sheet.sheet1

        all_rows = worksheet.get_all_records()  

        columns = [c.lower() for c in all_rows[0].keys()]

        if "sup name" not in columns:
            return "The 'sup name' column isn't found."

        all_data = [
            {k.lower(): v for k, v in row.items()}  
            for row in all_rows
        ] 

        sup_names = [row.get("sup name", "") for row in all_data]

        return sup_names

    except Exception as e:
        print(f"❌ Error reading names: {e}")
        return None
    

def read_sup_domains(sup_domains_sheet_url):
    """
    Read, return domains from sup sheet. 
    """
    try:
        print("✅ Google Sheets authorized (sup domains)")

        # Extract spreadsheet ID and gid from URL
        spreadsheet_id = re.search(r"/d/([a-zA-Z0-9-_]+)", sup_domains_sheet_url).group(1)
        gid_match = re.search(r"gid=([0-9]+)", sup_domains_sheet_url)
        gid = int(gid_match.group(1)) if gid_match else None

        sheet = gc.open_by_key(spreadsheet_id)

        if gid:
            worksheet = sheet.get_worksheet_by_id(gid)
        else:
            print("⚠️ No gid found in URL, defaulting to first worksheet")
            worksheet = sheet.sheet1

        all_rows = worksheet.get_all_records()  

        columns = [c.lower() for c in all_rows[0].keys()]

        if "sup domain" not in columns:
            return "The 'sup domain' column isn't found."

        all_data = [
            {k.lower(): v for k, v in row.items()}  
            for row in all_rows
        ] 

        sup_domains = [row.get("sup domain", "") for row in all_data]

        return sup_domains

    except Exception as e:
        print(f"❌ Error reading domains: {e}")
        return None
    

def read_sup_emails(sup_emails_sheet_url):
    """
    Read, return emails from sup sheet. 
    """
    try:
        print("✅ Google Sheets authorized (sup emails)")

        # Extract spreadsheet ID and gid from URL
        spreadsheet_id = re.search(r"/d/([a-zA-Z0-9-_]+)", sup_emails_sheet_url).group(1)
        gid_match = re.search(r"gid=([0-9]+)", sup_emails_sheet_url)
        gid = int(gid_match.group(1)) if gid_match else None

        sheet = gc.open_by_key(spreadsheet_id)

        if gid:
            worksheet = sheet.get_worksheet_by_id(gid)
        else:
            print("⚠️ No gid found in URL, defaulting to first worksheet")
            worksheet = sheet.sheet1

        all_rows = worksheet.get_all_records()  

        columns = [c.lower() for c in all_rows[0].keys()]

        if "email" not in columns:
            return "The 'email' column isn't found."

        all_data = [
            {k.lower(): v for k, v in row.items()}  
            for row in all_rows
        ] 

        sup_emails = [row.get("email", "") for row in all_data]

        return sup_emails

    except Exception as e:
        print(f"❌ Error reading emails: {e}")
        return None
 

def read_company_domains(sheet_url, is_company_geo_required):
    """
    Read, return company domains from sheet. 
    """
    try:
        print("\n✅ Google Sheets authorized (nac domains)")

        # Extract spreadsheet ID and gid from URL
        spreadsheet_id = re.search(r"/d/([a-zA-Z0-9-_]+)", sheet_url).group(1)
        gid_match = re.search(r"gid=([0-9]+)", sheet_url)
        gid = int(gid_match.group(1)) if gid_match else None

        sheet = gc.open_by_key(spreadsheet_id)

        if gid:
            worksheet = sheet.get_worksheet_by_id(gid)
        else:
            print("⚠️ No gid found in URL, defaulting to first worksheet")
            worksheet = sheet.sheet1
            
        all_rows = worksheet.get_all_records()  

        columns = [c.lower() for c in all_rows[0].keys()]
        
        domains = None
        country_names = None

        response = {"domains": domains, "country_names": country_names, "message": ""}

        if "domain" not in columns:
            response["message"] = "The 'domain' column isn't found."
            return response

        all_data = [
            {k.lower(): v for k, v in row.items()}  
            for row in all_rows
        ] 

        domains = [row.get("domain", "") for row in all_data]
        
        if is_company_geo_required:
            if "country" not in columns:
                response["message"] = "The 'country' column isn't found."
                return response
            
            country_names = [row.get("country", "") for row in all_data]

        print(f"✅ Found {len(domains)} companies")

        response = {"domains": domains, "country_names": country_names, "sheet": sheet , "message": ""} 

        return response

    except Exception as e:
        print(f"❌ Error reading companies: {e}")
        raise RuntimeError(f"Failed to read company domains: {e}")


def read_company_names(sheet_url, is_company_geo_required):
    """
    Read, return company names from sheet. 
    """
    try:
        print("\n✅ Google Sheets authorized (nac names)")

        # Extract spreadsheet ID and gid from URL
        spreadsheet_id = re.search(r"/d/([a-zA-Z0-9-_]+)", sheet_url).group(1)
        gid_match = re.search(r"gid=([0-9]+)", sheet_url)
        gid = int(gid_match.group(1)) if gid_match else None

        sheet = gc.open_by_key(spreadsheet_id)

        if gid:
            worksheet = sheet.get_worksheet_by_id(gid)
        else:
            print("⚠️ No gid found in URL, defaulting to first worksheet")
            worksheet = sheet.sheet1
            
        all_rows = worksheet.get_all_records()  

        columns = [c.lower() for c in all_rows[0].keys()]
        
        names = []
        country_names = []
        locations = []

        response = {"names": names, "country_names": country_names, "locations": locations, "message": "", "sheet": sheet}

        if "name" not in columns:
            response["message"] = "The 'name' column isn't found."
            return response

        all_data = [
            {k.lower(): v for k, v in row.items()}  
            for row in all_rows
        ] 

        names = [row.get("name", "") for row in all_data]
        
        if is_company_geo_required:
            if "country" not in columns:
                response["message"] = "The 'country' column isn't found."
                return response
            
            country_names = [row.get("country", "") for row in all_data]

        if "location" in columns:
            locations = [row.get("location", "") for row in all_data]

        print(f"✅ Found {len(names)} companies")

        response = {"names": names, "country_names": country_names, "locations": locations, "sheet": sheet, "message": ""} 

        return response

    except Exception as e:
        print(f"❌ Error reading companies: {e}")
        raise RuntimeError(f"Failed to read company names: {e}")
    

def read_company_data_mixed(sheet_url):
    """
    Read, return company names/domains from sheet. 
    """
    try:
        print("\n✅ Google Sheets authorized (mixed)")

        # Extract spreadsheet ID and gid from URL
        spreadsheet_id = re.search(r"/d/([a-zA-Z0-9-_]+)", sheet_url).group(1)
        gid_match = re.search(r"gid=([0-9]+)", sheet_url)
        gid = int(gid_match.group(1)) if gid_match else None

        sheet = gc.open_by_key(spreadsheet_id)

        if gid:
            worksheet = sheet.get_worksheet_by_id(gid)
        else:
            print("⚠️ No gid found in URL, defaulting to first worksheet")
            worksheet = sheet.sheet1
            
        all_rows = worksheet.get_all_records()  

        columns = [c.lower() for c in all_rows[0].keys()]

        existing_columns = []
        
        response = {"message": "", "sheet": sheet, "existing_columns": []}

        if "name" not in columns and "domain" not in columns:
            response["message"] = "The 'name'/'domain' column(s) isn't found."
            
            return response
        
        if "name" in columns:
            existing_columns.append("name")
        if "domain" in columns:
             existing_columns.append("domain")

        response["existing_columns"] = existing_columns

        return response

    except Exception as e:
        print(f"❌ Error reading companies: {e}")
        raise RuntimeError(f"Failed to read companies: {e}")