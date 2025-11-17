import os
from fastapi import FastAPI, Request, Form, Depends, HTTPException, Cookie, status, BackgroundTasks
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware
from fastapi.security import OAuth2PasswordBearer
from fastapi.encoders import jsonable_encoder
import json
from dotenv import load_dotenv
from core.jwt import verify_token, create_access_token
from openai import OpenAI
from api_calls import *
from utils.utils import *
from fixed_data.list_of_industries import INDUSTRIES
from fixed_data.functions import valid_functions
from fixed_data.levels import all_job_levels
from fixed_data.countries_id import country_ids
from google_service.utils import *
from datetime import datetime
from sqlalchemy.orm import Session
from models import init_db, SessionLocal, ProcessEntry
import uuid
from sqlalchemy import text, case, func, desc
from process.run_process import process_entry_logic
from cache_manager import get_processed_data, cleanup_old_cache_entries
import asyncio


load_dotenv()

init_db()

# 🔐 ENV VARS
SESSION_SECRET_KEY = os.getenv("SESSION_SECRET_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
LOGIN_PASSWORD = os.getenv("LOGIN_PASSWORD")

# 🧠 OpenAI Client
client = OpenAI(api_key=OPENAI_API_KEY)

# 📒 FastAPI setup
app = FastAPI()
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")
app.add_middleware(SessionMiddleware, secret_key=SESSION_SECRET_KEY)
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/login")



# ✅ Second startup event - Cache cleanup (NEW)
@app.on_event("startup")
async def startup_cache_cleanup():
    """
    Start the cache cleanup background task
    """
    print("🚀 Starting cache cleanup background task", flush=True)
    asyncio.create_task(cleanup_old_cache_entries())


# ✅ Migration-safe column adding
# def add_column_if_not_exists(db: Session, table_name: str, column_name: str, column_type: str):
#     try:
#         inspector = inspect(db.bind)
#         columns = [col['name'] for col in inspector.get_columns(table_name)]
        
#         if column_name not in columns:
#             alter_stmt = f'ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}'
#             db.execute(text(alter_stmt))
#             db.commit()
#             print(f"✅ Added column '{column_name}' to '{table_name}'", flush=True)
#         else:
#             print(f"ℹ️ Column '{column_name}' already exists in '{table_name}'", flush=True)
#     except ProgrammingError as e:
#         db.rollback()
#         print(f"❌ Error during migration: {e}", flush=True)


# ✅ Manual migration runs once on startup
# @app.on_event("startup")
# def apply_manual_migrations():
#     print("🚀 Startup event running", flush=True)
#     db = SessionLocal()
#     try:
#         add_column_if_not_exists(db, "process_entries", "id", "VARCHAR PRIMARY KEY")
#         add_column_if_not_exists(db, "process_entries", "name", "VARCHAR")
#         add_column_if_not_exists(db, "process_entries", "url", "VARCHAR")
#         add_column_if_not_exists(db, "process_entries", "status", "VARCHAR")
#         add_column_if_not_exists(db, "process_entries", "is_stopped", "BOOLEAN DEFAULT FALSE")
#         add_column_if_not_exists(db, "process_entries", "last_processed_row", "INTEGER DEFAULT 0 NOT NULL")
#         add_column_if_not_exists(db, "process_entries", "updated_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
#         add_column_if_not_exists(db, "process_entries", "error_message", "TEXT")
#         add_column_if_not_exists(db, "process_entries", "input_data", "TEXT")
#     except Exception as e:
#         print(f"❌ Manual migration failed: {e}", flush=True)
#     finally:
#         db.close()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ✅ Auth Helpers
def get_current_user(access_token: str = Cookie(None)):
    payload = verify_token(access_token)
    if payload is None:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    return payload


@app.get("/", response_class=HTMLResponse)
def password_page(request: Request):
    return templates.TemplateResponse("password.html", {"request": request})


@app.post("/login")
def login(request: Request, password: str = Form(...)):
    if password == LOGIN_PASSWORD:
        token = create_access_token({"sub": "admin"})
        response = RedirectResponse(url="/dashboard", status_code=303)
        response.set_cookie("access_token", token, httponly=True)
        return response
    return templates.TemplateResponse("password.html", {"request": request, "error": "Invalid password"})


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(
    request: Request, 
    user: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
    ):

    error = request.query_params.get("error")

    status_priority = case(
        (func.lower(func.trim(ProcessEntry.status)) == "in progress", 1),
        else_=0
    )

    entries = (
        db.query(ProcessEntry)
        .order_by(status_priority.desc(), desc(ProcessEntry.updated_at))
        .all()
    )

    all_sublevels = []
    for key_values in all_job_levels.values():
        all_sublevels.extend(key_values)  

    # all_sublevels_unique = list(dict.fromkeys(all_sublevels))

    all_countries = list(country_ids.keys())

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "entries": entries,
            "industries": INDUSTRIES,
            "functions": valid_functions,
            "levels": all_sublevels,
            "all_countries": all_countries,
            "cache_bust": datetime.now().timestamp(),
            "message": "The access for editing is needed." if error == "no_edit" else None,
            "status": "warning" if error == "no_edit" else None
        }
    )


@app.post("/process")
async def process_sheet(request: Request, db: Session = Depends(get_db), background_tasks: BackgroundTasks = None,):
    """
    Process the start action.
    """
    data = await request.json()

    sheet_url = data.get("sheet_url")

    try:
        sheet_name = get_sheet_name(sheet_url)
    except RuntimeError:
        return JSONResponse(
            status_code=400,
            content={"error": "no_edit"}
        )
        
    entry_id = uuid.uuid4().hex

    entry = ProcessEntry(
    id=entry_id,
    name=sheet_name,
    url=sheet_url,
    status="In Progress",
    last_processed_row=1,
    input_data={
        "geo": data.get("geo"),
        "exclude_keywords": data.get("exclude_keywords"),
        "sheet_url": data.get("sheet_url"),
        "company_geo": data.get("company_geo"),
        "sup_emails_sheet_url": data.get("sup_emails_sheet_url"),
        "sup_domains_sheet_url": data.get("sup_domains_sheet_url"),
        "sup_names_sheet_url": data.get("sup_names_sheet_url"),
        "goal": data.get("goal"),
        "lpc": data.get("lpc"),
        "size": data.get("size"),
        "industry": data.get("industry"),
        "revenue": data.get("revenue"),
        "requirements": data.get("requirements"),
        "process_type": data.get("process_type"),
        }
    )

    db.add(entry)
    db.commit()
    
    # db.refresh(entry)

    def run_main_logic(entry_id: str):
        """
        Run main process.
        """
        db = SessionLocal()  
        try:
            process_entry_logic(entry_id)
        finally:
            db.close()

    background_tasks.add_task(run_main_logic, entry_id)

    return JSONResponse(content={
        "success": True,
        "entry_id": entry_id,
        "message": "Processing started"
    })


@app.get("/api/entries")
def api_entries(request: Request, access_token: str = Cookie(None), db: Session = Depends(get_db)):
    """
    Update UI table via DB content.
    """
    if not access_token or verify_token(access_token) is None:
        if "text/html" in request.headers.get("accept", ""):
            return RedirectResponse(url="/", status_code=302)
        return JSONResponse(status_code=401, content={"error": "Unauthorized"})

    entries = db.query(ProcessEntry).order_by(ProcessEntry.id).all()

    return JSONResponse(content=jsonable_encoder([{
        "id": entry.id,
        "name": entry.name,
        "url": entry.url,
        "status": entry.status,
        "error_message": entry.error_message,

    } for entry in entries]))


@app.post("/stop/{entry_id}")
def stop_entry(entry_id: str, user: dict = Depends(get_current_user)):
    db = SessionLocal()
    try:
        entry = db.query(ProcessEntry).filter_by(id=entry_id).first()
        if entry:
            entry.is_stopped = True
            entry.status = "Stopped"
            db.commit()
            return {"success": True, "status": entry.status}  
        return {"success": False, "error": "Entry not found"}
    finally:
        db.close()


@app.post("/resume/{entry_id}")
def resume_process(entry_id: str, background_tasks: BackgroundTasks):
    """
    Continue process if failed or stopped.
    """
    db = SessionLocal()
    try:
        entry = db.query(ProcessEntry).filter_by(id=entry_id).first()
        if not entry:
            return {"error": "Entry not found"}

        # Only resume if stopped or failed
        if entry.status in ["Stopped", "Failed"]:
            entry.is_stopped = False
            entry.status = "In Progress"
            db.commit()
            db.refresh(entry)

        # Schedule processing in background
        background_tasks.add_task(process_entry_logic, entry_id)

        return {"message": "Resuming process in background"}

    except Exception as e:
        db.rollback()
        if entry:
            entry.status = "Failed"
            entry.error_message = str(e)
            db.commit()
        return {"error": f"Resume failed: {e}"}

    finally:
        db.close()


@app.post("/delete/{sheet_id}")
def delete_sheet(sheet_id: str, user: dict = Depends(get_current_user), db: Session = Depends(get_db)):
    row = db.query(ProcessEntry).filter_by(id=sheet_id).first()
    if row:
        db.delete(row)
        db.commit()
    return RedirectResponse(url="/dashboard", status_code=303)


# @app.post("/api/delete-all", status_code=status.HTTP_200_OK)
# def delete_all_entries(db: Session = Depends(get_db)):
#     db.query(ProcessEntry).delete()
#     db.commit()
#     return {"detail": "All entries deleted"}


@app.post("/api/delete-all", status_code=status.HTTP_200_OK)
def delete_all_entries(db: Session = Depends(get_db)):
    entries = db.query(ProcessEntry).all()
    for entry in entries:
        db.delete(entry)  # ORM delete → cascades to ProcessItem
    db.commit()
    return {"detail": "All entries and related items deleted"}


@app.get("/api/process-data/{entry_id}")
async def get_process_data(entry_id: str, request: Request):
    """
    Get processed data for frontend display
    """
    access_token = request.cookies.get("access_token")
    if not access_token or verify_token(access_token) is None:
        return JSONResponse(status_code=401, content={"error": "Unauthorized"})
    
    data = get_processed_data(entry_id)

    if not data:
        return JSONResponse(status_code=404, content={"error": "Process data not found or expired"})
    
    return JSONResponse(content={
        "success": True,
        "data": {
            "entry_id": entry_id,
            "entry_name": data.get("entry_name", "Unknown Entry"),
            "job_levels": data.get("job_levels", []),
            "job_functions": data.get("job_functions", []),
            "keywords": data.get("keywords", []),
            "geo_locations": data.get("geo_locations", []),
            "processed_at": data.get("timestamp", ""),
            "status": "processed"
        }
    })
