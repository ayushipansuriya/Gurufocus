from fastapi import FastAPI, Form, Request, Depends
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from main_updated import create_portfolios, download_portfolios, run_all_momentum_rankings
import uvicorn
import asyncio
from concurrent.futures import ThreadPoolExecutor
import shutil
from fastapi.responses import FileResponse
from pathlib import Path
from datetime import datetime


executor = ThreadPoolExecutor()
app = FastAPI()

# Serve static files (like CSS)
app.mount("/static", StaticFiles(directory="static"), name="static")

templates = Jinja2Templates(directory="templates")

# Fake credentials
VALID_EMAIL = "anistorco@gmail.com"
VALID_PASSWORD = "Upwork@2025"


@app.get("/", response_class=HTMLResponse)
async def login_page(request: Request):
    """Show login form"""
    return templates.TemplateResponse("login.html", {"request": request, "error": None})


@app.post("/login", response_class=HTMLResponse)
async def login(request: Request, email: str = Form(...), password: str = Form(...)):
    """Handle login submission"""
    if email != VALID_EMAIL:
        error = "Invalid email address"
    elif password != VALID_PASSWORD:
        error = "Incorrect password"
    else:
        # Successful login
        response = RedirectResponse(url="/dashboard", status_code=303)
        response.set_cookie("email", email)
        return response

        # If any error, re-render login page with specific message
    return templates.TemplateResponse(
        "login.html", {"request": request, "error": error, "email": email}
    )


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    """Protected page"""
    email = request.cookies.get("email")
    if not email:
        return RedirectResponse(url="/")
    return templates.TemplateResponse("dashboard.html", {"request": request, "email": email})



def run_full_pipeline():
    asyncio.run(_run_pipeline_async())

async def _run_pipeline_async():
    results = {"step1": "fail", "step2": "fail", "step3": "fail"}

    try:
        # Step 1: Create Portfolios
        repeated_id, single_id, main_id = await create_portfolios()
        results["step1"] = "success"

        # Step 2: Download Portfolios
        repeated_path, single_path, main_path, combined_path = await download_portfolios(
            repeated_id, single_id, main_id
        )
        results["step2"] = "success"

        # Step 3: Run Momentum Ranking
        run_all_momentum_rankings(repeated_path, single_path, main_path, combined_path)
        results["step3"] = "success"

    except Exception as e:
        print("Error occurred:", e)

    return results


@app.get("/run_process")
async def run_process():
    loop = asyncio.get_event_loop()
    results = await loop.run_in_executor(executor, lambda: asyncio.run(_run_pipeline_async()))
    return results

@app.get("/download")
async def download_all_files():
    folder_path = Path("Final_File")

    # Create timestamp for unique filename
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    zip_filename = f"GuruFocus_Portfolio_{timestamp}.zip"
    zip_path = Path(zip_filename)

    # If any old zip with same name exists, remove it
    if zip_path.exists():
        zip_path.unlink()

    # Create a new zip file (without extension, shutil adds .zip automatically)
    shutil.make_archive(zip_path.stem, "zip", folder_path)

    # Return the zip file as downloadable response
    return FileResponse(
        path=zip_path,
        filename=zip_filename,
        media_type="application/zip"
    )

@app.get("/logout")
async def logout():
    """Clear cookie and redirect"""
    response = RedirectResponse(url="/")
    response.delete_cookie("email")
    return response


if __name__ == "__main__":
    uvicorn.run("view:app", host="127.0.0.1", port=8000, reload=True)