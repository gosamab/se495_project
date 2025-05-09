import os
import json
import requests
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import threading
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

ALLOWED_EXTS = {".binetflow", ".csv", ".json", ".log", ".txt", ".conf", ".html", ".md", ".labeled"}
OUTPUT_DIR = "datasets"
MAX_WORKERS = 10
PARTIAL_LOG_FILE = os.path.join(OUTPUT_DIR, "partial_log.json")

lock = threading.Lock()
download_log = []

def sanitize_filename(name):
    return "".join(c if c.isalnum() or c in ("-", "_", ".") else "_" for c in name)

def is_allowed_file(url):
    return any(url.lower().endswith(ext) for ext in ALLOWED_EXTS)

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((requests.exceptions.RequestException,))
)
def safe_get(url, stream=False):
    return requests.get(url, timeout=10, stream=stream)

def get_file_links(base_url, visited=None):
    if visited is None:
        visited = set()

    if base_url in visited:
        return []

    visited.add(base_url)

    try:
        print(f"[i] Fetching links from {base_url}")
        response = safe_get(base_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        links = []

        for tag in soup.find_all("a", href=True):
            href = tag['href']
            
            if href in ("../", "./", "/publicDatasets/") or href == base_url:
                continue
                
            print(f"[i] Found link: {href}")
            
            if href.startswith("/"):
                base_parts = urlparse(base_url)
                full_url = f"{base_parts.scheme}://{base_parts.netloc}{href}"
            else:
                full_url = urljoin(base_url, href)

            if urlparse(full_url).netloc != urlparse(base_url).netloc:
                print(f"[x] Skipping external link: {full_url}")
                continue

            if full_url in visited:
                continue

            if href.endswith("/"):
                sub_links = get_file_links(full_url, visited)
                links.extend(sub_links)
            elif is_allowed_file(full_url):
                links.append(full_url)

        return links
    except Exception as e:
        print(f"[ERROR] Failed to fetch {base_url}: {e}")
        return []

def download_file(file_url, save_path):
    try:
        if os.path.exists(save_path):
            return {"status": "skipped", "path": save_path, "http_status": 0, "size": os.path.getsize(save_path)}
        
        response = safe_get(file_url, stream=True)
        response.raise_for_status()
        
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        with open(save_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        return {"status": "downloaded", "path": save_path, "http_status": 200, "size": os.path.getsize(save_path)}
    except requests.exceptions.HTTPError as e:
        return {"status": str(e.response.status_code), "path": save_path, "http_status": e.response.status_code, "size": 0}
    except Exception as e:
        return {"status": "error", "path": save_path, "http_status": -1, "error": str(e), "size": 0}

def process_capture(capture):
    required_fields = ["Capture_URL", "Capture_Name", "Malware", "Infection_Date"]
    for field in required_fields:
        if field not in capture or not capture[field]:
            print(f"[ERROR] Missing or empty field '{field}' in capture: {capture}")
            return
    
    base_url = capture["Capture_URL"]
    capture_name = sanitize_filename(capture["Capture_Name"])
    malware = capture["Malware"]
    infection_date = capture["Infection_Date"]

    file_links = get_file_links(base_url)
    if not file_links:
        print(f"[WARNING] No files found for capture: {capture_name}")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {}

        for file_url in file_links:
            parsed = urlparse(file_url)

            if "/publicDatasets/" in parsed.path:
                relative_path = parsed.path.split("/publicDatasets/", 1)[-1].lstrip("/")
            else:
                relative_path = os.path.basename(parsed.path)

            local_path = os.path.join(OUTPUT_DIR, capture_name, relative_path)
            futures[executor.submit(download_file, file_url, local_path)] = file_url

        for future in tqdm(as_completed(futures), total=len(futures), desc=capture_name):
            url = futures[future]
            parsed = urlparse(url)
            filename = os.path.basename(parsed.path)
            result = future.result()

            log_entry = {
                "Capture_Name": capture_name,
                "Infection_Date": infection_date,
                "Malware": malware,
                "URL": url,
                "filename": filename,
                **result
            }

            with lock:
                download_log.append(log_entry)
                temp_log_file = f"{PARTIAL_LOG_FILE}.tmp"
                with open(temp_log_file, "w") as f:
                    json.dump(download_log, f, indent=2)
                os.replace(temp_log_file, PARTIAL_LOG_FILE)

def summarize_results():
    summary = {
        "total_files": len(download_log),
        "downloaded": sum(1 for e in download_log if e["status"] == "downloaded"),
        "skipped": sum(1 for e in download_log if e["status"] == "skipped"),
        "404s": sum(1 for e in download_log if e["status"] == "404"),
        "errors": sum(1 for e in download_log if e["status"] == "error"),
        "captures": {}
    }
    for entry in download_log:
        cap = entry["Capture_Name"]
        if cap not in summary["captures"]:
            summary["captures"][cap] = {
                "Malware": entry["Malware"],
                "Infection_Date": entry["Infection_Date"],
                "files": 0
            }
        summary["captures"][cap]["files"] += 1
    return summary

def load_datasets_file():
    datasets_file = os.path.join(OUTPUT_DIR, 'datasets.json')
    try:
        if not os.path.exists(datasets_file):
            print(f"[ERROR] Datasets file not found: {datasets_file}")
            print("Please create a datasets.json file with your capture information.")
            return None
            
        with open(datasets_file, "r") as f:
            return json.load(f)
    except json.JSONDecodeError:
        print(f"[ERROR] Invalid JSON format in {datasets_file}")
        return None
    except Exception as e:
        print(f"[ERROR] Failed to load datasets file: {e}")
        return None

def load_partial_log():
    global download_log
    if os.path.exists(PARTIAL_LOG_FILE):
        try:
            with open(PARTIAL_LOG_FILE, "r") as f:
                download_log = json.load(f)
            print(f"[i] Loaded {len(download_log)} entries from partial log file")
        except json.JSONDecodeError:
            print(f"[WARNING] Partial log file exists but is corrupted. Starting fresh.")
            download_log = []
        except Exception as e:
            print(f"[WARNING] Could not load partial log: {e}. Starting fresh.")
            download_log = []
    else:
        download_log = []

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    load_partial_log()
    
    captures = load_datasets_file()
    if not captures:
        return
    
    if not isinstance(captures, list):
        print("[ERROR] datasets.json should contain a list of capture objects")
        return

    for capture in captures:
        process_capture(capture)

    summary = summarize_results()
    with open(os.path.join(OUTPUT_DIR, "download_status.json"), "w") as f:
        json.dump({"summary": summary, "details": download_log}, f, indent=2)

    print("[✓] Download complete. Summary saved to 'download_status.json'")
    print(f"[i] Total: {summary['total_files']} | Downloaded: {summary['downloaded']} | Skipped: {summary['skipped']} | Errors: {summary['errors']}")

if __name__ == "__main__":
    main()