import os
import json
import csv
import logging
import requests
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from tqdm import tqdm

OUTPUT_DIR = "datasets"
INPUT_JSON = os.path.join(OUTPUT_DIR, "datasets.json")
CSV_OUTPUT = os.path.join(OUTPUT_DIR, "all_files.csv")
LOG_FILE = os.path.join(OUTPUT_DIR, "scraper.log")
TIMEOUT = 20

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((requests.exceptions.RequestException,))
)
def safe_get(url, stream=False):
    """Make a GET request with retry logic"""
    return requests.get(url, timeout=TIMEOUT, stream=stream)

def get_all_file_links(base_url, visited=None, max_depth=20, current_depth=0):
    """Recursively fetch all file links from a directory listing page"""
    if visited is None:
        visited = set()
    
    if not base_url.endswith('/'):
        base_url += '/'
    
    if base_url in visited or current_depth > max_depth:
        return []
        
    visited.add(base_url)
    links = []
    
    try:
        logger.debug(f"Fetching links from {base_url}")
        response = safe_get(base_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        
        base_path_parts = urlparse(base_url).path.strip('/').split('/')
        base_dataset = base_path_parts[-1] if base_path_parts else ""
        
        for tag in soup.find_all("a", href=True):
            href = tag["href"].strip()
            
            if (tag.text.strip().lower() in ("parent directory", "parent") or 
                href in ("../", "./") or 
                href.startswith("?") or
                href.startswith("javascript:")):
                continue
                
            full_url = urljoin(base_url, href)
            
            if urlparse(full_url).netloc != urlparse(base_url).netloc:
                continue
            
            parsed_url = urlparse(full_url)
            path_parts = parsed_url.path.strip('/').split('/')
            
            is_dir = href.endswith('/') or '.' not in os.path.basename(parsed_url.path)
            
            if is_dir:
                if current_depth < max_depth:
                    dir_url = full_url if full_url.endswith('/') else full_url + '/'
                    sub_links = get_all_file_links(dir_url, visited, max_depth, current_depth + 1)
                    links.extend(sub_links)
            else:
                filename = os.path.basename(parsed_url.path)
                
                dataset_path = '/'.join(path_parts)
                
                links.append((filename, full_url, dataset_path))
                
        return links
    except Exception as e:
        logger.error(f"Error fetching links from {base_url}: {str(e)}")
        return []

def create_file_entry(entry):
    """Create a file entry without making HEAD requests"""
    filename, url, dataset_path, capture_name, malware, infection_date = entry
        
    return {
        "CaptureName": capture_name,
        "Malware": malware,
        "InfectionDate": infection_date,
        "FileName": filename,
        "URL": url,
        "Path": dataset_path
    }

def main():
    """Main function to run the scraper"""
    logger.info("Starting web scraper for dataset metadata collection")
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    try:
        with open(INPUT_JSON, "r") as f:
            datasets = json.load(f)
        logger.info(f"Loaded {len(datasets)} datasets from {INPUT_JSON}")
    except FileNotFoundError:
        logger.error(f"Input file not found: {INPUT_JSON}")
        return
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON in input file: {INPUT_JSON}")
        return
    
    results = []
    for dataset in tqdm(datasets, desc="Processing datasets"):
        base_url = dataset.get("Capture_URL")
        if not base_url:
            logger.warning(f"Missing Capture_URL in dataset: {dataset}")
            continue
            
        capture_name = dataset.get("Capture_Name", "Unknown")
        malware = dataset.get("Malware", "Unknown")
        infection_date = dataset.get("Infection_Date", "")
        
        logger.info(f"Crawling {base_url} for {capture_name}")
        file_links = get_all_file_links(base_url)
        logger.info(f"Found {len(file_links)} files for {capture_name}")
        
        for filename, url, dataset_path in file_links:
            entry = (filename, url, dataset_path, capture_name, malware, infection_date)
            results.append(create_file_entry(entry))
    
    if not results:
        logger.warning("No files found across all datasets")
        return
    
    if results:
        fieldnames = [
            "CaptureName", "Malware", "InfectionDate", "FileName", 
            "URL", "Path"
        ]
        
        try:
            with open(CSV_OUTPUT, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(results)
            logger.info(f"Metadata collection complete. Results saved to: {CSV_OUTPUT}")
            
            print(f"\nSummary:")
            print(f"- Total files found: {len(results)}")
            print(f"- Total unique datasets: {len(set(r['CaptureName'] for r in results))}")
            
        except Exception as e:
            logger.error(f"Error writing CSV file: {str(e)}")
    else:
        logger.warning("No results to write to CSV")

if __name__ == "__main__":
    main()