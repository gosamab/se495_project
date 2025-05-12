import os
import json
import requests
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from tqdm import tqdm

OUTPUT_DIR = "binetflow_files"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def download_binetflow_files(base_url):
    try:
        response = requests.get(base_url)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"[ERROR] Failed to access {base_url}: {e}")
        return

    print(f"[ACCESS] Had to access {base_url}")
    soup = BeautifulSoup(response.text, 'html.parser')
    for a_tag in soup.find_all('a', href=True):
        href = a_tag['href']
        if href.lower().endswith('.binetflow') or href.lower().endswith('.labeled'):
            print(f"[INFO] Found file link: {base_url} {href}")
            file_url = urljoin(base_url + '/', href)
            print(f"[INFO] File URL: {file_url}")
            file_name = os.path.basename(urlparse(file_url).path)
            file_path = os.path.join(OUTPUT_DIR, file_name)
            if os.path.exists(file_path):
                print(f"[SKIP] {file_name} already exists.")
                continue

            try:
                with requests.get(file_url, stream=True) as r:
                    r.raise_for_status()
                    with open(file_path, 'wb') as f:
                        for chunk in tqdm(r.iter_content(chunk_size=8192), desc=f"Downloading {file_name}", unit='B', unit_scale=True):
                            f.write(chunk)
                print(f"[DONE] {file_name} downloaded.")
            except requests.RequestException as e:
                print(f"[ERROR] Failed to download {file_url}: {e}")

def main():
    with open("data/extracted_links.json", "r", encoding='utf-8') as f:
        links = json.load(f)

    for entry in links:
        url = entry['url']
        print(f"[INFO] Checking {url}")
        download_binetflow_files(url)

if __name__ == "__main__":
    main()
