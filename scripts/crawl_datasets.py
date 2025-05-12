import requests
from bs4 import BeautifulSoup
import json

def extract_links_from_paragraphs(url):
    response = requests.get(url)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, 'html.parser')
    results = []

    for p in soup.find_all('p'):
        a_tag = p.find('a')
        if a_tag and a_tag.has_attr('href'):
            href = a_tag['href']
            title = a_tag.get_text(strip=True)
            results.append({'url': href, 'title': title})
    
    return results

url = "https://www.stratosphereips.org/datasets-normal"
links = extract_links_from_paragraphs(url)

with open("data/extracted_links.json", "w", encoding='utf-8') as f:
    json.dump(links, f, indent=2, ensure_ascii=False)

print("Saved to extracted_links.json")
