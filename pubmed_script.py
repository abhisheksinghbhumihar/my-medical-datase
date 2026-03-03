import pandas as pd
import os
import requests
import xml.etree.ElementTree as ET
import json
from tqdm import tqdm
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# ======================================================
# CONFIGURATION
# ======================================================
CSV_PATH = r"C:\Users\anjal\Downloads\oa_file_list.csv"
JSONL_OUTPUT_PATH = r"C:\generate_data\all_real_api_abstracts.jsonl"
TRACKER_FILE = r"C:\generate_data\processed_pmcids.txt"
MAX_WORKERS = 10  # Number of parallel requests (adjust based on your needs)
REQUEST_DELAY = 0.1  # Delay between requests per thread (in seconds)

def fetch_from_pubmed_api(pmcid):
    """Fetches the real Title and Abstract words from PubMed servers."""
    pmc_id_number = str(pmcid).replace("PMC", "").strip()
    url = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pmc&id={pmc_id_number}&retmode=xml"
    
    try:
        response = requests.get(url, timeout=10)
        if response.status_code != 200:
            return pmcid, None, None
            
        root = ET.fromstring(response.content)
        
        # Extract Full Title
        title_node = root.find(".//article-title")
        title = "".join(title_node.itertext()) if title_node is not None else ""
        
        # Extract Full Abstract
        abstract_node = root.find(".//abstract")
        if abstract_node is not None:
            paragraphs = abstract_node.findall(".//p")
            abstract = " ".join(["".join(p.itertext()) for p in paragraphs])
        else:
            abstract = ""
            
        title = " ".join(title.split())
        abstract = " ".join(abstract.split())
        
        return pmcid, title, abstract
        
    except Exception as e:
        return pmcid, None, None

def get_processed_ids():
    """Reads the tracker file so we don't download the same article twice."""
    if not os.path.exists(TRACKER_FILE):
        return set()
    with open(TRACKER_FILE, 'r') as f:
        return set(line.strip() for line in f)

def main():
    print("🚀 Loading CSV Index...")
    if not os.path.exists(CSV_PATH):
        print(f"❌ Error: CSV not found at {CSV_PATH}")
        return

    # Load only the column we need to save RAM
    df = pd.read_csv(CSV_PATH, usecols=['Accession ID'], low_memory=False)
    df_clean = df.dropna(subset=['Accession ID'])
    
    # Load previously processed IDs
    processed_ids = get_processed_ids()
    print(f"📂 Found {len(processed_ids)} articles already processed.")
    
    # Filter out the ones we already did
    df_remaining = df_clean[~df_clean['Accession ID'].isin(processed_ids)]
    remaining_ids = df_remaining['Accession ID'].tolist()
    total_to_fetch = len(remaining_ids)
    
    if total_to_fetch == 0:
        print("✅ All articles have already been downloaded!")
        return

    print(f"🌐 Fetching REAL text for {total_to_fetch} articles using {MAX_WORKERS} parallel workers...")
    print(f"⚠️ Effective rate: {MAX_WORKERS/REQUEST_DELAY:.0f} requests/second")
    
    os.makedirs(os.path.dirname(JSONL_OUTPUT_PATH), exist_ok=True)
    
    # Thread-safe file writing
    file_lock = Lock()
    valid_count = 0
    
    # Open files in append mode
    with open(JSONL_OUTPUT_PATH, 'a', encoding='utf-8') as outfile, \
         open(TRACKER_FILE, 'a', encoding='utf-8') as tracker:
        
        # Use ThreadPoolExecutor for parallel requests
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Submit all tasks
            futures = [executor.submit(fetch_from_pubmed_api, pmcid) for pmcid in remaining_ids]
            
            # Process results as they complete
            for future in tqdm(as_completed(futures), total=total_to_fetch, desc="Downloading Data"):
                pmcid, title, abstract = future.result()
                
                # Write to tracker (thread-safe)
                with file_lock:
                    tracker.write(f"{pmcid}\n")
                    tracker.flush()  # Ensure it's written immediately
                
                if title and abstract and len(abstract) > 100:
                    record = {
                        "Full title": title,
                        "Full abstract": abstract
                    }
                    # Write to JSONL (thread-safe)
                    with file_lock:
                        outfile.write(json.dumps(record, ensure_ascii=False) + '\n')
                        outfile.flush()
                        valid_count += 1
                
                # Small delay to avoid overwhelming the server
                time.sleep(REQUEST_DELAY / MAX_WORKERS)

    print(f"\n✅ Session Complete! Downloaded {valid_count} new REAL abstracts.")
    print(f"📁 Dataset saved at: {JSONL_OUTPUT_PATH}")

if __name__ == "__main__":
    main()
