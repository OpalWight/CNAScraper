import requests
from bs4 import BeautifulSoup
import json
import re
from langchain_text_splitters import RecursiveCharacterTextSplitter
from urllib.parse import urljoin

def get_structured_urls(base_url):
    """
    Crawls the main page to find all unique URLs for chapters and front-matter,
    and extracts their hierarchical context (Part title).
    """
    print("Phase 1: Starting structured URL extraction...")
    try:
        response = requests.get(base_url)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching the main URL: {e}")
        return []

    soup = BeautifulSoup(response.content, 'lxml')
    
    toc = soup.find('ol', class_='toc')
    if not toc:
        print("Could not find the Table of Contents.")
        return []

    structured_urls = []
    current_part_title = "General" # Default for items before the first "Part"

    for li in toc.find_all('li', recursive=False):
        # Check if this list item defines a new "Part"
        if 'part' in li.get('class', []):
            part_text_element = li.find('div', class_='toc-part-header')
            if part_text_element and part_text_element.find('span', class_='part-text'):
                 current_part_title = part_text_element.find('span', class_='part-text').get_text(strip=True)

        # Find all links within this list item (including nested ones)
        links = li.find_all('a')
        for a_tag in links:
            href = a_tag.get('href')
            if href and ('/chapter/' in href or '/front-matter/' in href):
                full_url = urljoin(base_url, href)
                structured_urls.append({
                    'url': full_url,
                    'part_title': current_part_title
                })
    
    # Remove potential duplicates while preserving order
    seen_urls = set()
    unique_structured_urls = []
    for item in structured_urls:
        if item['url'] not in seen_urls:
            unique_structured_urls.append(item)
            seen_urls.add(item['url'])

    print(f"Found {len(unique_structured_urls)} unique content URLs with structural context.")
    return unique_structured_urls

def scrape_and_clean_content(url):
    """
    Scrapes the title and main content from a given URL and cleans it.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching content from {url}: {e}")
        return None, None

    soup = BeautifulSoup(response.content, 'lxml')
    
    content_section = soup.find('section', class_='chapter')
    if not content_section:
        return None, None

    title_tag = content_section.find('h1', class_='entry-title')
    title = title_tag.get_text(strip=True) if title_tag else "No Title Found"

    text_parts = []
    learning_objectives = content_section.find('div', class_='textbox--learning-objectives')
    if learning_objectives:
        text_parts.append(learning_objectives.get_text(separator=' ', strip=True))

    for p_tag in content_section.find_all('p'):
        text_parts.append(p_tag.get_text(separator=' ', strip=True))

    full_text = ' '.join(text_parts)
    cleaned_text = re.sub(r'\s+', ' ', full_text).strip()
    
    return title, cleaned_text

def extract_chapter_number(url):
    """Extracts a chapter number like '8-4' from a URL slug."""
    match = re.search(r'/chapter/([\w-]+)/?$', url)
    if match:
        return match.group(1)
    return "N/A"

def main():
    """
    Main function to execute the entire scraping and processing pipeline.
    """
    base_url = "https://wtcs.pressbooks.pub/nurseassist/"
    
    # Phase 1: Get all structured URLs
    structured_urls = get_structured_urls(base_url)
    if not structured_urls:
        print("No URLs found. Exiting.")
        return

    # Phase 2 & 3: Scrape, clean, and chunk content with enhanced metadata
    print("\nPhase 2 & 3: Scraping content and generating chunks with enhanced metadata...")
    all_chunks = []
    
    text_splitter = RecursiveCharacterTextSplitter(
        separators=["\n\n", "\n", ". ", "? ", "! "],
        chunk_size=1000,
        chunk_overlap=150
    )

    for i, url_info in enumerate(structured_urls):
        url = url_info['url']
        part_title = url_info['part_title']
        
        print(f"Processing URL {i+1}/{len(structured_urls)}: {url}")
        title, content = scrape_and_clean_content(url)
        
        if not content:
            print(f"  -> No content found, skipping.")
            continue

        chapter_number = extract_chapter_number(url)
        chunks = text_splitter.split_text(content)
        
        for chunk_index, chunk in enumerate(chunks):
            all_chunks.append({
                'page_content': chunk,
                'metadata': {
                    'part_title': part_title,
                    'chapter_number': chapter_number,
                    'title': title,
                    'source_url': url,
                    'chunk_id': f"chunk_{chunk_index}"
                }
            })
    
    print(f"\nGenerated {len(all_chunks)} chunks in total.")

    # Phase 4: Save the output
    output_filename = 'scraped_content_enhanced.json'
    print(f"\nPhase 4: Saving structured data to {output_filename}...")
    try:
        with open(output_filename, 'w', encoding='utf-8') as f:
            json.dump(all_chunks, f, indent=4, ensure_ascii=False)
        print("Successfully saved the data.")
    except IOError as e:
        print(f"Error writing to file: {e}")

if __name__ == '__main__':
    main()