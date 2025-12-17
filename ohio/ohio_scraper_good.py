import requests
from bs4 import BeautifulSoup
import csv
import time
import os
import re
from urllib.parse import urljoin, urlparse
from datetime import datetime
import logging
import json

# Try to import PDF processing libraries
try:
    import PyPDF2
    PDF_SUPPORT = True
except ImportError:
    try:
        import pdfplumber
        PDF_SUPPORT = True
    except ImportError:
        PDF_SUPPORT = False
        print("Warning: Neither PyPDF2 nor pdfplumber found. PDF text extraction will be limited.")

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class OhioLegislatureScraper:
    def __init__(self):
        self.base_url = "https://legislature.ohio.gov"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none'
        })
        
        # Keywords to search for (exact matches only)
        self.keywords = [
            "Immigration", "Citizenship", "Alien", "Migrant", 
            "Undocumented", "Visa", "Border", "Foreign"
        ]
        
        # Create directory for bill texts
        self.bill_text_dir = "ohio_bill_texts"
        os.makedirs(self.bill_text_dir, exist_ok=True)
        
        # CSV fieldnames
        self.csv_fieldnames = [
            'State', 'GA', 'Policy (bill) identifier', 'Policy sponsor', 
            'Policy sponsor party', 'Link to bill', 'bill text', 'Cosponsor',
            'Act identifier', 'Matched keywords', 'Introduced date', 
            'Effective date', 'Passed introduced chamber date', 
            'Passed second chamber date', 'Dead date', 'Enacted (Y/N)', 
            'Enacted Date'
        ]
        
        # Track failed requests to avoid repeated attempts
        self.failed_urls = set()
        self.successful_bills = []
    
    def courtesy_pause(self, seconds=3):
        """Add courtesy pause to avoid taxing Ohio servers - increased default"""
        logger.info(f"Pausing for {seconds} seconds...")
        time.sleep(seconds)
    
    def test_connection(self):
        """Test basic connectivity to Ohio Legislature website"""
        try:
            logger.info("Testing connection to Ohio Legislature website...")
            response = self.session.get(self.base_url, timeout=30)
            response.raise_for_status()
            logger.info(f"Connection successful! Status: {response.status_code}")
            return True
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
    
    def get_bills_from_listing_page(self, assembly="136"):
        """Try to get bills from the main legislation listing page"""
        bills = []
        
        try:
            # Try the main legislation page for the assembly
            listing_url = f"{self.base_url}/legislation/{assembly}"
            logger.info(f"Trying to access listing page: {listing_url}")
            
            response = self.session.get(listing_url, timeout=30)
            logger.info(f"Listing page response: {response.status_code}")
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Look for bill links
                bill_links = soup.find_all('a', href=re.compile(rf'/legislation/{assembly}/(sb|hb)\d+', re.IGNORECASE))
                
                for link in bill_links:
                    href = link.get('href')
                    bill_url = urljoin(self.base_url, href)
                    
                    # Extract bill number and type
                    match = re.search(rf'/legislation/{assembly}/(sb|hb)(\d+)', href, re.IGNORECASE)
                    if match:
                        bill_type = match.group(1).upper()
                        bill_num = match.group(2)
                        bill_number = f"{bill_type}{bill_num}"
                        
                        bills.append({
                            'number': bill_number,
                            'url': bill_url,
                            'type': bill_type,
                            'assembly': assembly
                        })
                
                logger.info(f"Found {len(bills)} bills from listing page")
            
        except Exception as e:
            logger.error(f"Error accessing listing page: {e}")
        
        return bills
    
    def check_bill_exists(self, bill_type, bill_num, assembly, max_retries=3):
        """Check if a specific bill exists with retry logic"""
        bill_url = f"{self.base_url}/legislation/{assembly}/{bill_type.lower()}{bill_num}"
        
        if bill_url in self.failed_urls:
            return None
        
        for attempt in range(max_retries):
            try:
                # Use GET instead of HEAD as some servers don't handle HEAD well
                response = self.session.get(bill_url, timeout=30)
                
                if response.status_code == 200:
                    # Check if the page actually contains bill content
                    if "not found" not in response.text.lower() and len(response.text) > 1000:
                        logger.info(f"Found {bill_type}{bill_num}")
                        return {
                            'number': f'{bill_type}{bill_num}',
                            'url': bill_url,
                            'type': bill_type,
                            'assembly': assembly
                        }
                elif response.status_code == 404:
                    # Bill doesn't exist
                    return None
                elif response.status_code == 500:
                    logger.warning(f"Server error for {bill_type}{bill_num} (attempt {attempt + 1})")
                    if attempt < max_retries - 1:
                        self.courtesy_pause(5)  # Longer pause on server errors
                        continue
                    else:
                        self.failed_urls.add(bill_url)
                        return None
                else:
                    logger.warning(f"Unexpected status {response.status_code} for {bill_type}{bill_num}")
                    return None
                
            except requests.exceptions.Timeout:
                logger.warning(f"Timeout for {bill_type}{bill_num} (attempt {attempt + 1})")
                if attempt < max_retries - 1:
                    self.courtesy_pause(5)
                    continue
                else:
                    self.failed_urls.add(bill_url)
                    return None
            except Exception as e:
                logger.error(f"Error checking {bill_type}{bill_num}: {e}")
                if attempt < max_retries - 1:
                    self.courtesy_pause(3)
                    continue
                else:
                    self.failed_urls.add(bill_url)
                    return None
        
        return None
    
    def get_bills_by_systematic_search(self, assembly="136", max_bills_per_type=None, max_consecutive_failures=50):
        """Systematically search for bills with smart termination"""
        bills = []
        
        # Set reasonable default if no max specified
        if max_bills_per_type is None:
            max_bills_per_type = 2000  # High limit, but smart termination will stop earlier
        
        logger.info(f"Starting systematic search for Assembly {assembly}")
        logger.info(f"Max bills per type: {max_bills_per_type}, Smart termination: {max_consecutive_failures} consecutive failures")
        
        # Search Senate Bills
        logger.info("Searching for Senate Bills...")
        consecutive_failures = 0
        bill_num = 1
        
        while bill_num <= max_bills_per_type:
            bill = self.check_bill_exists("SB", bill_num, assembly)
            
            if bill:
                bills.append(bill)
                consecutive_failures = 0
                self.successful_bills.append(bill['number'])
                logger.info(f"✓ Found {bill['number']} - Total SB found: {len([b for b in bills if b['type'] == 'SB'])}")
            else:
                consecutive_failures += 1
                
                # Smart termination - stop after consecutive failures
                if consecutive_failures >= max_consecutive_failures:
                    logger.info(f"🛑 Stopping SB search after {consecutive_failures} consecutive failures at SB{bill_num}")
                    break
            
            # Courtesy pause between requests
            self.courtesy_pause(2)
            
            # Progress update every 25 bills
            if bill_num % 25 == 0:
                sb_count = len([b for b in bills if b['type'] == 'SB'])
                logger.info(f"📊 Progress: Checked SB1-SB{bill_num}, found {sb_count} Senate Bills")
            
            bill_num += 1
        
        # Longer pause between bill types
        self.courtesy_pause(5)
        
        # Search House Bills
        logger.info("Searching for House Bills...")
        consecutive_failures = 0
        bill_num = 1
        
        while bill_num <= max_bills_per_type:
            bill = self.check_bill_exists("HB", bill_num, assembly)
            
            if bill:
                bills.append(bill)
                consecutive_failures = 0
                self.successful_bills.append(bill['number'])
                logger.info(f"✓ Found {bill['number']} - Total HB found: {len([b for b in bills if b['type'] == 'HB'])}")
            else:
                consecutive_failures += 1
                
                # Smart termination - stop after consecutive failures
                if consecutive_failures >= max_consecutive_failures:
                    logger.info(f"🛑 Stopping HB search after {consecutive_failures} consecutive failures at HB{bill_num}")
                    break
            
            # Courtesy pause between requests
            self.courtesy_pause(2)
            
            # Progress update every 25 bills
            if bill_num % 25 == 0:
                hb_count = len([b for b in bills if b['type'] == 'HB'])
                logger.info(f"📊 Progress: Checked HB1-HB{bill_num}, found {hb_count} House Bills")
            
            bill_num += 1
        
        logger.info(f"Systematic search complete. Found {len(bills)} total bills")
        return bills
    
    def get_all_bills(self, assembly="136", max_bills_per_type=None, max_consecutive_failures=50):
        """Get all bills using multiple strategies with smart termination"""
        bills = []
        
        # Test connection first
        if not self.test_connection():
            logger.error("Cannot connect to Ohio Legislature website")
            return []
        
        # Strategy 1: Try to get bills from listing page
        logger.info("Strategy 1: Trying listing page...")
        listing_bills = self.get_bills_from_listing_page(assembly)
        bills.extend(listing_bills)
        
        # Strategy 2: If listing didn't work well, try systematic search
        if len(bills) < 10:
            logger.info("Strategy 2: Listing page didn't yield many results, trying systematic search...")
            self.courtesy_pause(3)
            systematic_bills = self.get_bills_by_systematic_search(
                assembly, 
                max_bills_per_type=max_bills_per_type,
                max_consecutive_failures=max_consecutive_failures
            )
            bills.extend(systematic_bills)
        
        # Remove duplicates
        unique_bills = []
        seen_urls = set()
        for bill in bills:
            if bill['url'] not in seen_urls:
                unique_bills.append(bill)
                seen_urls.add(bill['url'])
        
        logger.info(f"Found {len(unique_bills)} total unique bills in Assembly {assembly}")
        return unique_bills
    
    def extract_text_from_pdf(self, pdf_content):
        """Extract text from PDF content using available libraries"""
        if not PDF_SUPPORT:
            return "[PDF file - install PyPDF2 or pdfplumber for text extraction]"
        
        try:
            # Try with pdfplumber first (generally more reliable)
            if 'pdfplumber' in globals():
                import io
                import pdfplumber
                
                with pdfplumber.open(io.BytesIO(pdf_content)) as pdf:
                    text = ""
                    for page in pdf.pages:
                        page_text = page.extract_text()
                        if page_text:
                            text += page_text + "\n"
                    return text.strip()
            
            # Fallback to PyPDF2
            elif 'PyPDF2' in globals():
                import io
                import PyPDF2
                
                pdf_reader = PyPDF2.PdfReader(io.BytesIO(pdf_content))
                text = ""
                for page in pdf_reader.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text += page_text + "\n"
                return text.strip()
                
        except Exception as e:
            logger.error(f"Error extracting PDF text: {e}")
            return f"[PDF text extraction failed: {str(e)}]"
        
        return "[PDF file - no extraction library available]"
    
    def check_bill_for_keywords(self, bill_text):
        """Check if bill text contains any of the target keywords (exact matches)"""
        if not bill_text:
            return []
        
        matched_keywords = []
        # Use word boundaries to ensure exact matches
        for keyword in self.keywords:
            pattern = r'\b' + re.escape(keyword) + r'\b'
            if re.search(pattern, bill_text, re.IGNORECASE):
                matched_keywords.append(keyword)
        
        return matched_keywords
    
    def get_bill_text_and_save(self, bill_number, bill_text_url, assembly):
        """Get bill text from URL and save as.txt file"""
        bill_text = ""
        bill_text_file_path = os.path.join(self.bill_text_dir, f"GA{assembly}_{bill_number}.txt")
        
        if not bill_text_url:
            return "", ""
        
        try:
            text_response = self.session.get(bill_text_url, timeout=30)
            text_response.raise_for_status()
            
            # Check if it's a PDF file
            content_type = text_response.headers.get('content-type', '').lower()
            is_pdf = 'pdf' in content_type or bill_text_url.lower().endswith('.pdf')
            
            if is_pdf:
                logger.info(f"Processing PDF for {bill_number} (Assembly {assembly})")
                bill_text = self.extract_text_from_pdf(text_response.content)
            else:
                # Handle HTML content
                text_soup = BeautifulSoup(text_response.content, 'html.parser')
                
                # Remove script and style elements
                for script in text_soup(["script", "style"]):
                    script.decompose()
                
                # Get text and clean it up
                bill_text = text_soup.get_text()
                
                # Clean up whitespace
                lines = (line.strip() for line in bill_text.splitlines())
                chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
                bill_text = '\n'.join(chunk for chunk in chunks if chunk)
            
            # Save as.txt file
            with open(bill_text_file_path, 'w', encoding='utf-8') as f:
                f.write(f"Bill: {bill_number}\n")
                f.write(f"General Assembly: {assembly}\n")
                f.write(f"Source URL: {bill_text_url}\n")
                f.write(f"Extracted on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write("=" * 80 + "\n\n")
                f.write(bill_text)
            
            logger.info(f"Saved text file for {bill_number} (Assembly {assembly})")
            return bill_text, bill_text_file_path
            
        except Exception as e:
            logger.error(f"Error processing bill text for {bill_number} (Assembly {assembly}): {e}")
            # Create an error file
            error_text = f"Error extracting text: {str(e)}"
            with open(bill_text_file_path, 'w', encoding='utf-8') as f:
                f.write(f"Bill: {bill_number}\n")
                f.write(f"General Assembly: {assembly}\n")
                f.write(f"Source URL: {bill_text_url}\n")
                f.write(f"Error on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write("=" * 80 + "\n\n")
                f.write(error_text)
            
            return error_text, bill_text_file_path
    
    def get_bill_details(self, bill):
        """Get detailed information about a specific bill"""
        logger.info(f"Processing {bill['number']} from Assembly {bill['assembly']}...")
        
        try:
            # Get main bill page with retries
            max_retries = 3
            response = None
            
            for attempt in range(max_retries):
                try:
                    response = self.session.get(bill['url'], timeout=30)
                    response.raise_for_status()
                    break
                except Exception as e:
                    logger.warning(f"Attempt {attempt + 1} failed for {bill['number']}: {e}")
                    if attempt < max_retries - 1:
                        self.courtesy_pause(5)
                    else:
                        logger.error(f"All attempts failed for {bill['number']}")
                        return None
            
            if not response:
                return None
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Check if bill page actually exists and has content
            if "not found" in response.text.lower() or len(response.text) < 1000:
                logger.info(f"Bill {bill['number']} appears to be empty or not found")
                return None
            
            # Get bill text URL from "Current Version" section
            bill_text_url = None
            current_version_section = soup.find('h2', string='Current Version')
            if current_version_section:
                next_p = current_version_section.find_next('p')
                if next_p:
                    text_link = next_p.find('a')
                    if text_link and text_link.get('href'):
                        bill_text_url = urljoin(self.base_url, text_link['href'])
            
            # If no "Current Version" found, try other patterns
            if not bill_text_url:
                # Look for any PDF or document links
                doc_links = soup.find_all('a', href=re.compile(r'\.(pdf|doc|docx)$', re.IGNORECASE))
                if doc_links:
                    bill_text_url = urljoin(self.base_url, doc_links[0]['href'])
                else:
                    # Try to find any document links
                    doc_links = soup.find_all('a', string=re.compile(r'(text|document|pdf)', re.IGNORECASE))
                    if doc_links:
                        bill_text_url = urljoin(self.base_url, doc_links[0]['href'])
            
            # Get bill text and save as.txt
            self.courtesy_pause(2)
            bill_text, bill_text_file_path = self.get_bill_text_and_save(
                bill['number'], bill_text_url, bill['assembly']
            )
            
            # Check for keywords
            matched_keywords = self.check_bill_for_keywords(bill_text)
            
            # If no keywords found, skip this bill
            if not matched_keywords:
                logger.info(f"No matching keywords found in {bill['number']} (Assembly {bill['assembly']})")
                # Remove the text file since we don't need it
                if os.path.exists(bill_text_file_path):
                    os.remove(bill_text_file_path)
                return None
            
            logger.info(f"✓ MATCH! Found keywords {matched_keywords} in {bill['number']} (Assembly {bill['assembly']})")
            
            # Get sponsor information
            sponsors = []
            sponsor_party = ""
            sponsor_section = soup.find('h2', string='Primary Sponsors')
            if sponsor_section:
                sponsor_div = sponsor_section.find_next('div')
                if sponsor_div:
                    sponsor_links = sponsor_div.find_all('a')
                    sponsors = [link.get_text(strip=True) for link in sponsor_links]
            
            # Get cosponsors
            cosponsors = []
            cosponsor_section = soup.find('h2', string='Cosponsors')
            if cosponsor_section:
                cosponsor_div = cosponsor_section.find_next('div')
                if cosponsor_div:
                    cosponsor_links = cosponsor_div.find_all('a')
                    cosponsors = [link.get_text(strip=True) for link in cosponsor_links]
            
            # Get status information
            self.courtesy_pause(2)
            status_info = self.get_bill_status(bill)
            
            # Compile bill information
            bill_info = {
                'State': 'Ohio',
                'GA': bill['assembly'],
                'Policy (bill) identifier': bill['number'],
                'Policy sponsor': '; '.join(sponsors) if sponsors else '',
                'Policy sponsor party': sponsor_party,
                'Link to bill': bill['url'],
                'bill text': bill_text_file_path,
                'Cosponsor': '; '.join(cosponsors) if cosponsors else '',
                'Act identifier': '',
                'Matched keywords': '; '.join(matched_keywords),
                'Introduced date': status_info.get('introduced_date', ''),
                'Effective date': status_info.get('effective_date', ''),
                'Passed introduced chamber date': status_info.get('passed_first_chamber', ''),
                'Passed second chamber date': status_info.get('passed_second_chamber', ''),
                'Dead date': status_info.get('dead_date', ''),
                'Enacted (Y/N)': status_info.get('enacted', 'N'),
                'Enacted Date': status_info.get('enacted_date', '')
            }
            
            return bill_info
            
        except Exception as e:
            logger.error(f"Error processing bill {bill['number']} (Assembly {bill['assembly']}): {e}")
            return None
    
    def get_bill_status(self, bill):
        """Get status information from the bill's status page"""
        status_url = f"{bill['url']}/status"
        status_info = {
            'introduced_date': '',
            'effective_date': '',
            'passed_first_chamber': '',
            'passed_second_chamber': '',
            'dead_date': '',
            'enacted': 'N',
            'enacted_date': ''
        }
    
        try:
            response = self.session.get(status_url, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find the status table
            status_table = soup.find('table', class_='data-grid legislation-status-table')
            if not status_table:
                logger.warning(f"No status table found for {bill['number']}")
                return status_info
            
            # Get all rows from tbody
            tbody = status_table.find('tbody')
            if not tbody:
                logger.warning(f"No tbody found in status table for {bill['number']}")
                return status_info
            
            rows = tbody.find_all('tr')
            logger.info(f"Found {len(rows)} status rows for {bill['number']}")
            
            # Determine bill's originating chamber
            bill_type = bill['type']  # 'SB' or 'HB'
            originating_chamber = 'Senate' if bill_type == 'SB' else 'House'
            second_chamber = 'House' if bill_type == 'SB' else 'Senate'
            
            for row in rows:
                try:
                    # Get date from first cell (th with class="date-cell")
                    date_cell = row.find('th', class_='date-cell')
                    if not date_cell:
                        continue
                    
                    date_span = date_cell.find('span')
                    if not date_span:
                        continue
                    
                    date = date_span.get_text(strip=True)
                    
                    # Get chamber from second cell
                    chamber_cell = row.find('td', class_='chamber-cell')
                    chamber_span = chamber_cell.find('span') if chamber_cell else None
                    chamber = chamber_span.get_text(strip=True) if chamber_span else ''
                    
                    # Get action from third cell
                    action_cell = row.find('td', class_='action-cell')
                    if not action_cell:
                        continue
                    
                    # Get action text - could be direct text or in a span
                    action_span = action_cell.find('span')
                    if action_span:
                        action = action_span.get_text(strip=True)
                    else:
                        # Get direct text, handling <br> tags
                        action = action_cell.get_text(strip=True)
                    
                    action_lower = action.lower()
                    
                    logger.debug(f"{bill['number']}: {date} | {chamber} | {action}")
                    
                    # Parse different types of actions
                    if 'introduced' in action_lower:
                        status_info['introduced_date'] = date
                        logger.info(f"Found introduced date for {bill['number']}: {date}")
                    
                    elif 'effective' in action_lower:
                        status_info['effective_date'] = date
                        logger.info(f"Found effective date for {bill['number']}: {date}")
                    
                    elif 'signed by the governor' in action_lower:
                        status_info['enacted'] = 'Y'
                        status_info['enacted_date'] = date
                        logger.info(f"Found enacted date for {bill['number']}: {date}")
                    
                    elif 'passed' in action_lower and chamber:
                        # Check if this is the originating chamber
                        if chamber == originating_chamber:
                            if not status_info['passed_first_chamber']:  # Only set if not already set
                                status_info['passed_first_chamber'] = date
                                logger.info(f"Found passed first chamber date for {bill['number']}: {date} ({chamber})")
                        
                        # Check if this is the second chamber
                        elif chamber == second_chamber:
                            if not status_info['passed_second_chamber']:  # Only set if not already set
                                status_info['passed_second_chamber'] = date
                                logger.info(f"Found passed second chamber date for {bill['number']}: {date} ({chamber})")
                    
                    # Look for other potential "dead" indicators
                    elif any(dead_word in action_lower for dead_word in ['withdrawn', 'died', 'failed', 'rejected', 'vetoed']):
                        if not status_info['dead_date']:  # Only set if not already set
                            status_info['dead_date'] = date
                            logger.info(f"Found potential dead date for {bill['number']}: {date} ({action})")
                    
                except Exception as e:
                    logger.error(f"Error parsing status row for {bill['number']}: {e}")
                    continue
            
            # Log final status for debugging
            logger.info(f"Final status for {bill['number']}: Introduced={status_info['introduced_date']}, "
                    f"First Chamber={status_info['passed_first_chamber']}, "
                    f"Second Chamber={status_info['passed_second_chamber']}, "
                    f"Enacted={status_info['enacted']}, "
                    f"Enacted Date={status_info['enacted_date']}, "
                    f"Effective={status_info['effective_date']}")
            
        except Exception as e:
            logger.error(f"Error fetching status for {bill['number']} (Assembly {bill['assembly']}): {e}")
        
        return status_info
        
    def scrape_bills(self, assemblies=None, output_file="ohio_immigration_bills.csv", max_bills_per_type=None, max_consecutive_failures=50):
        """Main method to scrape bills from specified assemblies with smart termination"""
        logger.info("Starting Ohio Legislature bill scraping...")
        
        # If no assemblies specified, use current one
        if assemblies is None:
            assemblies = ["136"]
        elif isinstance(assemblies, str):
            assemblies = [assemblies]
        
        all_matching_bills = []
        
        # Process each assembly
        for assembly in assemblies:
            logger.info(f"\n{'='*60}")
            logger.info(f"PROCESSING GENERAL ASSEMBLY {assembly}")
            logger.info(f"Smart termination: {max_consecutive_failures} consecutive failures")
            if max_bills_per_type:
                logger.info(f"Max bills per type: {max_bills_per_type}")
            else:
                logger.info("No artificial limit on bills per type")
            logger.info(f"{'='*60}")
            
            try:
                # Get all bills from this assembly
                all_bills = self.get_all_bills(
                    assembly, 
                    max_bills_per_type=max_bills_per_type,
                    max_consecutive_failures=max_consecutive_failures
                )
                logger.info(f"Found {len(all_bills)} total bills in Assembly {assembly}")
                
                if len(all_bills) == 0:
                    logger.warning(f"No bills found for Assembly {assembly}. This might indicate an issue with the website or our scraping method.")
                    continue
                
                # Process each bill and collect matching ones
                assembly_matching_bills = []
                
                for i, bill in enumerate(all_bills, 1):
                    logger.info(f"Processing bill {i}/{len(all_bills)}: {bill['number']} (Assembly {assembly})")
                    
                    bill_info = self.get_bill_details(bill)
                    if bill_info:
                        assembly_matching_bills.append(bill_info)
                        all_matching_bills.append(bill_info)
                        logger.info(f"✓ Added {bill['number']} to results")
                    
                    # Courtesy pause between bills
                    self.courtesy_pause(3)
                
                logger.info(f"Assembly {assembly} complete: {len(assembly_matching_bills)} matching bills found")
                
                # Longer pause between assemblies
                if assembly != assemblies[-1]:  # Don't pause after the last assembly
                    self.courtesy_pause(10)
                
            except Exception as e:
                logger.error(f"Error processing Assembly {assembly}: {e}")
                continue
        
        # Save results to CSV
        logger.info(f"\nSaving {len(all_matching_bills)} total matching bills to {output_file}")
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=self.csv_fieldnames)
            writer.writeheader()
            writer.writerows(all_matching_bills)
        
        # Print summary
        print(f"\n{'='*60}")
        print(f"SCRAPING SUMMARY")
        print(f"{'='*60}")
        print(f"Total bills processed: {len(self.successful_bills)}")
        print(f"Bills with immigration keywords: {len(all_matching_bills)}")
        print(f"Failed URLs: {len(self.failed_urls)}")
        print(f"Results saved to: {output_file}")
        print(f"Bill texts saved to: {self.bill_text_dir}/")
        print(f"{'='*60}")
        
        logger.info(f"Scraping complete! Found {len(all_matching_bills)} bills with immigration-related keywords across {len(assemblies)} assemblies.")
        return all_matching_bills

def main():
    """Main function with assembly selection controls"""
    
    # ===== EASY CONFIGURATION - EDIT THESE VALUES =====
    
    # Assembly Options - Choose one:
    ASSEMBLIES_TO_SEARCH = ["135"]                    # Current assembly only
    # ASSEMBLIES_TO_SEARCH = ["135"]                  # Previous assembly only  
    # ASSEMBLIES_TO_SEARCH = ["136", "135"]           # Current + previous
    # ASSEMBLIES_TO_SEARCH = ["136", "135", "134"]    # Recent assemblies
    # ASSEMBLIES_TO_SEARCH = ["134", "133", "132"]    # Older assemblies
    
    # Search Limits - Choose one:
    MAX_BILLS_PER_TYPE = None                         # No limit (recommended)
    # MAX_BILLS_PER_TYPE = 100                        # Limit for faster testing
    # MAX_BILLS_PER_TYPE = 500                        # Medium limit
    
    # Smart Termination - Stop after X consecutive failures:
    MAX_CONSECUTIVE_FAILURES = 50                     # Default: 50 consecutive failures
    # MAX_CONSECUTIVE_FAILURES = 20                   # More aggressive termination
    # MAX_CONSECUTIVE_FAILURES = 100                  # More thorough search
    
    # Output filename:
    OUTPUT_FILENAME = "ohio_immigration_bills_GA135.csv"
    
    # ==================================================
    
    # Check for PDF libraries
    if not PDF_SUPPORT:
        print("\n" + "="*60)
        print("PDF PROCESSING SETUP RECOMMENDED")
        print("="*60)
        print("For better PDF text extraction, install one of these:")
        print("pip install pdfplumber  # Recommended")
        print("pip install PyPDF2      # Alternative")
        print("="*60 + "\n")
    
    scraper = OhioLegislatureScraper()
    
    # Show configuration
    print("Ohio Legislature Immigration Bill Scraper")
    print("="*50)
    print(f"Configuration:")
    print(f"  Assemblies: {ASSEMBLIES_TO_SEARCH}")
    print(f"  Max bills per type: {MAX_BILLS_PER_TYPE if MAX_BILLS_PER_TYPE else 'No limit'}")
    print(f"  Smart termination: {MAX_CONSECUTIVE_FAILURES} consecutive failures")
    print(f"  Output file: {OUTPUT_FILENAME}")
    print("="*50)
    
    try:
        results = scraper.scrape_bills(
            assemblies=ASSEMBLIES_TO_SEARCH, 
            output_file=OUTPUT_FILENAME,
            max_bills_per_type=MAX_BILLS_PER_TYPE,
            max_consecutive_failures=MAX_CONSECUTIVE_FAILURES
        )
        
    except KeyboardInterrupt:
        print("\nScraping interrupted by user")
        print(f"Partial results may be available in the output files")
    except Exception as e:
        logger.error(f"Scraping failed: {e}")

if __name__ == "__main__":
    main()