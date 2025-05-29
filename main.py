'''
JobPortal Scraper - Automated job listing aggregator from multiple Bangladeshi job portals.
Author: Taj Uddin
Description: This script scrapes job listings, stores them in a database, performs salary analysis, and sends daily email reports.
Note: Ensure you have the required environment variables set for database and email configurations.
'''

import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import pandas as pd
import mysql.connector
from mysql.connector import Error
import psycopg2
from psycopg2 import sql
import hashlib
import time
import random
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
import re
from urllib.parse import urljoin, urlparse
import json
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional
import schedule
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('job_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class JobListing:
    """Data class for job listings"""
    title: str
    company: str
    location: str
    salary: Optional[str]
    description: str
    requirements: str
    posted_date: str
    deadline: Optional[str]
    job_type: str
    experience: str
    url: str
    source: str
    hash_id: str

class DatabaseManager:
    """Handles database operations for both MySQL and PostgreSQL"""
    
    def __init__(self, db_type='mysql'):
        self.db_type = db_type
        self.connection = None
        self.connect()
        self.create_tables()
    
    def connect(self):
        """Establish database connection"""
        try:
            if self.db_type == 'mysql':
                self.connection = mysql.connector.connect(
                    host=os.getenv('MYSQL_HOST', 'localhost'),
                    database=os.getenv('MYSQL_DATABASE', 'job_portal'),
                    user=os.getenv('MYSQL_USER', 'root'),
                    password=os.getenv('MYSQL_PASSWORD', '')
                )
            elif self.db_type == 'postgresql':
                self.connection = psycopg2.connect(
                    host=os.getenv('POSTGRES_HOST', 'localhost'),
                    database=os.getenv('POSTGRES_DATABASE', 'job_portal'),
                    user=os.getenv('POSTGRES_USER', 'postgres'),
                    password=os.getenv('POSTGRES_PASSWORD', '')
                )
            logger.info(f"Connected to {self.db_type} database successfully")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise
    
    def create_tables(self):
        """Create necessary tables"""
        if self.db_type == 'mysql':
            create_table_query = """
            CREATE TABLE IF NOT EXISTS job_listings (
                id INT AUTO_INCREMENT PRIMARY KEY,
                hash_id VARCHAR(64) UNIQUE NOT NULL,
                title VARCHAR(255) NOT NULL,
                company VARCHAR(255) NOT NULL,
                location VARCHAR(255),
                salary VARCHAR(100),
                description TEXT,
                requirements TEXT,
                posted_date DATE,
                deadline DATE,
                job_type VARCHAR(50),
                experience VARCHAR(100),
                url TEXT,
                source VARCHAR(50),
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT TRUE,
                INDEX idx_hash_id (hash_id),
                INDEX idx_company (company),
                INDEX idx_location (location),
                INDEX idx_posted_date (posted_date)
            )
            """
        else:  # PostgreSQL
            create_table_query = """
            CREATE TABLE IF NOT EXISTS job_listings (
                id SERIAL PRIMARY KEY,
                hash_id VARCHAR(64) UNIQUE NOT NULL,
                title VARCHAR(255) NOT NULL,
                company VARCHAR(255) NOT NULL,
                location VARCHAR(255),
                salary VARCHAR(100),
                description TEXT,
                requirements TEXT,
                posted_date DATE,
                deadline DATE,
                job_type VARCHAR(50),
                experience VARCHAR(100),
                url TEXT,
                source VARCHAR(50),
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT TRUE
            );
            CREATE INDEX IF NOT EXISTS idx_hash_id ON job_listings(hash_id);
            CREATE INDEX IF NOT EXISTS idx_company ON job_listings(company);
            CREATE INDEX IF NOT EXISTS idx_location ON job_listings(location);
            CREATE INDEX IF NOT EXISTS idx_posted_date ON job_listings(posted_date);
            """
        
        cursor = self.connection.cursor()
        try:
            if self.db_type == 'postgresql':
                for query in create_table_query.split(';'):
                    if query.strip():
                        cursor.execute(query)
            else:
                cursor.execute(create_table_query)
            self.connection.commit()
            logger.info("Tables created successfully")
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            raise
        finally:
            cursor.close()
    
    def insert_job(self, job: JobListing) -> bool:
        """Insert job listing into database"""
        cursor = self.connection.cursor()
        try:
            if self.db_type == 'mysql':
                query = """
                INSERT IGNORE INTO job_listings 
                (hash_id, title, company, location, salary, description, requirements, 
                 posted_date, deadline, job_type, experience, url, source)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
            else:  # PostgreSQL
                query = """
                INSERT INTO job_listings 
                (hash_id, title, company, location, salary, description, requirements, 
                 posted_date, deadline, job_type, experience, url, source)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (hash_id) DO NOTHING
                """
            
            values = (
                job.hash_id, job.title, job.company, job.location, job.salary,
                job.description, job.requirements, job.posted_date, job.deadline,
                job.job_type, job.experience, job.url, job.source
            )
            
            cursor.execute(query, values)
            self.connection.commit()
            
            if cursor.rowcount > 0:
                logger.info(f"New job inserted: {job.title} at {job.company}")
                return True
            return False
            
        except Exception as e:
            logger.error(f"Error inserting job: {e}")
            self.connection.rollback()
            return False
        finally:
            cursor.close()
    
    def get_salary_analysis(self) -> Dict:
        """Get salary analysis from database"""
        cursor = self.connection.cursor()
        try:
            query = """
            SELECT 
                location,
                COUNT(*) as job_count,
                AVG(CAST(REGEXP_REPLACE(salary, '[^0-9]', '') AS UNSIGNED)) as avg_salary
            FROM job_listings 
            WHERE salary IS NOT NULL AND salary != ''
            GROUP BY location
            ORDER BY job_count DESC
            LIMIT 10
            """
            
            if self.db_type == 'postgresql':
                query = """
                SELECT 
                    location,
                    COUNT(*) as job_count,
                    AVG(CAST(REGEXP_REPLACE(salary, '[^0-9]', '', 'g') AS INTEGER)) as avg_salary
                FROM job_listings 
                WHERE salary IS NOT NULL AND salary != ''
                GROUP BY location
                ORDER BY job_count DESC
                LIMIT 10
                """
            
            cursor.execute(query)
            results = cursor.fetchall()
            
            return {
                'location_analysis': [
                    {'location': row[0], 'job_count': row[1], 'avg_salary': row[2]}
                    for row in results
                ]
            }
        except Exception as e:
            logger.error(f"Error in salary analysis: {e}")
            return {}
        finally:
            cursor.close()
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")

class WebScraper:
    """Base web scraper class with common functionality"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.driver = None
        self.setup_selenium()
    
    def setup_selenium(self):
        """Setup Selenium WebDriver with proper configuration"""
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
        
        try:
            self.driver = webdriver.Chrome(options=chrome_options)
            self.wait = WebDriverWait(self.driver, 10)
            logger.info("Selenium WebDriver initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing WebDriver: {e}")
            raise
    
    def get_page_content(self, url: str, use_selenium: bool = False) -> Optional[BeautifulSoup]:
        """Get page content using requests or Selenium"""
        try:
            if use_selenium:
                self.driver.get(url)
                time.sleep(random.uniform(2, 4))  # Random delay
                html_content = self.driver.page_source
            else:
                response = self.session.get(url, timeout=10)
                response.raise_for_status()
                html_content = response.text
            
            return BeautifulSoup(html_content, 'html.parser')
            
        except Exception as e:
            logger.error(f"Error fetching page {url}: {e}")
            return None
    
    def generate_hash_id(self, job_data: Dict) -> str:
        """Generate unique hash ID for job listing"""
        unique_string = f"{job_data['title']}{job_data['company']}{job_data['location']}"
        return hashlib.md5(unique_string.encode()).hexdigest()
    
    def clean_text(self, text: str) -> str:
        """Clean and normalize text"""
        if not text:
            return ""
        return re.sub(r'\s+', ' ', text.strip())
    
    def parse_salary(self, salary_text: str) -> Optional[str]:
        """Parse and normalize salary information"""
        if not salary_text:
            return None
        
        # Remove extra spaces and normalize
        salary_text = self.clean_text(salary_text)
        
        # Extract salary patterns (BDT, Taka, etc.)
        salary_patterns = [
            r'(\d+(?:,\d+)*)\s*(?:to|-)?\s*(\d+(?:,\d+)*)?\s*(?:BDT|Taka|TK)',
            r'BDT\s*(\d+(?:,\d+)*)\s*(?:to|-)?\s*(\d+(?:,\d+)*)?',
            r'(\d+(?:,\d+)*)\s*(?:to|-)?\s*(\d+(?:,\d+)*)?'
        ]
        
        for pattern in salary_patterns:
            match = re.search(pattern, salary_text, re.IGNORECASE)
            if match:
                return salary_text
        
        return salary_text if salary_text.lower() != 'negotiable' else 'Negotiable'
    
    def close(self):
        """Close WebDriver and session"""
        if self.driver:
            self.driver.quit()
        self.session.close()

class BdjobsScraper(WebScraper):
    """Scraper for Bdjobs.com"""
    
    def __init__(self):
        super().__init__()
        self.base_url = "https://jobs.bdjobs.com"
        self.search_url = "https://jobs.bdjobs.com/jobsearch.asp"
    
    def scrape_jobs(self, keywords: List[str] = None, max_pages: int = 5) -> List[JobListing]:
        """Scrape jobs from Bdjobs"""
        jobs = []
        
        try:
            for page in range(1, max_pages + 1):
                logger.info(f"Scraping Bdjobs page {page}")
                
                # Construct search URL
                params = {'fc': '1', 'pg': str(page)}
                if keywords:
                    params['q'] = ' '.join(keywords)
                
                soup = self.get_page_content(self.search_url, use_selenium=True)
                if not soup:
                    continue
                
                job_elements = soup.find_all('div', class_='job-list-item')
                
                for job_element in job_elements:
                    try:
                        job_data = self.extract_job_data(job_element)
                        if job_data:
                            jobs.append(job_data)
                    except Exception as e:
                        logger.error(f"Error extracting job data: {e}")
                        continue
                
                # Random delay between pages
                time.sleep(random.uniform(3, 6))
            
        except Exception as e:
            logger.error(f"Error scraping Bdjobs: {e}")
        
        logger.info(f"Scraped {len(jobs)} jobs from Bdjobs")
        return jobs
    
    def extract_job_data(self, job_element) -> Optional[JobListing]:
        """Extract job data from job element"""
        try:
            # Extract basic information
            title_element = job_element.find('h3') or job_element.find('a', class_='job-title')
            title = self.clean_text(title_element.get_text()) if title_element else "N/A"
            
            company_element = job_element.find('div', class_='company-name') or job_element.find('span', class_='company')
            company = self.clean_text(company_element.get_text()) if company_element else "N/A"
            
            location_element = job_element.find('div', class_='location') or job_element.find('span', class_='location')
            location = self.clean_text(location_element.get_text()) if location_element else "N/A"
            
            # Extract job URL
            url_element = job_element.find('a')
            job_url = urljoin(self.base_url, url_element.get('href')) if url_element else ""
            
            # Create job data dictionary
            job_data = {
                'title': title,
                'company': company,
                'location': location,
                'salary': 'N/A',
                'description': 'N/A',
                'requirements': 'N/A',
                'posted_date': datetime.now().strftime('%Y-%m-%d'),
                'deadline': None,
                'job_type': 'Full-time',
                'experience': 'N/A',
                'url': job_url,
                'source': 'Bdjobs'
            }
            
            # Generate hash ID
            hash_id = self.generate_hash_id(job_data)
            
            return JobListing(
                hash_id=hash_id,
                **job_data
            )
            
        except Exception as e:
            logger.error(f"Error extracting job data: {e}")
            return None

class JobscomScraper(WebScraper):
    """Scraper for Jobs.com.bd"""
    
    def __init__(self):
        super().__init__()
        self.base_url = "https://jobs.com.bd"
        self.search_url = "https://jobs.com.bd/jobs"
    
    def scrape_jobs(self, keywords: List[str] = None, max_pages: int = 5) -> List[JobListing]:
        """Scrape jobs from Jobs.com.bd"""
        jobs = []
        
        try:
            for page in range(1, max_pages + 1):
                logger.info(f"Scraping Jobs.com.bd page {page}")
                
                url = f"{self.search_url}?page={page}"
                soup = self.get_page_content(url)
                if not soup:
                    continue
                
                job_elements = soup.find_all('div', class_='job-item')
                
                for job_element in job_elements:
                    try:
                        job_data = self.extract_job_data(job_element)
                        if job_data:
                            jobs.append(job_data)
                    except Exception as e:
                        logger.error(f"Error extracting job data: {e}")
                        continue
                
                time.sleep(random.uniform(2, 4))
            
        except Exception as e:
            logger.error(f"Error scraping Jobs.com.bd: {e}")
        
        logger.info(f"Scraped {len(jobs)} jobs from Jobs.com.bd")
        return jobs
    
    def extract_job_data(self, job_element) -> Optional[JobListing]:
        """Extract job data from job element"""
        # Similar implementation as BdjobsScraper
        # This is a template - you'll need to adapt based on actual HTML structure
        pass

class EmailNotifier:
    """Handle email notifications"""
    
    def __init__(self):
        self.smtp_server = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
        self.smtp_port = int(os.getenv('SMTP_PORT', '587'))
        self.email = os.getenv('EMAIL_ADDRESS')
        self.password = os.getenv('EMAIL_PASSWORD')
        self.recipients = os.getenv('EMAIL_RECIPIENTS', '').split(',')
    
    def send_daily_report(self, new_jobs: List[JobListing], salary_analysis: Dict):
        """Send daily job report via email"""
        if not self.email or not self.password:
            logger.warning("Email credentials not configured")
            return
        
        try:
            msg = MIMEMultipart()
            msg['From'] = self.email
            msg['To'] = ', '.join(self.recipients)
            msg['Subject'] = f"Daily Job Report - {datetime.now().strftime('%Y-%m-%d')}"
            
            # Create email body
            body = self.create_email_body(new_jobs, salary_analysis)
            msg.attach(MIMEText(body, 'html'))
            
            # Send email
            server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            server.starttls()
            server.login(self.email, self.password)
            server.send_message(msg)
            server.quit()
            
            logger.info(f"Daily report sent to {len(self.recipients)} recipients")
            
        except Exception as e:
            logger.error(f"Error sending email: {e}")
    
    def create_email_body(self, new_jobs: List[JobListing], salary_analysis: Dict) -> str:
        """Create HTML email body"""
        html = f"""
        <html>
        <body>
            <h2>Daily Job Scraping Report</h2>
            <p><strong>Date:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            <p><strong>New Jobs Found:</strong> {len(new_jobs)}</p>
            
            <h3>Recent Job Listings</h3>
            <table border="1" cellpadding="5" cellspacing="0">
                <tr>
                    <th>Title</th>
                    <th>Company</th>
                    <th>Location</th>
                    <th>Source</th>
                </tr>
        """
        
        for job in new_jobs[:10]:  # Show first 10 jobs
            html += f"""
                <tr>
                    <td>{job.title}</td>
                    <td>{job.company}</td>
                    <td>{job.location}</td>
                    <td>{job.source}</td>
                </tr>
            """
        
        html += """
            </table>
            
            <h3>Location Analysis</h3>
            <p>Top locations by job count:</p>
            <ul>
        """
        
        for location_data in salary_analysis.get('location_analysis', [])[:5]:
            html += f"<li>{location_data['location']}: {location_data['job_count']} jobs</li>"
        
        html += """
            </ul>
        </body>
        </html>
        """
        
        return html

class JobPortalScraper:
    """Main scraper orchestrator"""
    
    def __init__(self, db_type='mysql'):
        self.db = DatabaseManager(db_type)
        self.scrapers = {
            'bdjobs': BdjobsScraper(),
            'jobscom': JobscomScraper()
        }
        self.email_notifier = EmailNotifier()
        self.new_jobs_today = []
    
    def scrape_all_portals(self, keywords: List[str] = None, max_pages: int = 3):
        """Scrape all configured job portals"""
        logger.info("Starting job scraping process")
        all_jobs = []
        
        # Use ThreadPoolExecutor for concurrent scraping
        with ThreadPoolExecutor(max_workers=2) as executor:
            future_to_scraper = {
                executor.submit(scraper.scrape_jobs, keywords, max_pages): name
                for name, scraper in self.scrapers.items()
            }
            
            for future in as_completed(future_to_scraper):
                scraper_name = future_to_scraper[future]
                try:
                    jobs = future.result()
                    all_jobs.extend(jobs)
                    logger.info(f"Completed scraping {scraper_name}: {len(jobs)} jobs")
                except Exception as e:
                    logger.error(f"Error scraping {scraper_name}: {e}")
        
        # Store jobs in database
        new_jobs_count = 0
        for job in all_jobs:
            if self.db.insert_job(job):
                self.new_jobs_today.append(job)
                new_jobs_count += 1
        
        logger.info(f"Scraping completed. {new_jobs_count} new jobs added to database")
        return all_jobs
    
    def run_daily_scrape(self):
        """Run daily scraping routine"""
        try:
            # Scrape jobs
            self.scrape_all_portals(
                keywords=['python', 'developer', 'engineer', 'analyst'],
                max_pages=5
            )
            
            # Generate salary analysis
            salary_analysis = self.db.get_salary_analysis()
            
            # Send email report
            if self.new_jobs_today:
                self.email_notifier.send_daily_report(self.new_jobs_today, salary_analysis)
            
            # Reset daily counter
            self.new_jobs_today = []
            
        except Exception as e:
            logger.error(f"Error in daily scrape: {e}")
    
    def schedule_scraping(self):
        """Schedule regular scraping"""
        # Schedule daily scraping at 9 AM
        schedule.every().day.at("09:00").do(self.run_daily_scrape)
        
        logger.info("Scheduled daily scraping at 9:00 AM")
        
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    
    def close(self):
        """Clean up resources"""
        self.db.close()
        for scraper in self.scrapers.values():
            scraper.close()
        logger.info("Job scraper closed successfully")

def main():
    """Main function to run the scraper"""
    try:
        # Initialize scraper
        scraper = JobPortalScraper(db_type='mysql')  # or 'postgresql'
        
        # Run one-time scraping
        scraper.run_daily_scrape()
        
        # Uncomment below to run scheduled scraping
        # scraper.schedule_scraping()
        
    except KeyboardInterrupt:
        logger.info("Scraper stopped by user")
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
    finally:
        if 'scraper' in locals():
            scraper.close()

if __name__ == "__main__":
    main()