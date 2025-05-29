# job_portal_scraping
Job Portal Scraper project that follows industry best practices and includes features like Real-time data extraction, duplicate filtering, salary analysis, automated email alerts, etc. This is an automated job listing aggregator from multiple Bangladeshi job portals.

# Installation & Setup Guide
Step 1: Install Python Dependencies
bashpip install -r requirements.txt
Step 2: Install ChromeDriver
`Option 1: Download manually from https://chromedriver.chromium.org/`
# Option 2: Use webdriver-manager (already included in requirements)

# For Ubuntu/Debian:
sudo apt-get update
sudo apt-get install -y google-chrome-stable

# For Windows: Download Chrome from official website
Step 3: Database Setup
For MySQL:

Install MySQL
```bash
sudo apt-get install mysql-server  # Ubuntu
# or download from https://dev.mysql.com/downloads/
```

# Create database
```bash
mysql -u root -p
CREATE DATABASE job_portal;
For PostgreSQL:
bash# Install PostgreSQL
sudo apt-get install postgresql postgresql-contrib  # Ubuntu
```

# Create database
```
sudo -u postgres psql
CREATE DATABASE job_portal;
```

# Step 4: Configure Environment

Copy .env.example to .env
Update database credentials
Configure email settings (use App Password for Gmail)

# Step 5: Run the Scraper
 One-time scraping
```bash
python main.py
```

# Or import and use programmatically
from main import JobPortalScraper
scraper = JobPortalScraper()
scraper.run_daily_scrape()


#  **Key Technical Achievements**

## **1. Advanced Web Scraping Architecture**
* **Multi-portal support**: Handles different site structures (Bdjobs, Jobs.com.bd)
* **Dual scraping methods**: BeautifulSoup for static content, Selenium for JavaScript-heavy sites
* **Intelligent error handling**: Retry logic, timeout management, graceful degradation
* **Rate limiting**: Respectful scraping with random delays and proper headers

## **2. Production-Grade Database Design**
* **Dual database support**: MySQL and PostgreSQL compatibility
* **Smart duplicate detection**: MD5 hash-based uniqueness checking
* **Query optimization**: Proper indexing for fast searches
* **Connection pooling**: Efficient resource management

## **3. Advanced Features**
* **Concurrent processing**: ThreadPoolExecutor for parallel scraping
* **Real-time analytics**: Salary analysis and trend tracking
* **Automated notifications**: HTML email reports with job summaries
* **Scheduling system**: Automated daily/hourly runs
* **Comprehensive logging**: Production-ready monitoring