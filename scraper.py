import requests
from bs4 import BeautifulSoup
import matplotlib.pyplot as plt
import csv
from datetime import datetime
import time
import re
from collections import defaultdict
import pandas as pd
import argparse
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
import sqlite3
from typing import List, Dict, Optional

class JobMarketAnalyzer:
    def __init__(self, db_path: str = "job_analysis.db"):
        self.technologies = {
            "python": 0,
            "javascript": 0,
            "typescript": 0,
            "java": 0,
            "c#": 0, 
            "c++": 0,
            "go": 0,
            "rust": 0,
            "ruby": 0,
            "php": 0,
            "swift": 0,
            "kotlin": 0,
            "scala": 0,
            "dart": 0,
            "react": 0,
            "angular": 0,
            "vue": 0,
            "node": 0,
            "django": 0,
            "flask": 0,
            "aws": 0,
            "azure": 0,
            "gcp": 0,
            "docker": 0,
            "kubernetes": 0,
            "sql": 0,
            "nosql": 0,
            "mongodb": 0,
            "postgresql": 0,
            "mysql": 0
        }
        
        self.locations = defaultdict(int)
        self.salary_ranges = defaultdict(int)
        self.job_titles = defaultdict(int)
        self.job_posts = []
        self.db_path = db_path
        self._init_db()
        
    def _init_db(self) -> None:
        """Initialize database tables"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS technologies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT UNIQUE,
                    count INTEGER DEFAULT 0
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS locations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT UNIQUE,
                    count INTEGER DEFAULT 0
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS salaries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    range TEXT UNIQUE,
                    count INTEGER DEFAULT 0
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS job_titles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT UNIQUE,
                    count INTEGER DEFAULT 0
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS job_posts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source TEXT,
                    content TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()
    
    def _update_db(self) -> None:
        """Update database with current analysis data"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Update technologies
            for tech, count in self.technologies.items():
                cursor.execute("""
                    INSERT OR IGNORE INTO technologies (name, count) VALUES (?, ?)
                    ON CONFLICT(name) DO UPDATE SET count = count + excluded.count
                """, (tech, count))
            
            # Update locations
            for loc, count in self.locations.items():
                cursor.execute("""
                    INSERT OR IGNORE INTO locations (name, count) VALUES (?, ?)
                    ON CONFLICT(name) DO UPDATE SET count = count + excluded.count
                """, (loc, count))
            
            # Update salaries
            for sal, count in self.salary_ranges.items():
                cursor.execute("""
                    INSERT OR IGNORE INTO salaries (range, count) VALUES (?, ?)
                    ON CONFLICT(range) DO UPDATE SET count = count + excluded.count
                """, (sal, count))
            
            # Update job titles
            for title, count in self.job_titles.items():
                cursor.execute("""
                    INSERT OR IGNORE INTO job_titles (title, count) VALUES (?, ?)
                    ON CONFLICT(title) DO UPDATE SET count = count + excluded.count
                """, (title, count))
            
            # Save job posts
            for post in self.job_posts:
                cursor.execute("""
                    INSERT INTO job_posts (source, content) VALUES (?, ?)
                """, ("Hacker News", post))
            
            conn.commit()
    
    def scrape_site(self, url: str) -> None:
        """Scrape job postings from various sites"""
        if "news.ycombinator.com" in url:
            self.scrape_hn_thread(url)
        elif "indeed.com" in url:
            self.scrape_indeed(url)
        elif "linkedin.com" in url:
            self.scrape_linkedin(url)
        elif "stackoverflow.com" in url:
            self.scrape_stackoverflow(url)
        else:
            print(f"Unsupported website: {url}")
    
    def scrape_hn_thread(self, url: str) -> None:
        """Scrape Hacker News 'Who is hiring?' thread"""
        print(f"Scraping Hacker News thread: {url}")
        
        try:
            headers = {'User-Agent': 'Mozilla/5.0'}
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, "html.parser")
            indent_tags = soup.find_all(class_="ind", indent=0)
            
            for indent in indent_tags:
                comment = indent.find_next(class_="comment")
                if comment:
                    post_text = comment.get_text().lower()
                    self.job_posts.append(post_text)
                    self._analyze_post(post_text)
            
            print(f"Found {len(self.job_posts)} job postings from Hacker News")
            time.sleep(1)  # Be polite
            
        except Exception as e:
            print(f"Error scraping Hacker News thread {url}: {e}")
    
    def scrape_indeed(self, url: str) -> None:
        """Scrape job postings from Indeed"""
        print(f"Scraping Indeed: {url}")
        
        try:
            headers = {'User-Agent': 'Mozilla/5.0'}
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, "html.parser")
            job_cards = soup.find_all('div', class_='job_seen_beacon')
            
            for card in job_cards:
                title = card.find('h2', class_='jobTitle').get_text().lower()
                company = card.find('span', class_='companyName').get_text().lower()
                location = card.find('div', class_='companyLocation').get_text().lower()
                snippet = card.find('div', class_='job-snippet').get_text().lower()
                
                post_text = f"{title} at {company} in {location}. {snippet}"
                self.job_posts.append(post_text)
                self._analyze_post(post_text)
            
            print(f"Found {len(job_cards)} job postings from Indeed")
            time.sleep(2)  # Be polite
            
        except Exception as e:
            print(f"Error scraping Indeed {url}: {e}")
    
    def scrape_linkedin(self, url: str) -> None:
        """Scrape job postings from LinkedIn"""
        print(f"Scraping LinkedIn: {url}")
        
        try:
            headers = {'User-Agent': 'Mozilla/5.0'}
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, "html.parser")
            job_cards = soup.find_all('div', class_='base-card')
            
            for card in job_cards:
                title = card.find('h3', class_='base-search-card__title').get_text().strip().lower()
                company = card.find('h4', class_='base-search-card__subtitle').get_text().strip().lower()
                location = card.find('span', class_='job-search-card__location').get_text().strip().lower()
                
                post_text = f"{title} at {company} in {location}"
                self.job_posts.append(post_text)
                self._analyze_post(post_text)
            
            print(f"Found {len(job_cards)} job postings from LinkedIn")
            time.sleep(2)  # Be polite
            
        except Exception as e:
            print(f"Error scraping LinkedIn {url}: {e}")
    
    def scrape_stackoverflow(self, url: str) -> None:
        """Scrape job postings from Stack Overflow"""
        print(f"Scraping Stack Overflow: {url}")
        
        try:
            headers = {'User-Agent': 'Mozilla/5.0'}
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, "html.parser")
            job_listings = soup.find_all('div', class_='-job')
            
            for job in job_listings:
                title = job.find('h2', class_='mb4').get_text().strip().lower()
                company = job.find('h3', class_='fc-black-700').get_text().strip().lower()
                tags = [tag.get_text().lower() for tag in job.find_all('a', class_='post-tag')]
                
                post_text = f"{title} at {company}. Skills: {', '.join(tags)}"
                self.job_posts.append(post_text)
                self._analyze_post(post_text)
            
            print(f"Found {len(job_listings)} job postings from Stack Overflow")
            time.sleep(2)  # Be polite
            
        except Exception as e:
            print(f"Error scraping Stack Overflow {url}: {e}")
    
    def _analyze_post(self, post: str) -> None:
        """Analyze a single job post"""
        # Analyze technology mentions
        words = {
            word.strip(".,/:;!@()[]{}'\"") 
            for word in post.split()
        }
        
        for tech in self.technologies:
            if tech in words:
                self.technologies[tech] += 1
        
        # Extract location
        location_pattern = r"\b(located in|based in|remote|hybrid|onsite|in)\s+([a-zA-Z\s,]+)"
        matches = re.findall(location_pattern, post)
        if matches:
            location = matches[0][1].strip()
            self.locations[location] += 1
        
        # Extract salary
        salary_pattern = r"\$(\d{2,3}k|\d{3},\d{3})"
        salaries = re.findall(salary_pattern, post)
        if salaries:
            for salary in salaries:
                self.salary_ranges[salary] += 1
        
        # Extract job title
        title_pattern = r"\b(looking for|seeking|position|role)\s+([a-zA-Z\s]+)\b"
        title_matches = re.findall(title_pattern, post)
        if title_matches:
            title = title_matches[0][1].strip()
            self.job_titles[title] += 1
    
    def save_to_csv(self, filename: str = "job_analysis.csv") -> bool:
        """Save analysis data to CSV file"""
        try:
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                
                # Write technologies
                writer.writerow(["Category", "Item", "Count"])
                for tech, count in sorted(self.technologies.items(), key=lambda x: x[1], reverse=True):
                    if count > 0:
                        writer.writerow(["Technology", tech, count])
                
                # Write locations
                for loc, count in sorted(self.locations.items(), key=lambda x: x[1], reverse=True):
                    writer.writerow(["Location", loc, count])
                
                # Write salaries
                for sal, count in sorted(self.salary_ranges.items(), key=lambda x: x[1], reverse=True):
                    writer.writerow(["Salary", f"${sal}", count])
                
                # Write job titles
                for title, count in sorted(self.job_titles.items(), key=lambda x: x[1], reverse=True):
                    writer.writerow(["Job Title", title, count])
            
            print(f"Data saved to {filename}")
            return True
        
        except Exception as e:
            print(f"Error saving CSV: {e}")
            return False
    
    def visualize_data(self, filename: str = "job_analysis.png") -> None:
        """Generate visualization of the analysis data"""
        plt.figure(figsize=(16, 20))
        
        # Technology Plot
        plt.subplot(2, 2, 1)
        filtered_tech = {k: v for k, v in self.technologies.items() if v > 0}
        sorted_tech = dict(sorted(filtered_tech.items(), key=lambda item: item[1], reverse=True))
        bars = plt.bar(sorted_tech.keys(), sorted_tech.values())
        for bar in bars:
            height = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2., height,
                    f'{height}', ha='center', va='bottom')
        plt.title("Technology Mentions")
        plt.xticks(rotation=45, ha='right')
        
        # Location Plot
        plt.subplot(2, 2, 2)
        if self.locations:
            sorted_loc = dict(sorted(self.locations.items(), key=lambda item: item[1], reverse=True)[:10])
            plt.pie(sorted_loc.values(), labels=sorted_loc.keys(), autopct='%1.1f%%')
            plt.title("Top Locations")
        
        # Salary Plot
        plt.subplot(2, 2, 3)
        if self.salary_ranges:
            sorted_sal = dict(sorted(self.salary_ranges.items(), key=lambda item: item[1], reverse=True)[:5])
            plt.bar([f"${x}" for x in sorted_sal.keys()], sorted_sal.values())
            plt.title("Salary Ranges Mentioned")
        
        # Job Titles Plot
        plt.subplot(2, 2, 4)
        if self.job_titles:
            sorted_titles = dict(sorted(self.job_titles.items(), key=lambda item: item[1], reverse=True)[:5])
            plt.barh(list(sorted_titles.keys()), list(sorted_titles.values()))
            plt.title("Top Job Titles")
        
        plt.suptitle(f"Job Market Analysis - {datetime.now().strftime('%Y-%m-%d')}")
        plt.tight_layout()
        plt.savefig(filename, dpi=300)
        plt.close()
        print(f"Visualization saved to {filename}")
    
    def generate_report(self) -> str:
        """Generate a text report of the analysis"""
        report = []
        report.append("\n=== Job Market Analysis Report ===")
        report.append(f"Total Job Posts Analyzed: {len(self.job_posts)}")
        
        report.append("\nTop Technologies:")
        for tech, count in sorted(self.technologies.items(), key=lambda x: x[1], reverse=True)[:5]:
            if count > 0:
                report.append(f"- {tech.title()}: {count} mentions")
        
        if self.locations:
            report.append("\nTop Locations:")
            for loc, count in sorted(self.locations.items(), key=lambda x: x[1], reverse=True)[:3]:
                report.append(f"- {loc.title()}: {count} mentions")
        
        if self.salary_ranges:
            report.append("\nSalary Ranges Mentioned:")
            for sal, count in sorted(self.salary_ranges.items(), key=lambda x: x[1], reverse=True)[:3]:
                report.append(f"- ${sal}: {count} mentions")
        
        return "\n".join(report)
    
    def send_email_notification(
        self,
        recipient: str,
        sender: str,
        password: str,
        subject: str = "Job Market Analysis Report",
        smtp_server: str = "smtp.gmail.com",
        smtp_port: int = 587
    ) -> bool:
        """Send analysis report via email"""
        try:
            # Generate report content
            text_report = self.generate_report()
            html_report = f"<pre>{text_report}</pre>"
            
            # Create message
            msg = MIMEMultipart()
            msg['From'] = sender
            msg['To'] = recipient
            msg['Subject'] = subject
            
            # Attach text and HTML versions
            msg.attach(MIMEText(text_report, 'plain'))
            msg.attach(MIMEText(html_report, 'html'))
            
            # Attach visualization
            image_path = "job_analysis.png"
            self.visualize_data(image_path)
            with open(image_path, 'rb') as f:
                img = MIMEImage(f.read())
                img.add_header('Content-Disposition', 'attachment', filename=image_path)
                msg.attach(img)
            
            # Attach CSV
            csv_path = "job_analysis.csv"
            self.save_to_csv(csv_path)
            with open(csv_path, 'rb') as f:
                csv_attachment = MIMEText(f.read().decode('utf-8'), 'csv')
                csv_attachment.add_header('Content-Disposition', 'attachment', filename=csv_path)
                msg.attach(csv_attachment)
            
            # Send email
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()
                server.login(sender, password)
                server.send_message(msg)
            
            print(f"Email sent successfully to {recipient}")
            return True
        
        except Exception as e:
            print(f"Error sending email: {e}")
            return False
    
    def load_from_db(self) -> None:
        """Load analysis data from database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Load technologies
                cursor.execute("SELECT name, count FROM technologies")
                for name, count in cursor.fetchall():
                    if name in self.technologies:
                        self.technologies[name] = count
                
                # Load locations
                cursor.execute("SELECT name, count FROM locations")
                for name, count in cursor.fetchall():
                    self.locations[name] = count
                
                # Load salaries
                cursor.execute("SELECT range, count FROM salaries")
                for range_, count in cursor.fetchall():
                    self.salary_ranges[range_] = count
                
                # Load job titles
                cursor.execute("SELECT title, count FROM job_titles")
                for title, count in cursor.fetchall():
                    self.job_titles[title] = count
                
                # Load job posts
                cursor.execute("SELECT content FROM job_posts")
                self.job_posts = [row[0] for row in cursor.fetchall()]
                
            print("Data loaded from database successfully")
        
        except Exception as e:
            print(f"Error loading from database: {e}")

def main():
    # Set up CLI interface
    parser = argparse.ArgumentParser(description="Job Market Analyzer")
    parser.add_argument("urls", nargs="*", help="URLs to scrape for job postings")
    parser.add_argument("--email", help="Send report to this email address")
    parser.add_argument("--sender", help="Email sender address")
    parser.add_argument("--password", help="Email sender password")
    parser.add_argument("--load-db", action="store_true", help="Load data from database")
    parser.add_argument("--save-db", action="store_true", help="Save data to database")
    args = parser.parse_args()
    
    analyzer = JobMarketAnalyzer()
    
    # Load existing data if requested
    if args.load_db:
        analyzer.load_from_db()
    
    # Scrape provided URLs
    if args.urls:
        for url in args.urls:
            analyzer.scrape_site(url)
    
    # Save to database if requested
    if args.save_db:
        analyzer._update_db()
    
    # Generate and display report
    report = analyzer.generate_report()
    print(report)
    
    # Save outputs
    analyzer.save_to_csv()
    analyzer.visualize_data()
    
    # Send email if requested
    if args.email and args.sender and args.password:
        analyzer.send_email_notification(
            recipient=args.email,
            sender=args.sender,
            password=args.password
        )

if __name__ == "__main__":
    main()