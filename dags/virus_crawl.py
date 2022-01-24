import os
import sys
import csv
import random
import traceback
from time import sleep
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options

class CrawlJobs:
    # Initialize attributes
    def __init__(self, link):
        self.browser = None
        self.header = False
        self.HOME = link
        self.init_driver()
        self.browser.get(self.HOME)
    
    # Driver options
    def options_driver(self):
        CHROMEDRIVER_PATH = './chromedriver'
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        s=Service(CHROMEDRIVER_PATH)
        driver = webdriver.Chrome(
            service=s, options=chrome_options)
        return driver
    
    # Initialize driver object
    def init_driver(self):
        self.browser = self.options_driver()
    
    # Write result to csv file
    def write_csv(self, filename, content=None, header=None):
        with open(filename, 'a') as csvfile: 
            csvwriter = csv.writer(csvfile, delimiter='\t') 
            if self.header == False:
                csvwriter.writerow(header)
                self.header = True
            csvwriter.writerows(content)
    
    # Get header attributes
    def get_header(self, selector, table):
        header = table.find_element(By.TAG_NAME, selector)
        ths = header.find_elements(By.TAG_NAME, 'th')
        return [' '.join(th.text.split('\n')) for th in ths if th.text != '']
    
    # Get country cases information
    def get_country_info(self, selector):
        tds = selector.find_elements(By.TAG_NAME, 'td')
        return [td.text for td in tds]
    
    # Get all countries cases information
    def get_countries_info(self, selector, header, file_name):
        trs = selector.find_elements(By.TAG_NAME, 'tr')
        res = []
        for tr in trs:
            res.append(self.get_country_info(tr))
            if len(res) > 40:
                res = self.remove_empty_rows(res)
                self.write_csv(file_name, res, header)
                res.clear()
        if len(res) > 0:
            res = self.remove_empty_rows(res)
            self.write_csv(file_name, res, header)
    
    # Remove empty rows - Cleaning
    def remove_empty_rows(self, content):
        return [c for c in content if any(c) == True]
    
    # Get information of specified selector
    def get_info(self, selector):
        tab_panel = self.browser.find_element(By.ID, selector)
        table = tab_panel.find_element(By.TAG_NAME, 'table')
        # Get header attribute
        header = self.get_header('thead', table)
        # Body content
        bodys = table.find_elements(By.TAG_NAME, 'tbody')
        for body in bodys:
        # Specific world information
            if 'today' in selector:
                date = datetime.today().strftime('%d-%m-%Y')
            elif 'yesterday2' in selector:
                date = datetime.today() - timedelta(days=2)
                date = date.strftime('%d-%m-%Y')
            else:
                date = datetime.today() - timedelta(days=1)
                date = date.strftime('%d-%m-%Y')
            self.get_countries_info(body, header, f"data/{date}.csv")
        self.header = False
        
    # Main crawling
    def crawl(self):
        print('--------------------- Running Crawling Job')
        sleep(1)
        tabs = {
            'https://www.worldometers.info/coronavirus/#main_table': ['nav-today']
        }
        for tab in tabs.keys():
            self.browser.get(tab)
            sleep(1)
            selectors = tabs[tab]
            for selector in selectors:
                a = self.browser.find_element(By.XPATH, f"//a[contains(@href, '#{selector}')]")
                self.browser.execute_script("arguments[0].click();", a)
                sleep(1)
                self.get_info(selector)
        print('--------------------- Done Crawling Job')
        self.browser.close()


def run_etl_crawl():
    crawl_jobs = CrawlJobs('https://www.worldometers.info/coronavirus/')
    crawl_jobs.crawl()