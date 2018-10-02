# -*- coding: utf-8 -*-
"""
Created on Tue Oct  2 06:34:31 2018

@author: Roel
"""

import time
import bs4 as bs # pip install beautifulsoup4
import requests
import pandas as pd # pip install pandas
import datetime
from urllib import parse
import uuid
import csv
import random
import os

# General functions
############################

# Test if a scraped value has successfully been scraped.

def test_value(v):
    if v is None:
        v = 'error'
    else:
        v = v.text.strip().replace('"', '')
    return v

def test_attribute(v, attr):
    if v is None:
        v = 'error'
    else:
        v = v[attr]
    return v

# Read multiple CSV file
def read_from_files_in_folder(pattern):
    folder = os.getcwd()
    files = os.listdir(folder + '\\' + pattern)
    files = [file for file in files if pattern in file]
    for i, file in enumerate(files):
        if i <= 1:
            job_list = pd.read_csv(folder + '\\' + pattern + '\\' + file, encoding = 'utf-8', sep=',', error_bad_lines=False, quotechar='"', quoting=csv.QUOTE_ALL)
        else:
            job_list = job_list.append(pd.read_csv(folder + '\\' + pattern + '\\' + file, encoding = 'utf-8',  sep=',', error_bad_lines=False, quotechar='"', quoting=csv.QUOTE_ALL))
        print(str(i) + " - Reading file " + file + ".")
    return job_list

def scrape_job(url):
    url = 'https://be.indeed.com' + url
    page = requests.get(url, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36'})
    parsed_page = bs.BeautifulSoup(page.text, 'html.parser')
    
    dom_title = test_value(parsed_page.select_one('.jobsearch-JobInfoHeader-title'))
    dom_employer = test_value(parsed_page.select_one('.jobsearch-InlineCompanyRating'))
    dom_metadata = test_value(parsed_page.select_one('.jobsearch-JobMetadataHeader-item'))
    dom_description = test_value(parsed_page.select_one('.jobsearch-JobComponent-description'))
    dom_footerdata = test_value(parsed_page.select_one('.jobsearch-JobMetadataFooter'))
    
    table_job = pd.DataFrame({'scrape_date': [str(time.strftime("%c"))],
                                              'title': [dom_title],
                                              'employer' : [dom_employer],
                                              'meta_header': [dom_metadata],
                                              'meta_footer': [dom_footerdata],
                                              'description': [dom_description]
                                              })
    return table_job

def scrape_jobs(links, start):
    for i, link in enumerate(links[start:]):
        job = scrape_job(link)
        print('Scraped ' + str(start + i) + ' of ' + str(len(links)) + ' jobs.')
        job.to_csv('jobs\\jobs_' + str(start + i) + '.csv',index=False, encoding='utf-8', quoting=csv.QUOTE_ALL, quotechar='"')
        time.sleep(random.uniform(0.3, 1.1))
    
def scrape_overview_page(url):
    page = requests.get(url, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36'})
    parsedPage = bs.BeautifulSoup(page.text, 'html.parser')
    elems = parsedPage.find_all(attrs={'data-tn-element':'jobTitle'})
    links = [link['href'] for link in elems]
    return links
    
def scrape_overview_pages(keyword, location, startpage, endpage):
    i = startpage
    while i * 10 <= endpage * 10:
        link = 'https://be.indeed.com/jobs?q=' + keyword+ '&l=' + location + '&start=' + str(i * 10)
        dom_links = scrape_overview_page(link)
        time.sleep(random.uniform(0.5, 1.2))
        print('Scraped page ' + str(i) + ' of ' + str(endpage) + ' - ' + link)
        i += 1
        df_links = pd.DataFrame({'url': dom_links})
        df_links.to_csv('links\\links_' + str(i) + '.csv',index=False, encoding='utf-8', quoting=csv.QUOTE_ALL, quotechar='"')
    links = read_from_files_in_folder('links')
    return links

# Scrape links
# links = scrape_overview_pages('data','Belgium',0,100)
# links.to_csv('links.csv', index=False, encoding='utf-8', quoting=csv.QUOTE_ALL, quotechar='"')
# Scrape jobs
links = pd.read_csv('links.csv', encoding = 'utf-8', sep=',', error_bad_lines=False, quotechar='"', quoting=csv.QUOTE_ALL)
links = links['url'].tolist()

# scrape_jobs(links,0)
jobs = read_from_files_in_folder('jobs')
jobs.to_csv('jobs.csv',index=False, encoding='utf-8', quoting=csv.QUOTE_ALL, quotechar='"')