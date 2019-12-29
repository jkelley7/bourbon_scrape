import pandas as pd
import numpy as np
import re
from bs4 import BeautifulSoup
import urllib
import requests
from datetime import datetime
import datetime
import sqlite3
from sqlite3 import Error
from sqlite_funcs import *
from itertools import chain
import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

import os
# Import smtplib for the actual sending function
import smtplib

# Here are the email package modules we'll need
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

#set the column width wide
pd.set_option('display.max_colwidth',200)
today = datetime.datetime.today()
today_ = today.strftime('%Y_%m_%d')
time_of_run = today.strftime("%m/%d/%y %H:%M")

parent = Path().resolve()
log_file = parent / 'logs' / f'{today_}_bb_scrape.log'
logfile = parent / 'logs' /'bb_scrape.log'

def main():
    bb_dbase_filename = "./databases/break_bour_db.db"
    #_log.info('we are about to scrape')

    #where the magic happens
    page_link = 'https://www.breakingbourbon.com/release-calendar'
    page_response = requests.get(page_link,verify=False, timeout=5)
    page_content = BeautifulSoup(page_response.content, "html.parser")
    date_updated = page_content.find('div',{'class':'desktoptext center'}).getText()
    date_updated2 = re.findall(r'\d{2}/\d{2}/\d{2}',date_updated)[0]
    datetime_format = datetime.datetime.strptime(date_updated2, '%m/%d/%y')
    
    prev_refresh = read_file()
    if date_updated2 != prev_refresh:
        # scrape the page and find all the products
        prods_df = scrape_page(page_content)
        
        # create the connection
        conn = create_connection(bb_dbase_filename)
        
        # show me the new ones
        new_df = find_new_products(prods_df)

        # Check the database to see if they're in there, if not add them
        check_and_insert(conn,new_df)

        # find the latest updates
        new_prod_to_email = sql_latest_updates(conn, today_query())
        
        # because sometimes we have dups
        new_prod_to_email = new_prod_to_email.drop_duplicates(keep='first')

        # email the sucker
        email(new_prod_to_email, sender= os.environ['SENDER'], reciever = os.environ['RECIEVER'] )

        write_file(new_data = date_updated2)
        
    else:
        _log = logging.getLogger(main.__name__)
        _log.info('No updates during this run')

    return date_updated2


def write_file(file_name ='last_update.txt', loc = '../webscraping/',new_data = '1/1/2019'):
    file = open(loc+file_name,'w')
    file.write(new_data)
    file.close()

def read_file(file_name ='last_update.txt', loc = '../webscraping/'):
    new_file = open(loc+file_name,'r')
    last_updated = new_file.readline()

    return last_updated

def email(df, sender = os.environ['SENDER'], reciever = os.environ['RECIEVER']):
    # Create the container (outer) email message.
    _log = logging.getLogger(email.__name__)
    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.ehlo()
        server.starttls()
        server.login(os.environ['GMAIL_UID'], os.environ['GMAIL_SID'])
        msg = MIMEMultipart('mixed')
        msg['Subject'] = 'New Breaking Bourbon Release Updates'
        msg['From'] = sender
        msg['To'] = reciever
        msg.preamble = 'New Breaking Bourbon Release Updates'
        html_message = """\
        <html>
            <body>
            There are new updates to the Breaking Bourbon release calendar! 
            <br><br>
            Check out the <a href="http://breakingbourbon.com/release-list.html">release calendar</a>
            <br><br>
            </body>
            </html>
            """
        html_part = MIMEText(html_message, 'html')
        pd_table = MIMEText(df.to_html(index=False),'html')
        msg.attach(html_part)
        msg.attach(pd_table)
        msg.as_string()
        server.send_message(msg)
        _log.info(f'email sent')
    except Exception as e:
        _log.warning(f'Email was not sent due toe {e}')

def scrape_page(page_content):
    """ This takes in the page content from beautiful soup, scrapes the page and returns a data frame containing date scrape, month of release, product and sub details of product
    :Params
    -----------
    page_content - beautiful soup scrapped web site
    
    :returns
    ------------
    dfs - a data frame containing date scrapped, month of release, product and sub details of product"""
    _log = logging.getLogger(scrape_page.__name__)
    
    invis = re.compile(r'(invisible)')
    name = []
    sub_desc = []
    new_ex = []
    month = []
    
    for container in page_content.body.find_all('div', {'class': "workspace w-container"}):
        try:
            cur_working_month = container.find('div', class_='month-div').getText().lower()
        except:
            pass
        for child in container.find_all('div',{'class':'w-dyn-item'}):
            #Name
            name.append(child.find('div', {'class':"name-div"}).getText())
            
            #sub description
            try:
                sub_desc.append(child.find('div', {'class':"calendar-text w-richtext"}).getText())
            except:
                sub_desc.append('')
                
            #New or expected date
            hidden_count = []
            for findhidden in child.find('div', {'class':"name-tag-div"}).children:
                is_invis = []
                is_invis.extend([True for i in findhidden['class'] if bool(invis.search(i, re.I))])
                if sum(is_invis) == 0:
                    new_ex.append(findhidden.getText())
                else:
                    hidden_count.append(True)
            if sum(hidden_count)>1:
                new_ex.append('')
            month.append(cur_working_month)
    
    df = pd.DataFrame({'month': month,
              'product': name,
              'product_desc': sub_desc,
              'new':new_ex})
    
    _log.info(f'A total of {df.shape[0]} products were on the page')
    return df

def check_and_insert(conn, df, cols_to_check = ['product','month'], cols_to_insert = ['product','product_desc','month']):
    """Check to see if what we found today has already been added. If it hasn't been added then add it
    
    -----------
    Params
    -----------
    conn - a connection to a database
    df - a pandas dataframe of new scrapped products
    cols_to_check - cols to check to see if the product is in the database
    cols_to_insert - cols to insert into the database if the product is not in the database
    
    -----------
    Returns
    ------------
    Nothing data is added to the database
    """
    _log = logging.getLogger(check_and_insert.__name__)
    curs = conn.cursor()
    
    recs_to_insert = []
    df_records = df.to_records(index=False)
    
    check_b4_insert = df_records[cols_to_check]
    insert_recs = df_records[cols_to_insert]

    for rec in check_b4_insert:
        print(rec)
        curs.execute("select * from new_whisky where 1=1 and product = ? and month = ?",rec)
        double_check = curs.fetchone()
        recs_to_insert.append(double_check == None)
    if any(recs_to_insert):
        new_additions = insert_recs[recs_to_insert]
        _log.info(f'inserted {len(new_additions)} new records')
        for new_add in new_additions:
            _log.info(f'New record {new_add}')
        for new_add in new_additions:
            curs.execute("INSERT INTO new_whisky (product, product_desc, month) VALUES(?,?,?)",new_add)
            conn.commit()
            
    else:
        print('nothing to insert')


def sql_latest_updates(conn,sql):
    """Returns the lastest data"""

    data = pd.read_sql(sql,conn)
    
    return data

def today_query():
    """Returns anything that was inserted into the table today"""
    
    qry_today = """
                SELECT
                date_posted,
                month AS Month_release,
                product AS product_name,
                product_desc AS product_details

                FROM new_whisky
                WHERE
                date(date_posted) > date('now','-1 day')"""
    return qry_today

def find_new_products(df):
    """ Take a data frame of all products and find the new products
    
    -----------
    Params
    -----------
    df - a pandas data frame with month, product and product description
    
    -----------
    Returns
    ------------
    new_df - a new pandas dataframe with just new products"""
    _log = logging.getLogger(find_new_products.__name__)

    df['date_posted'] = pd.to_datetime(datetime.date.today(), format='%Y-%m-%d')
    df['date_posted'] = df.date_posted.dt.date
    
    new_df = df[df['new'] != '']
    _log.info(f'found {new_df.shape[0]} new products')

    return new_df

if __name__ == '__main__':
    #run this thang!
    logger_fmt ='%(asctime)s - %(name)s - %(levelname)s - %(lineno)d - %(message)s'
    logging.basicConfig(level=logging.INFO,format=logger_fmt, handlers=[TimedRotatingFileHandler(logfile,when='d', interval = 30)])#logging.FileHandler(log_file)
    main()

