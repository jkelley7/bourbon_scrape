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
time_of_run = today.strftime("%m/%d/%y %H:%M")


def main():
    bb_dbase_filename = "./databases/break_bour_db.db"
    #_log.info('we are about to scrape')

    #where the magic happens
    page_link = 'https://breakingbourbon.com/release-list.html'
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
        email(new_prod_to_email, sender='jkelleypga@gmail.com', reciever= 'jkelleypga@gmail.com')

        write_file(new_data = date_updated2)
        
    else:
        print(f'{time_of_run} No Updates')

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

def scrape_page(page_content):
    """ This takes in the page content from beautiful soup, scrapes the page and returns a data frame containing date scrape, month of release, product and sub details of product
    :Params
    -----------
    page_content - beautiful soup scrapped web site
    
    :returns
    ------------
    dfs - a data frame containing date scrapped, month of release, product and sub details of product"""
    month = []
    month_prods = []
    dfs = pd.DataFrame([],columns=['month', 'product','product_desc'])
    for div in page_content.body.find_all_next('div',{'class':'month-div'}):
        month_prods = []
        # get the month name in the review header
        month_val = div.find_next('div',{'class':'reviewheader'}).get_text(strip=True)
        month.append(month_val)
        #find the desktop text as this is where everthing is
        text_holder = div.find_next('div',{'class':'desktoptext'})
        month_prods.extend(text_holder.get_text(separator='<br>', strip=True)
                      #.replace('<br>',',')
                      .strip(' ')
                      .split('<br>'))
        #month_prods = list(chain.from_iterable(month_prods))
        # remove the u200d label
        month_prods = remove_u200d(month_prods)
        # check for zero length strings
        month_prods = check_list_for_zeros(month_prods)
        # remove the term bottle labels
        month_prods = remove_bottle_label(month_prods)
        # split the list and make df
        dfs = pd.concat([dfs,split_list_prod_prod_desc(month_prods,month_val)], ignore_index=True)
        #_log.info(f'A total of {dfs.shape[0]} products were on the page')
    return dfs

def check_and_insert(conn, df, cols_to_check = ['product','month'], cols_to_insert = ['product','product_desc','month']):
    """Check to see if what we found today has already been added. If it hasn't been added then add it
    :Params
    -------------
    conn - a connection to a database
    df - a pandas dataframe of new scrapped products
    cols_to_check - cols to check to see if the product is in the database
    cols_to_insert - cols to insert into the database if the product is not in the database
    
    :returns
    --------------
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
            #_log.info(f'New record {new_add}')
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

def remove_u200d(list_):
    """Remove u200d values in a list"""
    list_ = [i.strip('\u200d') for i in list_]
    return list_

def pop_empties(list_,len_var = 1):
    """
    Pops off un-neccasry spaces
    :Params
    ---------
    list_ - a list to cycle thru
    
    :returns
    ---------
    list_ - your cleaned up list
    """
    for idx, ls in enumerate(list_):
        if len(list_[idx]) <= len_var:
            list_.pop(idx)
    return list_

def check_list_for_zeros(list_):
    """Check list for any string with a length of 0
    returns:
    ----------
    a list"""
    if len(min(list_,key=len)) <= 1 and len(list_) > 0:
        return check_list_for_zeros(pop_empties(list_))
    else:
        return list_

def remove_bottle_label(list_, removal_val = 'bottlelabel'):
    """Removes the words bottle label from the list
    removal value must be one word
    """
    for idx, value in enumerate(list_):
        word = (list_[idx]
            .lower()
            .strip(' ')
            .replace(' ',''))
        if word == removal_val or word == removal_val + 's' :
            list_.pop(idx)
    return list_

def split_list_prod_prod_desc(list_, month):
    """This takes our list and split it into products and product descriptions"""
    sub_prod = []
    for idx, _ in enumerate(list_):
        if list_[idx][0] == '-':
            sub_prod.append(list_[idx])
            list_.pop(idx)
        elif idx > 0:
            #product.append(test_prod[idx])
            sub_prod.append('')
        else:
            pass

    dfs = (pd.DataFrame([[month]*len(list_),list_, sub_prod])
     .T
     .rename(columns = {0:'month',
             1:'product',
             2:'product_desc'})
    )
    return dfs

def find_new_products(df):
    """ Take a data frame of all products and find the new products
    :Params
    -----------
    df - a pandas data frame with month, product and product description
    
    :Returns
    ------------
    new_df - a new pandas dataframe with just new products"""
    _log = logging.getLogger(find_new_products.__name__)
    df['date_posted'] = pd.to_datetime(datetime.date.today(), format='%Y-%m-%d')
    df['date_posted'] = df.date_posted.dt.date
    df = df[['date_posted','month','product','product_desc']]
    
    new_df = df[df['product'].str.contains(r'\[new',case = False, regex=True)]
    new_df['product'] = new_df['product'].str.replace(r'\[NEW]','', case = False)
    _log.info(f'found {new_df.shape[0]} new products')
    return new_df

#run this thang!
main()
logging.basicConfig()