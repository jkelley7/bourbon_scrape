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

import os
# Import smtplib for the actual sending function
import smtplib

# Here are the email package modules we'll need
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def main():
    bb_dbase_filename = "./databases/break_bour_db.db"

    #where the magic happens
    page_link = 'https://breakingbourbon.com/release-list.html'
    page_response = requests.get(page_link,verify=False, timeout=5)
    page_content = BeautifulSoup(page_response.content, "html.parser")
    date_updated = page_content.find('span',{'id':'u130963'}).getText().strip(' ')
    date_updated2 = re.findall(r'\d{2}/\d{2}/\d{2}',date_updated)[0]
    datetime_format = datetime.datetime.strptime(date_updated2, '%m/%d/%y')
    
    prev_refresh = read_file()
    if date_updated2 != prev_refresh:
        #find all the tags that say new
        returned_list = find_new_update_info(page_content)
        
        #create the connection
        conn = create_connection(bb_dbase_filename)
        
        #check if all things labeled new are in the database if not insert them
        check_and_insert(conn, returned_list)
        
        #find the latest updates
        df = sql_latest_updates(conn, today_query())
        
        #email the df to people who want to know
        email(df)

        write_file(new_data = date_updated2)
        
    else:
        print('No Updates')

    return date_updated2


def write_file(file_name ='last_update.txt', loc = './webscraping/',new_data = '1/1/2019'):
    file = open(loc+file_name,'w')
    file.write(new_data)
    file.close()

def read_file(file_name ='last_update.txt', loc = './webscraping/'):
    new_file = open(loc+file_name,'r')
    last_updated = new_file.readline()
    new_file.close()

    return last_updated

def email(df, sender, reciever):
    # Create the container (outer) email message.
    server = smtplib.SMTP('smtp.gmail.com', 587)
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
    #original code
    #text = "There are new to updates to Breaking Bourbons release calendar. Check out the "
    #textpart = MIMEText(text, 'plain')
    #text2 = MIMEText('<a href="http://breakingbourbon.com/release-list.html">release calendar</a>','html')
    #msg.attach(textpart)
    #msg.attach(text2)
    #msg.as_string()
    server.send_message(msg)

def find_new_update_info(page_content):
    """Takes the page content aka a beautiful soups page response content
    
    :param page_content: a beautiful soup object of a parse html page"""
    
    product_list = []
    sub_cat = []
    cat_id = []
    bb_prod_id = []
    date_run = []
    sample = page_content.findAll('p')
    for i in sample:
        find_this = re.compile(r'\[new', flags= re.IGNORECASE)
        in_this_string = i.getText()
        if find_this.search(in_this_string) is not None:
            str_len = len(in_this_string)
            found = find_this.search(in_this_string)
            product_list.append(in_this_string[:found.span()[0]-1])
            sub_cat.append(i.findNextSibling().text[1:])
            cat_id.append(i.attrs['id'])
            bb_prod_id.append(i.findChild().attrs['id'])
            date_run.append(datetime.date.today())
    
    return list(zip(date_run,bb_prod_id,product_list, sub_cat))

def check_and_insert(conn, list_to_insert):
    """This function checks if what was scrapped is already inserted
    :param conn: a connection to a database
    :param list_to_insert: a zipped list you would like to insert"""
    curs = conn.cursor()
    
    for i in list_to_insert:
        curs.execute("select * from new_whisky where 1=1 and product_desc = ? and product_sub_desc = ?",i[2:])
        double_check = curs.fetchone()
        if double_check is None:
            curs.execute("INSERT INTO new_whisky (date_posted, bb_prod_id, product_desc, product_sub_desc) VALUES(?,?,?,?)",i)
        else:
            print('Nothing to insert')
    conn.commit()

def sql_latest_updates(conn,sql):
    """Returns the lastest data and converts it into pandas dataframe
    
    :params conn: a sqlite3 database connection
    :param sql: the sql string to query from the database"""
    data = pd.read_sql(sql,conn)
    return data

def today_query():
    """Just query today and yesterday"""
    qry_today = """
                SELECT
                date_posted,
                product_desc AS product_name,
                product_sub_desc AS product_details

                FROM new_whisky
                WHERE
                date(date_posted) >= date('now','-1 day')"""
    return qry_today

#run this thang!
main()