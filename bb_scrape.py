from bs4 import BeautifulSoup
import urllib
import requests
from datetime import datetime
import re

import os
# Import smtplib for the actual sending function
import smtplib

# Here are the email package modules we'll need
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def main():
    page_link = 'https://breakingbourbon.com/release-list.html'
    page_response = requests.get(page_link,verify=False, timeout=5)
    page_content = BeautifulSoup(page_response.content, "html.parser")
    date_updated = page_content.find('span',{'id':'u130963'}).getText().strip(' ')
    date_updated2 = re.findall(r'\d{2}/\d{2}/\d{2}',date_updated)[0]
    datetime_format = datetime.strptime(date_updated2, '%m/%d/%y')

    prev_refresh = read_file()
    if date_updated2 != prev_refresh:

        email()

        write_file(new_data = date_updated2)
        
    else:
        print('No Updates')

    return date_updated2

def write_file(file_name ='last_update.txt', loc = 'C:/Users/JOSH/AnacondaProjects/webscraping/',new_data = '1/1/2019'):
    file = open(loc+file_name,'w')
    file.write(new_data)
    file.close()

def read_file(file_name ='last_update.txt', loc = 'C:/Users/JOSH/AnacondaProjects/webscraping/'):
    new_file = open(loc+file_name,'r')
    last_updated = new_file.readline()
    new_file.close()

    return last_updated

def email(sender = 'jkelleypga@gmail.com', reciever = 'jkelleypga@gmail.com; texasstevens@icloud.com'):
    # Create the container (outer) email message.
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(os.environ['GMAIL_UID'], os.environ['GMAIL_SID'])
    msg = MIMEMultipart('text')
    msg['Subject'] = 'New Bourbon Release Updates'
    msg['From'] = sender
    msg['To'] = reciever
    msg.preamble = 'New Bourbon Release Updates'
    text = "There are new to updates to Breaking Bourbons release calendar. Check out the "
    textpart = MIMEText(text, 'plain')
    text2 = MIMEText('<a href="http://breakingbourbon.com/release-list.html">release calendar</a>','html')
    msg.attach(textpart)
    msg.attach(text2)
    msg.as_string()
    server.send_message(msg)

    
main()