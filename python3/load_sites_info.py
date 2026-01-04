import os
import configparser
import argparse
import app_logger as l
import requests
import base64
import json
import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import psycopg2
import time

def read_config():
    config = configparser.ConfigParser()
    config.read("../config/config.properties")      
    return config

def open_connection(config):
    conn_string = "host=%s dbname=%s user=%s password=%s port=%s" % (
            config.get('root','db_host'),
            config.get('root','db_schema'), 
            config.get('root','db_user'),
            config.get('root','db_password'),
            config.get('root','db_port')
        )
    conn = psycopg2.connect(conn_string)
    conn.set_isolation_level(0)
    return conn

def process_event(url,hdrs,cur):
    r = requests.get(url,verify=False,headers=hdrs)
    if r.status_code == 200:
        all_data = r.json()
        sql = f"insert into events (merchant_id,event_ts,event_type,event_content) select '{all_data['store']}','{all_data['ts']}',"
        sql = sql + f"{all_data['type']},'{all_data['content']}'"  
        cur.execute(sql)
    
def main():
    parser = argparse.ArgumentParser(description='Use: python validator command [arguments].')
    parser.add_argument('--date', help='starting time to find orders')
    
    args = parser.parse_args()
    
    config = read_config()
    
    date_str = args.date
    if date_str!=None and len(date_str)==10:
        date_str = args.date
    else:
        date_str = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        
    login = f"{config.get('root','mon_usr')}:{config.get('root','mon_pwd')}"
    base64_login = base64.b64encode(login.encode('ascii')).decode("utf-8")

    hdrs = {
        'Authorization': f'Basic {base64_login}',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept': 'application/json'  
    }    
    
    scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]
    credentials = ServiceAccountCredentials.from_json_keyfile_name('../config/creds.json',scope)
    client = gspread.authorize(credentials)
    sheet = client.open_by_key('1sk4V9NMUID1E0flG4h2mJMnYoqAxTInO1FgWFZCaSPU')
    ws = sheet.get_worksheet(0)
    data = ws.get_all_records()
    num_rows = len(data)
    cur_row = num_rows + 2

    conn = open_connection(config)
    cur = conn.cursor()
    
    sql = 'select merchant_id from merchants'
    cur.execute(sql)
    sites = cur.fetchall()
        
    for site in sites:
        site = site[0]
        url = f"{config.get('root','mon_site')}/{site}_{date_str}.json"
        l.logger.info("url="+url)
        
        r = requests.get(url,verify=False,headers=hdrs)
        if r.status_code == 200:
            all_data = r.json()

            # --- Update GoogleSheet
            ws.update_cell(cur_row,1,all_data['MerchantName'])
            ws.update_cell(cur_row,2,all_data['Date'])        
            ws.update_cell(cur_row,3,all_data['TotalCardPaymentCount'])
            ws.update_cell(cur_row,4,all_data['TotalCardAmount'])
            ws.update_cell(cur_row,5,all_data['TotalCashAmount'])
            ws.update_cell(cur_row,6,all_data['TotalVoidsCount'])
            ws.update_cell(cur_row,7,all_data['TotalOrderAmount-TotalCardAmount-TotalCashAmount'])
            ws.update_cell(cur_row,8,all_data['TotalSessionAmount'])
            #
            ws.update_cell(cur_row,11,all_data['UsageDiskRootPerc'])
            ws.update_cell(cur_row,12,all_data['UsageDiskHomePerc'])
            
            cur_row += 1
            time.sleep(15)
            # ---
            
            # --- Store to DB        
            sql = f"delete from monitoring_data where monitoring_date='{all_data['Date']}' and merchant_id='{all_data['MerchantName']}'"  
            cur.execute(sql)
    
            first_sql = f"insert into monitoring_data (monitoring_date,merchant_id,param_id,param_value) select '{all_data['Date']}','{all_data['MerchantName']}',"
            sql = first_sql + f"'card_payment_cnt',{all_data['TotalCardPaymentCount']}"  
            cur.execute(sql)
            
            sql = first_sql + f"'card_amount',{all_data['TotalCardAmount']}"  
            cur.execute(sql)
     
            sql = first_sql + f"'cash_amount',{all_data['TotalCashAmount']}"  
            cur.execute(sql)
    
            sql = first_sql + f"'voids_count',{all_data['TotalVoidsCount']}"  
            cur.execute(sql)
    
            sql = first_sql + f"'diff_amount',{all_data['TotalOrderAmount-TotalCardAmount-TotalCashAmount']}"  
            cur.execute(sql)
    
            sql = first_sql + f"'session_amount',{all_data['TotalSessionAmount']}"  
            cur.execute(sql)
            
            #

            sql = first_sql + f"'disc_perc_root',{all_data['UsageDiskRootPerc']}"  
            cur.execute(sql)
            
            sql = first_sql + f"'disc_perc_home',{all_data['UsageDiskHomePerc']}"  
            cur.execute(sql)
            
            # ---        
        else:
            l.logger.error(f'Error: status = {r.status_code}')
            #quit()
            
        today_str = datetime.datetime.today().strftime('%Y-%m-%d')
        process_event(f"{config.get('root','mon_site')}/{site}_BeginUpdateCertificate_{today_str}.json",hdrs,cur)
        process_event(f"{config.get('root','mon_site')}/{site}_HttpdRestartResult_{today_str}.json",hdrs,cur)
        process_event(f"{config.get('root','mon_site')}/{site}_EndUpdateCertificate_{today_str}.json",hdrs,cur)
        
if __name__== "__main__": main()