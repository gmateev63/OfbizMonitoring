import os
import psycopg2
import configparser
import argparse
import data_tools
import app_logger as l
import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def read_config(args):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_file = args.config if args.config else os.path.join( script_dir, "../config/config.properties")
    #_LOGGER.debug("Config file ""%s""." % (config_file))
    l.logger.debug(f"Config file - {config_file}")
    config = configparser.ConfigParser()
    config.read(config_file)
    return config

def open_connection(config):
    conn_string = "host=%s dbname=%s user=%s password=%s port=%s" % (
            config.get('root','all.db_host'),
            config.get('root','all.db_schema'), 
            config.get('root','all.db_user'),
            config.get('root','all.db_password'),
            config.get('root','all.db_port')
        )

    #_LOGGER.debug("Connect to dataase ""%s""." % (config.get('root','all.db_schema')))
    l.logger.debug(f"Connect to database '{config.get('root','all.db_schema')}'")

    conn = psycopg2.connect(conn_string)
    conn.set_isolation_level(0) 
    return conn

def fmt_date(dtime):
    return dtime.strftime('%Y-%m-%d')

def fmt_time(dtime):
    return dtime.strftime('%H:%m:%S')

def send_mail(config,subject,body):
    smtp_server = config.get('root','all.smtp_server')
    smtp_port = config.get('root','all.smtp_port')
    from_email=config.get('root','all.from_email')
    password=config.get('root','all.password')
    to_email=config.get('root','all.to_email')
    
    message = MIMEMultipart()
    message['From'] = from_email
    message['To'] = to_email
    message['Subject'] = subject
    message.attach(MIMEText(body, 'plain'))
    
    with smtplib.SMTP(smtp_server, smtp_port) as server:
        #server.set_debuglevel(1)
        server.starttls()
        server.login(from_email, password)
        server.sendmail(from_email, to_email, message.as_string())
    
    l.logger.info('Email sent successfully.')

def export(config,conn,args):
    cur = conn.cursor()
    dbTimezone = config.get('root','all.db_timezone')
    cur.execute(f"SET TIME ZONE '{dbTimezone}'")

    date_str = args.date
    if date_str!=None and len(date_str)==10:
        date_str = args.date
    else:
        date_str = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')

    sql = f"""select 
    (select coalesce(sum(grand_total),0) from order_header 
     where order_date>=s.ddate and order_date<s.ddate+1) order_sum,
    (select count(1) from payment 
     where payment_method_type_id='EFT_ACCOUNT' 
     and effective_date>=s.ddate and effective_date<s.ddate+1) etf_count,
    (select coalesce(sum(amount),0) from payment 
     where payment_method_type_id='EFT_ACCOUNT' 
     and effective_date>=s.ddate and effective_date<s.ddate+1) etf_sum,
    (select count(1) from payment 
     where payment_method_type_id='CASH' 
     and effective_date>=s.ddate and effective_date<s.ddate+1) cash_count,
    (select coalesce(sum(amount),0) from payment 
     where payment_method_type_id='CASH' 
     and effective_date>=s.ddate and effective_date<s.ddate+1) cash_sum,
    (select count(1) from ofb_payment_session where session_status=3         
     and date_opened>=s.ddate and date_opened<s.ddate+1) void
    from (select cast('{date_str}' as date) ddate) s"""

    res = data_tools.get_result_from_query(conn,sql)        
    res_line = res[0]
    
    subject = f"Monitoring for {config.get('root','all.store')} - {date_str}"
    
    d7 = res_line['order_sum'] - res_line['etf_sum'] - res_line['cash_sum']
    
    body = "MerchantName\tDate\tTotalCardPaymentCount\tTotalCardAmount\tTotalCashAmount\tTotalVoidsCount\tTotalOrderAmount-TotalCardAmount-TotalCashAmount\n"
    body += f"{config.get('root','all.store')}\t{date_str}\t{res_line['etf_count']}\t{res_line['etf_sum']}\t"
    body += f"{res_line['cash_sum']}\t{res_line['void']}\t{d7}"
    
    send_mail(config,subject,body)
        
def main():    
    parser = argparse.ArgumentParser(description='Use: python validator command [arguments].')
    parser.add_argument('--config', help='configuration file location')
    parser.add_argument('--date', help='starting time to find orders')
    
    args = parser.parse_args()
    config = read_config(args)
    conn = open_connection(config)
        
    export(config,conn,args)
        
    l.logger.info('End.')

if __name__== "__main__": main()