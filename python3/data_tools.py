import psycopg2
import psycopg2.extras
import app_logger as l

def to_dict(row_dict):
    result = {}
#    for key, value in row_dict.iteritems():
    for key, value in row_dict.items():
        result[key] = value
    return result;

def get_result_from_query(conn,sql):
    result = []
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    try:
        cur.execute(sql)

        for record in cur:
            result.append(to_dict(record))
        #_LOGGER.debug('Exec postgre sql script:SUCCESS')
        l.logger.debug('Exec postgre sql script:SUCCESS')
    finally:
        cur.close()
    return result

def get_payment_type(conn,order_id):
    sql = f"""select p.payment_method_type_id from payment p 
        where p.payment_ref_num ='{order_id}'; 
        """
    cr = get_result_from_query(conn,sql)
    result = ''
    for mth in cr:
        if mth['payment_method_type_id']=='CASH': result = 'CASH'
        if mth['payment_method_type_id']=='CREDIT_CARD': result = 'CREDIT_CARD'
    return result
