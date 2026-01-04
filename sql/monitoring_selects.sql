-- email
select 
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
 and effective_date>=s.ddate and effective_date<s.ddate+1) etf_count,
(select coalesce(sum(amount),0) from payment 
 where payment_method_type_id='CASH' 
 and effective_date>=s.ddate and effective_date<s.ddate+1) etf_sum,
(select count(1) from ofb_payment_session where session_status=3         
 and date_opened>=s.ddate and date_opened<s.ddate+1) void
from (select cast('2024-01-04' as date) ddate) s;


-- orders
select * from order_header 
where order_date>='2024-01-05' and order_date<'2024-01-06';

-- payments
select * from payment where effective_date>='2024-01-05' and effective_date<'2024-01-06';

-- transaction
SELECT * FROM ofb_payment_transaction where
created_stamp='2024-01-05' and created_stamp<'2024-01-06'; and payment_type is not null
order by created_stamp;

-- void
select * from ofb_payment_session where session_status=3 
and date_opened>='2023-11-29' and date_opened<'2023-11-30';