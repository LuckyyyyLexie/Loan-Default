use bank;
################ clean and preprocessing data ################
select distinct k_symbol
from trans
;

select account_id,
       sum(case when operation = 'VYBER KARTOU' then 1 else 0 end) as times_credit_withdraw,
       sum(case when k_symbol = 'VKLAD' then amount else 0 end) as times_credit_incash,
       sum(case when k_symbol = 'PREVOD Z UCTU' then 1 else 0 end) as times_collect_anotherbank,
       sum(case when k_symbol = 'VYBER' then amount else 0 end) as times_cash_withdraw,
       sum(case when k_symbol = 'PREVOD NA UCET' then 1 else 0 end) as times_remittance,
       sum(case when k_symbol = 'SIPO' then 1 else 0 end) as times_payment_for_household,
       sum(case when k_symbol = 'SIPO' then amount else 0 end) as payment_for_household,
       sum(case when k_symbol = 'SLUZBY' then 1 else 0 end) as times_payment_for_dailylife,
       sum(case when k_symbol = 'SLUZBY' then amount else 0 end) as payment_for_dailylife,
       sum(case when k_symbol = 'UVER' then 1 else 0 end) as times_payment_for_loan,
       sum(case when k_symbol = 'UVER' then amount else 0 end) as payment_for_loan,
       sum(case when k_symbol = 'POJISTNE' then 1 else 0 end) as times_payment_for_insurance,
       sum(case when k_symbol = 'POJISTNE' then amount else 0 end) as payment_for_insurance,
       sum(case when k_symbol = 'DUCHOD' then 1 else 0 end) as times_payment_for_oldagepension,
       sum(case when k_symbol = 'DUCHOD' then amount else 0 end) as payment_for_oldagepension,
       sum(case when k_symbol = 'UROK' then 1 else 0 end) as times_payment_for_interestcredited,
       sum(case when k_symbol = 'UROK' then amount else 0 end) as payment_for_interestcredited,
       sum(case when k_symbol = 'SANKC. UROK' then 1 else 0 end) as times_payment_for_sactioninterest,
       sum(case when k_symbol = 'SANKC. UROK' then amount else 0 end) as payment_for_sactioninterest
from trans
group by account_id;
################ create features - transaction information ################
-- With the limitation of transaction date(before loan date),
-- all of the values in old age pension and loan payment are zero,drop them off
drop view if exists trans_feature;
create view trans_feature as
select l.account_id,
       min(balance) as minbalance,
       sum(case when balance<0 then 1 else 0 end) as neg_balance_times,
       sum(case when k_symbol = 'SIPO' then 1 else 0 end) as times_payment_for_household,
       sum(case when k_symbol = 'SIPO' then t.amount else 0 end) as payment_for_household,
       sum(case when k_symbol = 'SLUZBY' then 1 else 0 end) as times_payment_for_dailylife,
       sum(case when k_symbol = 'SLUZBY' then t.amount else 0 end) as payment_for_dailylife,
       sum(case when k_symbol = 'POJISTNE' then 1 else 0 end) as times_payment_for_insurance,
       sum(case when k_symbol = 'POJISTNE' then t.amount else 0 end) as payment_for_insurance,
       sum(case when k_symbol = 'UROK' then 1 else 0 end) as times_payment_for_interestcredited,
       sum(case when k_symbol = 'UROK' then t.amount else 0 end) as payment_for_interestcredited,
       sum(case when k_symbol = 'SANKC. UROK' then 1 else 0 end) as times_payment_for_sactioninterest,
       sum(case when k_symbol = 'SANKC. UROK' then t.amount else 0 end) as payment_for_sactioninterest
from loans l
left join trans t
    on l.account_id = t.account_id
where t.trans_date < l.loan_date
group by l.account_id;

################ create features - disposition information ################
-- whether the account with partner or not
drop view if exists disp_type;
create view disp_type
as
(select account_id,
       case when count = 2 then 'Yes' else 'No' end as account_partner
from
(select account_id,
       count(*) as count
from disps
group by account_id) as t);

################ create features - loan information ################
-- sum of late payments and days since last late payment
drop view if exists loan_feature;
create view loan_feature
as
select account_id, loan_id, loan_amount, duration, payments, status, sum_late_payments, days_since_last_late_payment
from
(select *,
    min(days_since_lastlatepayment) over (partition by account_id) as days_since_last_late_payment
from
(select account_id, loan_id, loan_amount, duration, payments, status, trans_date, next_payment_date, loan_date, sum_late_payments,
    case when over_month > 0 then datediff(loan_date, next_payment_date) else NULL end as days_since_lastlatepayment
from
(select
    *,
    sum(over_month) over (partition by account_id) as sum_late_payments
from
(select *,
       case when span_days > 30 then 1 else 0 END as over_month
from
(select *,
    datediff(next_payment_date, trans_date) as span_days,
    count(k_symbol) over (partition by account_id) as sum_KSymbol
from
(select
       t.account_id, t.amount, t.balance, t.k_symbol, loan_id, status, t.trans_date as trans_date,
       lead(t.trans_date) over (partition by t.account_id, t.k_symbol) as next_payment_date,
       l.loan_date as loan_date, l.amount as loan_amount, l.duration, l.payments
from trans t
left join loans l on t.account_id = l.account_id
where t.trans_date < l.loan_date and t.k_symbol in ('SIPO', 'POJISTNE', 'SLUZBY', 'SANKC. UROK', 'UVER'))
as a) as b) as c) as d) as e) as f
;

################ combine the cleaned and processed data ################
drop table if exists Loan_Default;
create table Loan_Default
    as
(select loans.account_id as account_id,
        loan_date,
       lf.loan_amount, lf.duration, lf.payments,lf.status,lf.sum_late_payments,lf.days_since_last_late_payment,
       tf.minbalance, tf.neg_balance_times, tf.times_payment_for_household, tf.payment_for_household, tf.times_payment_for_dailylife,
       tf.payment_for_dailylife, tf.times_payment_for_insurance, tf.payment_for_insurance, tf.times_payment_for_interestcredited,
       tf.payment_for_interestcredited, tf.times_payment_for_sactioninterest, tf.payment_for_sactioninterest,
       dt.account_partner,
       birth_date, gender,d.*
from loans
left join loan_feature lf on loans.account_id = lf.account_id
left join trans_feature tf on lf.account_id = tf.account_id
left join disp_type dt on loans.account_id = dt.account_id
left join accounts a on loans.account_id = a.account_id
left join clients c on a.district_id = c.district_id
left join districts d on c.district_id = d.district_id
group by loans.account_id)
;