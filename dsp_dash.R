### dsp deep dive dash ###
source("~/r_files/deven_onboarding-master/reqfiles/snowflake.R")

library(dplyr)
library(scales)
library(googlesheets)

# token <- gs_auth(cache = FALSE)
# gd_token()
# saveRDS(token, file = "googlesheets_token.rds")
gs_auth(token = "C:/Users/deven.choi/Documents/googlesheets_token.rds")

# query to pull snowflake data for top 10 DSPs going back 90 days
query <- "
select 
x.utc_date_sid, 
x.utc_rollup_date, 
x.facility,
--x.week,
x.week2,
x.month, 
x.advertiser_account_nk,
a.advertiser_account_name, 
x.region,
x.device_type,
case when x.device_type in ('desktop', 'mobile web') and x.p_mapped_adunit_type in ('BANNER') then 'display'
when x.device_type in ('mobile app') and x.p_mapped_adunit_type in ('BANNER') then 'mobile app'
when x.p_mapped_adunit_type in ('VIDEO') then 'video'
else 'test' end as product_type,
x.p_mapped_adunit_type, 
x.tot_tx_rounds,
x.req_sent,
x.req_sent_wdata,
x.solicits,
x.throttled_, 
x.timeout_,
x.error_,
x.empty_,
x.tot_nonzero_ssrtb,
x.tot_nonzero_bids, 
x.tot_self_complete_count,

x.winning_bids,
x.impressions,
x.tot_total_bids_usd,
x.winning_bids_sum_usd,
x.win_billable_sum_usd,
x.spend_usd,
x.Reqs_Less_Throttled,
x.Unbillable,
x.Valid_Nonzero_Bids,
x.Total_Blocked,
x.AQ_blocks_zblock,
x.pub_blocks,
x.AQ_Blocks,
x.AQ_Scanner_Blocks,

x.tot_bids_discarded_brand,
x.tot_bids_discarded_unbranded,
x.tot_bids_discarded_brand_5Z_generic,
x.tot_bids_discarded_brand_5Z_adserver,
x.tot_bids_discarded_brand_5Z_video,
x.tot_bids_discarded_brand_5Z_malicious,
x.aq_scanner_brand,
x.aq_scanner_attribute,
x.aq_scanner_category,
x.aq_scanner_crid,
x.aq_scanner_language,
x.aq_scanner_type,
x.aq_scanner_ecrid,
x.tot_bids_discarded_domain
from (

SELECT utc_date_sid,
utc_rollup_date,
facility,
--date(timestampadd(d,5-(dayofweek_iso(utc_rollup_date)),date(utc_rollup_date))) as week,
--date(timestampadd(d,7-(dayofweek_iso(utc_rollup_date)),date(utc_rollup_date))) as week2,
CASE
WHEN dayofweek(utc_rollup_date) =6 THEN utc_rollup_date
WHEN dayofweek(utc_rollup_date) =7 THEN dateadd('day', 6, utc_rollup_date)
ELSE dateadd('day', 6-dayofweek(utc_rollup_date), utc_rollup_date)
END AS week, --case when dayofweek(utc_rollup_date) =1 then utc_rollup_date else date_add(utc_rollup_date, 8-dayofweek(utc_rollup_date)) end as week2,
--0 as week2,
CASE
WHEN dayofweek(utc_rollup_date) = 6 THEN dateadd('day', 2, utc_rollup_date)
WHEN dayofweek(utc_rollup_date) = 7 THEN dateadd('day', 2, dateadd('day', 6, utc_rollup_date))
ELSE dateadd('day', 2, dateadd('day', 6-dayofweek(utc_rollup_date), utc_rollup_date))
END AS week2, --to_date(utc_rollup_date::varchar, 'YYYY-MM-01') as month,
to_char(utc_rollup_date, 'yyyy-MM-01') AS month,
f.advertiser_account_nk,
case when f.country_code = 'US' then 'US' 
when f.country_code = 'JP' then 'JP'
when f.country_code in ('AT','BE','CZ','DE','DK','ES','FI','GB','GR','HR','HU','IE','IT','NL','NO','PL','PT','RU','SE','TR', 'FR', 'CH') then 'EMEA' 
else 'ROW' end as region,
case when utc_date_sid < 20160524 then 'ALL' 
when is_mobile_app then 'mobile app' 
when u_mobl_dev_cat in ( 'desktop', 'browser') then 'desktop' 
when u_mobl_dev_cat in ('mobile', 'tablet', 'text') then 'mobile web' else 'mobile web' end as device_type,
case when is_mobile_app then 'mobile app' else 'non-mobile app' end as device_type2,
p_mapped_adunit_type,
sum(tot_tx_rounds) as tot_tx_rounds,
sum(tot_ssrtb_requests) as req_sent,
sum(tot_rtb_data_sends) as req_sent_wdata,
sum(tot_solicit_rounds) as solicits,
sum(case when utc_date_sid <= 20160628 then tot_bids_discarded_throttled else tot_ssrtb_throttled end) as throttled_, --throttled rate
sum(case when utc_date_sid <= 20160628 then tot_bids_discarded_timeout else tot_ssrtb_timeouts end) as timeout_, --timeout rate
sum(case when utc_date_sid <= 20160628 then tot_bids_discarded_error else tot_ssrtb_error end ) as error_, --error rate
sum(case when utc_date_sid <= 20160628 then tot_bids_discarded_empty else tot_ssrtb_empty end ) as empty_,--empty rate
sum(tot_ssrtb_nonzero_single_bids) + sum(tot_ssrtb_nonzero_multi_bids) as tot_nonzero_ssrtb, --bid rate (ssrtb)
sum(tot_bids_received) as tot_nonzero_bids, 
sum(tot_bids_discarded_selfcompete) as tot_self_complete_count,
sum(tot_bids_discarded_brand) as tot_bids_discarded_brand,
sum(tot_bids_discarded_unbranded) as tot_bids_discarded_unbranded,
sum(tot_bids_discarded_brand_5Z_generic) as tot_bids_discarded_brand_5Z_generic,
sum(tot_bids_discarded_brand_5Z_adserver) as tot_bids_discarded_brand_5Z_adserver,
sum(tot_bids_discarded_brand_5Z_video) as tot_bids_discarded_brand_5Z_video,
sum(tot_bids_discarded_brand_5Z_malicious) as tot_bids_discarded_brand_5Z_malicious,
sum(tot_bids_discarded_aq_brand) as aq_scanner_brand,
sum(tot_bids_discarded_aq_attribute) as aq_scanner_attribute,
sum(tot_bids_discarded_aq_category) as aq_scanner_category,
sum(tot_bids_discarded_aq_crid) as aq_scanner_crid,
sum(tot_bids_discarded_aq_language) as aq_scanner_language,
sum(tot_bids_discarded_aq_type) as aq_scanner_type,
sum(tot_bids_discarded_aq_ecrid) as aq_scanner_ecrid,
sum(tot_bids_discarded_domain) as tot_bids_discarded_domain, 
sum(tot_wins) as winning_bids,
sum(tot_impressions_paid) as impressions,
--sum(tot_total_bids*c.exchange_rate) as nonzero_bids_sum_usd,
sum(tot_total_bids_usd) as tot_total_bids_usd,
sum(case when utc_date_sid <= 20160628 then tot_winning_bids * c.exchange_rate else tot_winning_bids_usd end) as winning_bids_sum_usd,
sum(tot_sum_win_bid_value_billable_usd) as win_billable_sum_usd,
sum(tot_spend_usd) as spend_usd,
sum(tot_ssrtb_requests) - sum(case when utc_date_sid <= 20160628 then tot_bids_discarded_throttled else tot_ssrtb_throttled end) as Reqs_Less_Throttled,
sum(tot_wins) - sum(tot_impressions_paid) as Unbillable,
sum(tot_bids_received_valid) as Valid_Nonzero_Bids,
sum(tot_bids_discarded_brand) + sum(tot_bids_discarded_unbranded) 
+ sum(tot_bids_discarded_brand_5Z_generic) + sum(tot_bids_discarded_brand_5Z_adserver) + sum(tot_bids_discarded_brand_5Z_video) + sum(tot_bids_discarded_brand_5Z_malicious)
+ sum(tot_bids_discarded_aq_attribute) + sum(tot_bids_discarded_aq_brand) + sum(tot_bids_discarded_aq_category) + sum(tot_bids_discarded_aq_crid) + sum(tot_bids_discarded_aq_language) + sum(tot_bids_discarded_aq_type)+
+ sum(tot_bids_discarded_domain) + sum(tot_bids_discarded_pmp_buyer) + sum(tot_bids_discarded_brand) + sum(tot_bids_discarded_pmp_aq_brand)
as Total_Blocked,
sum(tot_bids_discarded_brand_5Z_generic) + sum(tot_bids_discarded_brand_5Z_adserver) + sum(tot_bids_discarded_brand_5Z_video) + sum(tot_bids_discarded_brand_5Z_malicious)
as AQ_blocks_zblock,
sum(tot_bids_discarded_brand) + sum(tot_bids_discarded_unbranded)	as pub_blocks,
sum(tot_bids_discarded_brand_5Z_generic) + sum(tot_bids_discarded_brand_5Z_adserver) + sum(tot_bids_discarded_brand_5Z_video) + sum(tot_bids_discarded_brand_5Z_malicious)
+ sum(tot_bids_discarded_aq_attribute) + sum(tot_bids_discarded_aq_brand) + sum(tot_bids_discarded_aq_category) + sum(tot_bids_discarded_aq_crid) + sum(tot_bids_discarded_aq_language) + sum(tot_bids_discarded_aq_type) + sum(tot_bids_discarded_aq_ecrid)
as AQ_Blocks,
sum(tot_bids_discarded_aq_attribute) + sum(tot_bids_discarded_aq_brand) + sum(tot_bids_discarded_aq_category) + sum(tot_bids_discarded_aq_crid) + sum(tot_bids_discarded_aq_language) + sum(tot_bids_discarded_aq_type) + sum(tot_bids_discarded_aq_ecrid)
as AQ_Scanner_Blocks
from mstr_datamart.ox_rtb_data_daily_fact f
left join mstr_datamart.currency_exchange_daily_fact c on f.utc_date_sid = c.date_sid and f.a_coin = c.base and c.currency = 'USD'

where utc_rollup_date >= current_date - 91 
and utc_rollup_date < current_date
and tot_ssrtb_requests > 100

group by 1,2,3,4,5,6,7,8,9,10,11
) X

left join mstr_datamart.advertiser_dim a on x.advertiser_account_nk = a.advertiser_account_nk 
where x.advertiser_account_nk in (
'537073246',
'537073292',
'537073294',
'537073301',
'537073219',
'537148859',
'537073399',
'537073283',
'537073287',
'537073286');
"

snowflake_data <- dbGetQuery(snowflake, query)
names(snowflake_data) <- tolower(names(snowflake_data))

query <- "
select
date_sid
,b.advertiser_account_name as dsp_name
,facility as facility1
,case when dev_type in ('desktop', 'mobile web') and p_mapped_adunit_type in ('BANNER') then 'display'
when dev_type in ('mobile app') and p_mapped_adunit_type in ('BANNER') then 'mobile app'
when p_mapped_adunit_type in ('VIDEO') then 'video'
else 'test' end as product_type1
,buyer_synced as buyer_synced
,case when gdpr_country = 'GDPR COUNTRIES' then 'EMEA' else gdpr_country end as dsp_region
,(sum(REQ_SEEN_WDEAL)+sum(REQ_SEEN_WODEAL)) as ssrtb_seen
,(sum(REQ_THROTTLED_WDEAL)+sum(REQ_THROTTLED_WODEAL)+sum(PBID_THROTTLED_IT)+sum(TS_THROTTLED_IT)) as ssrtb_throttled

from businessintelligence.ssrtb_pbid_requests as a
left join mstr_datamart.advertiser_dim b on a.b_act_id = b.advertiser_account_nk

where b_act_id in (
'537073246', 
'537073292',
'537073294',
'537073301',
'537073219',
'537148859',
'537073399',
'537073283',
'537073287',
'537073286'
)
and date_sid between to_char(current_date - 91, 'yyyymmdd') and to_char(current_date - 1, 'yyyymmdd')
group by 1,2,3,4,5,6
order by 7 desc;
"

hive_data <- dbGetQuery(snowflake, query)
names(hive_data) <- tolower(names(hive_data))

hive_data$dsp_region <- toupper(hive_data$dsp_region)

hive_data$ssrtb_throttled[is.na(hive_data$ssrtb_throttled)] <- 0

# summarize snowflake data by date, dsp, and region
summarized_snowflake_data <- snowflake_data %>%
  group_by(utc_date_sid, advertiser_account_name, region) %>%
  summarize(net_bid_requests = sum(reqs_less_throttled),
            matched_bid_requests = sum(req_sent_wdata),
            timeouts = sum(timeout_),
            errors = sum(error_),
            tot_nonzero_ssrtb = sum(tot_nonzero_ssrtb),
            tot_nonzero_bids = sum(tot_nonzero_bids),
            blocks = sum(total_blocked),
            valid_nonzero_bids = sum(valid_nonzero_bids),
            winning_bids = sum(winning_bids),
            impressions = sum(impressions),
            tot_total_bids_usd = sum(tot_total_bids_usd),
            winning_bids_sum_usd = sum(winning_bids_sum_usd),
            spend = sum(spend_usd))

# summarize hive data by date, dsp, and region
summarized_hive_data <- hive_data %>%
  group_by(date_sid, dsp_name, dsp_region) %>%
  summarize(ssrtb_requests_seen = sum(ssrtb_seen)*100,
            ssrtb_requests_throttled = sum(ssrtb_throttled)*100,
            ssrtb_requests_total = sum(ssrtb_seen + ssrtb_throttled)*100)

# create key for 
summarized_snowflake_data$key <- paste0(summarized_snowflake_data$utc_date_sid, summarized_snowflake_data$advertiser_account_name, summarized_snowflake_data$region)
summarized_hive_data$key <- paste0(summarized_hive_data$date_sid, summarized_hive_data$dsp_name, summarized_hive_data$dsp_region)

# merge hive pbid data with overall data
overall <- left_join(summarized_snowflake_data, summarized_hive_data, by = "key")

# remove redundant columns and rename others
overall$key <- NULL
overall$date_sid <- NULL
overall$dsp_name <- NULL
overall$dsp_region <- NULL

# add in calculated fields
data <- overall %>%
  group_by(utc_date_sid, advertiser_account_name, region) %>%
  summarize(net_bid_requests = sum(net_bid_requests),
            timeout_rate = sum(timeouts/net_bid_requests, na.rm = TRUE),
            error_rate = sum(errors/net_bid_requests, na.rm = TRUE),
            throttle_rate = sum(ssrtb_requests_throttled/ssrtb_requests_total, na.rm = TRUE),
            bid_rate = sum(tot_nonzero_ssrtb/net_bid_requests, na.rm = TRUE),
            block_rate = sum(blocks/tot_nonzero_bids, na.rm = TRUE),
            valid_rate = sum(valid_nonzero_bids/tot_nonzero_bids, na.rm = TRUE),
            internal_win_rate = sum(winning_bids/tot_nonzero_bids, na.rm = TRUE),
            dsp_billable_rate = sum(impressions/winning_bids, na.rm = TRUE),
            bid_cpm = sum(tot_total_bids_usd/tot_nonzero_bids*1000, na.rm = TRUE),
            winning_bid_cpm = sum(winning_bids_sum_usd/winning_bids*1000, na.rm = TRUE),
            impressions = sum(impressions),
            spend = sum(spend),
            cpm = sum(spend/impressions*1000, na.rm = TRUE),
            dsp_str = sum(impressions/net_bid_requests, na.rm = TRUE),
            dsp_rcpm = sum(spend/net_bid_requests*1000, na.rm = TRUE),
            dsp_win_rate = sum(impressions/tot_nonzero_bids, na.rm = TRUE),
            dsp_match_rate = sum(matched_bid_requests/net_bid_requests, na.rm = TRUE))

# add in paid to bid and replace NAs with 0
data$paid_to_bid <- data$cpm/data$winning_bid_cpm
data$paid_to_bid[is.na(data$paid_to_bid)] <- 0

write.csv(data, "C:/Users/deven.choi/Desktop/dsp_dash_data.csv", row.names = FALSE)

# update google sheets
#gs_update <- gs_title("DSP Dash Raw")
#gs_edit_cells(gs_update, ws = "Sheet1", input = data, anchor = "A1", trim = TRUE, col_names = TRUE)
gs_upload("C:/Users/deven.choi/Desktop/dsp_dash_data.csv", sheet_title = "DSP Dash Raw", verbose = TRUE, overwrite = TRUE)

rm(list=ls())