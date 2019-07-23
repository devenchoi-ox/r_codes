# app - who buys what
options(java.parameters = "-Xmx8000m")

# input
first_date = Sys.Date() - 91
end_date = Sys.Date() - 1

source("~/r_files/deven_onboarding-master/reqfiles/snowflake.R")

# library
library(tidyr)
library(dplyr)
library(scales)
library(RJDBC)
library(reshape2)
options(scipen=999)


query <- "SELECT 
a.site_nk
,xyz.site_name
,xyz.account_name as SF_Account_Name
,xyz.yield_analyst as YM
,xyz.AM
,case     
when p_platform_id in ('0bf376ff-32dc-4dec-835b-d4e9fa3062c1','41369f8c-6fd8-4c86-b8bb-fad81774416e','bbb82fae-1d27-4d90-bb10-e24164ecd7bc') then 'eb'
when xyz.site_name like '%A9%' then 'a9'
when a.x_bidout then 'other bidout'
when a.p_mobl_sdk_version is not null then 'sdk'
when (a.publisher_account_nk in ('537127785','537124160','537113957','537152826','537127194') or a.site_nk in ('540155685','540151073','540131696','539974417','539854618','539744338','539646619','539645874','539513282','539472296','539444135','539408677','539316923','539316826','539304096','539246808','539164681','539078156','539044239','538963579','537151860','537151512','537151480','537143821','537137865','537135714','537120563','537116995','537113195','537080940','537074811')) then 's2s'
else 'js tags' end as integration_type
FROM mstr_datamart.supply_demand_country_daily_fact AS a 
LEFT JOIN mstr_datamart.dim_sites_to_owners AS xyz ON a.site_nk = xyz.site_nk
WHERE a.instance_rollup_date >= 'dog'
AND a.instance_rollup_date < 'cat'
GROUP BY 1,2,3,4,5,6;"

query <- gsub('dog', first_date, query)
query <- gsub('cat', end_date, query)
site_names <- dbGetQuery(snowflake, query)

query <- "SELECT 
to_char(bb.utc_rollup_date, 'yyyy-mm') as month
,bb.site_nk
,bb.advertiser_account_nk
,bb.p_mobl_app_bundle as bundle_id
,bb.p_mobl_app_name as app_name
,bb.x_rewarded as rewarded_flag
,bb.p_mapped_adunit_type as creative_type
,ROUND(SUM(bb.tot_usd_a_spend), 2) AS spend_usd
FROM mstr_datamart.ox_transaction_sum_daily_fact AS bb
WHERE bb.utc_rollup_date >= current_date - 91
AND bb.utc_rollup_date < current_date -1
AND bb.is_mobile_app = 'true'
AND bb.site_nk IS NOT NULL
AND bb.advertiser_account_nk IN 
(
    SELECT bb.advertiser_account_nk
    FROM mstr_datamart.ox_transaction_sum_daily_fact AS bb
    WHERE bb.utc_rollup_date >= current_date - 91
    AND bb.utc_rollup_date < current_date -1
    AND bb.is_mobile_app = 'true'
    AND bb.tot_usd_a_spend > 0.5
    GROUP BY 1
)
and bb.tot_mkt_req > 5000
GROUP BY 1,2,3,4,5,6,7
ORDER BY 8 DESC;"

bundles_supply <- dbGetQuery(snowflake, query)

query <- "SELECT 
to_char(bb.utc_rollup_date, 'yyyy-mm') as month
,bb.advertiser_account_nk
,bb.p_mobl_app_bundle as bundle_id
,bb.x_rewarded as rewarded_flag
,bb.p_mapped_adunit_type as creative_type
,ROUND(SUM(bb.tot_usd_a_spend), 2) AS spend_usd
FROM mstr_datamart.ox_transaction_sum_daily_fact AS bb
WHERE bb.utc_rollup_date >= current_date - 91
AND bb.utc_rollup_date < current_date -1
AND bb.is_mobile_app = 'true'
AND bb.site_nk IS NOT NULL
AND bb.advertiser_account_nk IN 
(
    SELECT bb.advertiser_account_nk
    FROM mstr_datamart.ox_transaction_sum_daily_fact AS bb
    WHERE bb.utc_rollup_date >= current_date - 91
    AND bb.utc_rollup_date < current_date -1
    AND bb.is_mobile_app = 'true'
    AND bb.tot_usd_a_spend > 0.5
    GROUP BY 1
)
GROUP BY 1,2,3,4,5
ORDER BY 6 DESC;"

bundles_demand <- dbGetQuery(snowflake, query)

query <- "select 
f.advertiser_account_nk
,d.advertiser_account_name
from mstr_datamart.ox_transaction_sum_daily_fact AS f
join mstr_datamart.advertiser_dim d on f.advertiser_account_nk = d.advertiser_account_nk
where f.utc_rollup_date >= current_date - 91
and f.utc_rollup_date < current_date - 1
and f.is_mobile_app = 'true'
group by 1,2
ORDER BY 1,2;"

dsp_names <- dbGetQuery(snowflake, query)

query <- "select 
a.advertiser_account_name
,u1.name as PDM
from mstr_datamart.advertiser_dim as a
left join mstr_datamart.SF_Product__c sfp on cast(a.advertiser_account_nk as varchar) = cast(sfp.Instance_ID__c as varchar) and sfp.Product__c = 'Ad Exchange for Buyers (AXB)'
left join mstr_datamart.sf_account sfa on sfp.Account__c = sfa.id 
left join mstr_datamart.SF_AccountTeamMember atm1 
on sfa.id = atm1.accountid and atm1.teammemberrole in ('Account Manager - Platform Demand')
left join mstr_datamart.sf_user as u1 
on atm1.UserId = u1.id
where u1.name is not null
group by 1,2;"

pdm_names <- dbGetQuery(snowflake, query)

# disconnect Snowflake DB
rm(sDriver, snowflake, first_date, end_date)

names(site_names) <- tolower(names(site_names))
names(bundles_supply) <- tolower(names(bundles_supply))
names(bundles_demand) <- tolower(names(bundles_demand))
names(dsp_names) <- tolower(names(dsp_names))
names(pdm_names) <- tolower(names(pdm_names))

#combine and format supply view data
bundles_supply <- left_join(bundles_supply, site_names, by = "site_nk")
bundles_supply <- left_join(bundles_supply, dsp_names, by = "advertiser_account_nk") 
bundles_s <- bundles_supply
bundles_s$advertiser_account_nk <- NULL
bundles_s$spend_usd[is.na(bundles_s$spend_usd)] <- 0
bundles_s <- unique(bundles_s)

#combine and format demand view data
bundles_demand <- left_join(bundles_demand, dsp_names, by = "advertiser_account_nk") 
bundles_demand <- left_join(bundles_demand, pdm_names, by = "advertiser_account_name") 
bundles_d <- bundles_demand
bundles_d$advertiser_account_nk <- NULL
bundles_d$spend_usd[is.na(bundles_d$spend_usd)] <- 0
bundles_d <- unique(bundles_d)

#spread supply view
week_data_wide <- spread(bundles_s, advertiser_account_name, spend_usd)
week_data_wide[is.na(week_data_wide)] <- 0
week_data_wide$total_spend <-  rowSums(week_data_wide[, c(14:ncol(week_data_wide))], na.rm = TRUE)
week_data_wide <- week_data_wide[order(-week_data_wide$total_spend),]

bundles_supply <- week_data_wide

#spread demand view
week_data_wide <- spread(bundles_d, advertiser_account_name, spend_usd)
week_data_wide[is.na(week_data_wide)] <- 0
week_data_wide$total_spend <-  rowSums(week_data_wide[, c(8:ncol(week_data_wide))], na.rm = TRUE)
week_data_wide <- week_data_wide[order(-week_data_wide$total_spend),]

bundles_demand <- week_data_wide

# save file
write.csv(bundles_s, "~/bundles_s.csv", row.names = FALSE)
write.csv(bundles_supply, "~/bundles_supply.csv", row.names = FALSE)
write.csv(bundles_demand, "~/bundles_demand.csv", row.names = FALSE)

# remove everything
#rm(list=ls())

