source("~/r_files/deven_onboarding-master/reqfiles/snowflake.R")
library(lubridate)
library(dplyr)
library(mailR)
library(htmlTable)
library(scales)

# query for prior 4 weeks of data
query <- "
select

c.brand_name as advertiser
--,(case when a.deal_nk is not null then 'PD'
--when a.x_gd_deal_id is not null then 'TE'
--else 'OA' end) as auction_type_flag
,sum(tot_a_mkt_spend_usd) as spend_usd
,sum(tot_mkt_impressions) as impressions

from mstr_datamart.ox_buyer_brand_sum_hourly_fact as a
left join mstr_datamart.brand_dim c using (brand_nk)

where utc_timestamp >= current_date - 28
and coalesce(a.deal_nk, a.x_gd_deal_id) is not null
and country_code = 'US'
and tot_a_mkt_spend_usd > 0

group by 1
order by 2 desc"

prior_four <- dbGetQuery(snowflake, query)
names(prior_four) <- tolower(names(prior_four))

# query for week over week with brand name and deal id
query <- "
select

utc_date_sid
,c.brand_name as advertiser
--,(case when a.deal_nk is not null then 'PD'
--when a.x_gd_deal_id is not null then 'TE'
--else 'Other' end) as pmp_type_flag
,coalesce(d.deal_id, e.deal_id) as deal_id
,sum(tot_a_mkt_spend_usd) as spend_usd
,sum(tot_mkt_impressions) as impressions

from mstr_datamart.ox_buyer_brand_sum_hourly_fact as a
left join mstr_datamart.brand_dim c using (brand_nk)
left join mstr_datamart.deal_dim d using (deal_nk)
left join mstr_datamart.deal_dim e on a.x_gd_deal_id = e.deal_nk

where coalesce(a.deal_nk, a.x_gd_deal_id) is not null
and utc_date_sid between (date_start) and (date_end)
and country_code = 'US'
and tot_a_mkt_spend_usd > 0

group by 1,2,3
order by 4 desc;
"

# calculate dates
#latest_monday <- as.Date(cut(Sys.Date(), "weeks"))
#latest_monday <- format(latest_monday, "%Y%m%d")

latest_sunday <- as.Date(cut(Sys.Date(), "weeks")) - 1
#latest_sunday <- format(latest_sunday, "%Y%m%d")

# date_start <- latest_monday - 14
date_start <- Sys.Date() - 14
date_start <- format(date_start, "%Y%m%d")

date_end <- Sys.Date() - 1
date_end <- format(latest_sunday, "%Y%m%d")

query <- gsub("date_start", date_start, query)
query <- gsub("date_end", date_end, query)

deal_id <- dbGetQuery(snowflake, query)
names(deal_id) <- tolower(names(deal_id))

# MSTR data
# query <- "
# select
# 
# utc_date_sid
# ,country_code as geo
# ,sum(tot_bid_requests) as bid_requests
# ,sum(tot_pos_bid_opportunities) as bids
# ,sum(tot_mkt_impressions) as impressions
# 
# from mstr_datamart.ox_buyer_brand_sum_hourly_fact as a
# 
# where utc_date_sid between (date_start) and (date_end)
# and country_code = 'US'
# and coalesce(a.deal_nk, a.x_gd_deal_id) is not null
# 
# group by 1,2;
# "
# query <- gsub("date_start", date_start, query)
# query <- gsub("date_end", date_end, query)
# 
# mstr_data <- dbGetQuery(snowflake, query)
# names(mstr_data) <- tolower(names(mstr_data))

prior_four_weeks <- prior_four %>%
    group_by (advertiser) %>%
    summarize(advertiser_spend = sum(spend_usd),
              sold_impressions = sum(impressions),
              four_week_avg_advertiser_spend = sum(spend_usd) / 4
              #spend_cpm = sum((spend_usd / impressions) * 1000)
    )

# assign dates
prior1 <- format(Sys.Date() - 14, "%Y%m%d")
prior2 <- format(Sys.Date() - 8, "%Y%m%d")
after1 <- format(Sys.Date() - 7, "%Y%m%d")
after2 <- format(Sys.Date()- 1, "%Y%m%d")

deal_id_data <- deal_id %>%
    group_by (advertiser, deal_id) %>%
    summarize(advertiser_spend = sum(spend_usd),
              sold_impressions = sum(impressions),
              #spend_cpm = sum((spend_usd / impressions) * 1000),
              prior_week_spend = sum(spend_usd[utc_date_sid >= prior1 & utc_date_sid <= prior2]),
              curr_week_spend = sum(spend_usd[utc_date_sid >= after1 & utc_date_sid <= after2]),
              prior_week_imps = sum(impressions[utc_date_sid >= prior1 & utc_date_sid <= prior2]),
              curr_week_imps = sum(impressions[utc_date_sid >= after1 & utc_date_sid <= after2]))

rev_data <- deal_id %>%
    group_by (advertiser) %>%
    summarize(advertiser_spend = sum(spend_usd),
              sold_impressions = sum(impressions),
              #spend_cpm = sum((spend_usd / impressions) * 1000),
              prior_week_spend = sum(spend_usd[utc_date_sid >= prior1 & utc_date_sid <= prior2]),
              curr_week_spend = sum(spend_usd[utc_date_sid >= after1 & utc_date_sid <= after2]),
              prior_week_imps = sum(impressions[utc_date_sid >= prior1 & utc_date_sid <= prior2]),
              curr_week_imps = sum(impressions[utc_date_sid >= after1 & utc_date_sid <= after2]))


# remove NAs
prior_four_weeks <- na.omit(prior_four_weeks)
deal_id_data <- na.omit(deal_id_data)
rev_data <- na.omit(rev_data)

# add in CPM
prior_four_weeks$spend_cpm <- (prior_four_weeks$advertiser_spend / prior_four_weeks$sold_impressions) * 1000
deal_id_data$spend_cpm <- (deal_id_data$curr_week_spend / deal_id_data$curr_week_imps) * 1000
rev_data$spend_cpm <- (rev_data$curr_week_spend / rev_data$curr_week_imps) * 1000

prior_four_weeks <- prior_four_weeks[,c(1:2,5,3:4)]

deal_id_data <- deal_id_data[,c(1:2,9,5:8)]
rev_data <- rev_data[,c(1:2,8,4:7)]

# sort by spend
prior_four_weeks <- prior_four_weeks[order(-prior_four_weeks$advertiser_spend),]
deal_id_data <- deal_id_data[order(-deal_id_data$curr_week_spend ),]
rev_data <- rev_data[order(-rev_data$curr_week_spend ),]

# add in week over week change numbers + % of total
deal_id_data$spend_chg_abs <- deal_id_data$curr_week_spend - deal_id_data$prior_week_spend
deal_id_data$spend_chg_rel <- deal_id_data$spend_chg_abs / (abs(deal_id_data$spend_chg_abs) + deal_id_data$curr_week_spend)

rev_data$spend_chg_abs <- rev_data$curr_week_spend - rev_data$prior_week_spend
rev_data$spend_chg_rel <- rev_data$spend_chg_abs / (abs(rev_data$spend_chg_abs) + rev_data$curr_week_spend)
rev_data$percent_of_total <- rev_data$curr_week_spend / sum(rev_data$curr_week_spend)

# remove unnecessary columns
deal_id_data$prior_week_spend <- NULL
deal_id_data$prior_week_imps  <- NULL

rev_data$prior_week_spend <- NULL
rev_data$prior_week_imps  <- NULL

#### create tables

# "top revenue generating te/pmp advertisers"

# get top 5 advertisers
table1 <- rev_data[1:5,]

# join with avg 4 weeks data and remove unneeded columns
table1 <- left_join(table1, prior_four_weeks, by = "advertiser")
table1 <- table1[,c(1,3:8,12)]  

# add in delta to average spend column
table1$delta_vs_four_week_avg <- table1$curr_week_spend - table1$four_week_avg_advertiser_spend

# initialize data frame
data1 <- setNames(data.frame(matrix(ncol = 1, nrow = 0)), c("top_deal_id"))

# add in top deal id per advertiser
for(i in 1:5)
{
    # store advertiser name in variable
    adv_name <- as.character(table1[i,1])
    
    # filter for advertiser
    tmp_tbl <- filter(deal_id_data, advertiser == adv_name)
    
    # sort by top spend
    tmp_tbl <- tmp_tbl[order(-tmp_tbl$curr_week_spend ),]
    
    # take deal id with top spend and store it
    a <- tmp_tbl[1,2]
    
    data1 <- rbind(data1, a)
}

table1 <- cbind(table1, data1)

# "worst WoW te/pmp advertisers"

# get worst 5 advertisers week over week
table2 <- rev_data[order(rev_data$spend_chg_abs),]
table2 <- table2[1:5,]

# join with avg 4 weeks data and remove unneeded columns
table2 <- left_join(table2, prior_four_weeks, by = "advertiser")
table2 <- table2[,c(1,3:8,12)]

# add in delta to average spend column
table2$delta_vs_four_week_avg <- table2$curr_week_spend - table2$four_week_avg_advertiser_spend

# initialize data frame
data2 <- setNames(data.frame(matrix(ncol = 1, nrow = 0)), c("deal_id"))

# add in worst deal id per advertiser
for(i in 1:5)
{
    # store advertiser name in variable
    adv_name <- as.character(table2[i,1])
    
    # filter for advertiser
    tmp_tbl <- filter(deal_id_data, advertiser == adv_name)
    
    # sort by worst spend
    tmp_tbl <- tmp_tbl[order(tmp_tbl$spend_chg_abs),]
    
    # take deal id with worst spend and store it
    a <- tmp_tbl[1,2]
    
    data2 <- rbind(data2, a)
}

table2 <- cbind(table2, data2)

# "Best WoW te/pmp advertisers"

# get best 5 advertisers week over week
table3 <- rev_data[order(-rev_data$spend_chg_abs),]
table3 <- table3[1:5,]

# join with avg 4 weeks data and remove unneeded columns
table3 <- left_join(table3, prior_four_weeks, by = "advertiser")
table3 <- table3[,c(1,3:8,12)]

# add in delta to average spend column
table3$delta_vs_four_week_avg <- table3$curr_week_spend - table3$four_week_avg_advertiser_spend

# initialize data frame
data3 <- setNames(data.frame(matrix(ncol = 1, nrow = 0)), c("deal_id"))

# add in best deal id per advertiser
for(i in 1:5)
{
    # store advertiser name in variable
    adv_name <- as.character(table3[i,1])
    
    # filter for advertiser
    tmp_tbl <- filter(deal_id_data, advertiser == adv_name)
    
    # sort by top spend
    tmp_tbl <- tmp_tbl[order(-tmp_tbl$spend_chg_abs),]
    
    # take deal id with top spend and store it
    a <- tmp_tbl[1,2]
    
    data3 <- rbind(data3, a)
}

table3 <- cbind(table3, data3)

# "New deals / turned on brands"

# get advertisers with no spend last week
table4 <- rev_data[order(-rev_data$spend_chg_abs),]
table4$new_deals <- table4$spend_chg_abs - table4$curr_week_spend 

table4$flag <- ifelse(table4$curr_week_spend  > 20 & table4$new_deals > -10, "true", "false")
table4 <- table4[table4$flag == "true",]

table4 <- table4[1:5,]

# join with avg 4 weeks data and remove unneeded columns
table4 <- left_join(table4, prior_four_weeks, by = "advertiser")
table4 <- table4[,c(1,3:8,14)]

# add in delta to average spend column
table4$delta_vs_four_week_avg <- table4$curr_week_spend - table4$four_week_avg_advertiser_spend

# initialize data frame
data4 <- setNames(data.frame(matrix(ncol = 1, nrow = 0)), c("deal_id"))

# add in best deal id per advertiser
for(i in 1:5)
{
    # store advertiser name in variable
    adv_name <- as.character(table4[i,1])
    
    # filter for advertiser
    tmp_tbl <- filter(deal_id_data, advertiser == adv_name)
    
    # sort by top spend
    tmp_tbl <- tmp_tbl[order(-tmp_tbl$spend_chg_abs),]
    
    # take deal id with top spend and store it
    a <- tmp_tbl[1,2]
    
    data4 <- rbind(data4, a)
}

table4 <- cbind(table4, data4)

# formatting changes and clean up

# remove cumulative advertiser spend, sold impressions
table1$advertiser_spend.x <- NULL
table1$sold_impressions.x <- NULL

table2$advertiser_spend.x <- NULL
table2$sold_impressions.x <- NULL

table3$advertiser_spend.x <- NULL
table3$sold_impressions.x <- NULL

table4$advertiser_spend.x <- NULL
table4$sold_impressions.x <- NULL

# rename col names
table1 <- rename(table1, advertiser_spend = curr_week_spend , sold_impressions = curr_week_imps, spend_cpm = spend_cpm.x)
table2 <- rename(table2, advertiser_spend = curr_week_spend , sold_impressions = curr_week_imps, spend_cpm = spend_cpm.x)
table3 <- rename(table3, advertiser_spend = curr_week_spend , sold_impressions = curr_week_imps, spend_cpm = spend_cpm.x)
table4 <- rename(table4, advertiser_spend = curr_week_spend , sold_impressions = curr_week_imps, spend_cpm = spend_cpm.x)

# adjust formatting
table1 <- table1[,c(1,3:4,2,5:10)]
table1[,c(6:7)] <- sapply(table1[,c(6:7)], function(x) percent(x, accuracy = .01))
table1[,c(2,4:5,8:9)] <- sapply(table1[,c(2,4:5,8:9)], dollar)
table1[,3] <- sapply(table1[,3], comma)

table2 <- table2[,c(1,3:4,2,5:10)]
table2[,c(6:7)] <- sapply(table2[,c(6:7)], function(x) percent(x, accuracy = .01))
table2[,c(2,4:5,8:9)] <- sapply(table2[,c(2,4:5,8:9)], dollar)
table2[,3] <- sapply(table2[,3], comma)

table3 <- table3[,c(1,3:4,2,5:10)]
table3[,c(6:7)] <- sapply(table3[,c(6:7)], function(x) percent(x, accuracy = .01))
table3[,c(2,4:5,8:9)] <- sapply(table3[,c(2,4:5,8:9)], dollar)
table3[,3] <- sapply(table3[,3], comma)

table4 <- table4[,c(1,3:4,2,5:10)]
table4[,c(6:7)] <- sapply(table4[,c(6:7)], function(x) percent(x, accuracy = .01))
table4[,c(2,4:5,8:9)] <- sapply(table4[,c(2,4:5,8:9)], dollar)
table4[,3] <- sapply(table4[,3], comma)

# convert tables to html tables
table1_html <- htmlTable(table1, rnames = F)
table2_html <- htmlTable(table2, rnames = F)
table3_html <- htmlTable(table3, rnames = F)
table4_html <- htmlTable(table4, rnames = F)

# create email body
html_body <- paste0("<p><i><b> Top Revenue Generating TE/PMP Advertisers </i></b></p>", table1_html, "<p><i><b> Worst WoW Performing TE/PMP Advertisers </i></b></p>", table2_html, 
                    "<p><i><b> Best WoW Performing TE/PMP Advertisers </i></b></p>", table3_html, "<p><i><b> New Deals/Turned On Brands </i></b></p>", table4_html)


write.csv(table1, "~/r_files/pmp_dash/top_revenue.csv", row.names = F)
write.csv(table2, "~/r_files/pmp_dash/worst_wow.csv", row.names = F)
write.csv(table3, "~/r_files/pmp_dash/best_wow.csv", row.names = F)
write.csv(table4, "~/r_files/pmp_dash/new_deals.csv", row.names = F)

write.csv(prior_four_weeks, "~/r_files/pmp_dash/prior_four_weeks.csv", row.names = F)
write.csv(deal_id_data, "~/r_files/pmp_dash/deal_id_data.csv", row.names = F)
write.csv(rev_data, "~/r_files/pmp_dash/rev_data.csv", row.names = F)

# send out email
# tmpdir <-  "C:/Users/deven.choi/Documents/"
# tmpdir <- gsub("\\\\", "/", tmpdir)

send.mail(from = "Deven Choi <deven.choi@openx.com>",
          to =  c("deven.choi@openx.com", "eric.silverstein@openx.com", "john.sattari@openx.com", "usbuyerdev@openx.com",
                  "tish.whitcraft@openx.c"),
          subject = paste0("PMP Dash ", Sys.Date()),
          body = html_body,
          html = TRUE,
          smtp = list(host.name = "smtp.gmail.com", port = 465,
                      ssl=TRUE, user.name = "deven.choi@openx.com",
                      passwd = "password"),
          authenticate = TRUE,
          send = TRUE
)  

