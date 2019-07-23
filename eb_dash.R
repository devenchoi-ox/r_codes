# eb dash
library(tidyr)
library(dplyr)
library(scales)

# import exchange name mapping file
name_mapping <- read.csv("C:/Users/deven.choi/Desktop/ebdash/exchange_names.csv", stringsAsFactors = FALSE)
name_mapping <- unique(name_mapping)
name_mapping$Yield.partner <- tolower(name_mapping$Yield.partner)

# import pub name mapping
dfp_mapping <- read.csv("C:/Users/deven.choi/Desktop/ebdash/dfp_names.csv", stringsAsFactors = FALSE)
dfp_mapping <- unique(dfp_mapping)
dfp_mapping$DFP <- tolower(dfp_mapping$DFP)

# get the count of files
file_list <- list.files("C:/Users/deven.choi/Desktop/ebdash/data_files", full.names = TRUE)
num_files <- length(file_list)

# get column names
col_names <- read.csv(file_list[1], stringsAsFactors = FALSE, skip = 8, nrows = 1)
col_names <- colnames(col_names)

# get total number of columns
col_length <- length(col_names)

# initialize data frame with column names
data <- data.frame(matrix(ncol = length(col_names), nrow = 0))

# loop through and append data
for(i in 1:num_files)
{
    # pull publisher name
    pub_names <- read.csv(file_list[i], stringsAsFactors = FALSE)
    pub_names <- pub_names[2,2]

    # skip headers
    a <- read.csv(file_list[i], stringsAsFactors = FALSE, skip = 8)
    
    # remove commas and convert to numeric class
    a[] <- sapply(a, function(x) gsub(",","", x))
    a[,3:10] <- sapply(a[,3:10], as.numeric)
    
    # make column names uniform to account for different names
    colnames(a) <- c(1:col_length)
    
    # add in column for pub name
    a$pub_name <- pub_names
    
    # remove last row to remove Totals
    a <- a[-nrow(a),]
    
    # append the new data
    data <- rbind(data, a)
}

# add original column names back in along with pub name and exchange partner
colnames(data) <- c(col_names, "DFP")
data$Yield.partner <- tolower(data$Yield.partner)
data$DFP <- tolower(data$DFP)

data <- left_join(data, name_mapping, by = "Yield.partner")
data <- left_join(data, dfp_mapping, by = "DFP")

data$DFP <- NULL

# overall summary data
overall <- data %>%
    group_by (Publisher, Exchange, Date) %>%
    dplyr::summarize(Callouts = sum(Yield.group.callouts, na.rm = TRUE),
              Successful_responses = sum(Yield.group.successful.responses, na.rm = TRUE),
              Bids = sum(Yield.group.bids, na.rm = TRUE),
              Bids_in_auction = sum(Yield.group.bids.in.auction, na.rm = TRUE),
              Auctions_won = sum(Yield.group.auctions.won, na.rm = TRUE),
              Impressions = sum(Yield.group.impressions, na.rm = TRUE),
              Bid_rate = sum(Yield.group.bids, na.rm = TRUE)/sum(Yield.group.callouts, na.rm = TRUE),
              Bia_rate = sum(Yield.group.bids.in.auction, na.rm = TRUE)/sum(Yield.group.bids, na.rm = TRUE),
              Win_rate = sum(Yield.group.impressions, na.rm = TRUE)/sum(Yield.group.bids.in.auction, na.rm = TRUE),
              Revenue = sum(Yield.group.estimated.revenue...., na.rm = TRUE),
              CPM = sum(Yield.group.estimated.CPM...., na.rm = TRUE),
              Render_rate = sum(Yield.group.impressions, na.rm = TRUE)/sum(Yield.group.auctions.won, na.rm = TRUE),
              Auction_win_rate = sum(Yield.group.auctions.won, na.rm = TRUE)/sum(Yield.group.bids.in.auction, na.rm = TRUE))

# revenue only data
revenue <- data %>%
    group_by (Publisher, Exchange, Date) %>%
    dplyr::summarize(Revenue = sum(Yield.group.estimated.revenue...., na.rm = TRUE))

# clean up date
overall$Date <- as.Date(overall$Date, "%m/%d/%y")
revenue$Date <- as.Date(revenue$Date, "%m/%d/%y")

overall_dt <- overall

# clean up formatting for overall data
overall_dt[,c(10:12, 15:16)] <- sapply(overall_dt[,c(10:12, 15:16)], function(x) percent(x, accuracy = .01))
overall_dt[,13:14] <- sapply(overall_dt[,13:14], dollar)
overall_dt[,4:9] <- sapply(overall_dt[,4:9], comma)

# spread revenue data by exchange and format
revenue_wide <- spread(revenue, Exchange, Revenue)
revenue_wide[is.na(revenue_wide)] <- 0
revenue_wide$total_revenue <-  rowSums(revenue_wide[, c(3:ncol(revenue_wide))], na.rm = TRUE)

# show revenue as % of total
z <- ncol(revenue_wide) - 1
revenue_wide[,3:z] <- sapply(revenue_wide[,3:z], function(x) x/revenue_wide$total_revenue)
revenue_wide$total_revenue <- NULL
revenue_wide[,3:ncol(revenue_wide)] <- sapply(revenue_wide[,3:ncol(revenue_wide)], percent)

# export data
write.csv(overall, "C:/Users/deven.choi/Desktop/ebdash/overall_eb_data.csv", row.names = FALSE)
write.csv(overall_dt, "C:/Users/deven.choi/Desktop/ebdash/overall_dt_eb_data.csv", row.names = FALSE)
write.csv(revenue_wide, "C:/Users/deven.choi/Desktop/ebdash/revenue_eb_data.csv", row.names = FALSE)

rm(list=ls())