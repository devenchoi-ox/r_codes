source("~/r_files/deven_onboarding-master/reqfiles/snowflake.R")

# load libraries
library(dplyr)
library(scales)
library(tidyr)
library(caret)
library(randomForest)
library(Hmisc)
library(doParallel)
library(foreach)
library(e1071)
library(forecast)
library(ROCR)
library(lubridate)

# threshold testing percentage
threshold_number <- .90

# query to pull initial data set
query <- "
select 
utc_month_sid
,b.account_name as sf_account_name
,a.site_nk
,b.site_name as site_name
,case when p_mobl_app_bundle is not null then 1 else 0 end as app_flag
,case when p_mapped_adunit_type = 'VIDEO' then 1 else 0 end as video_flag
,case when b.business_client = 'google-bidout' then 1 else 0 end as eb_flag
,case when b.site_name like ('%A9%') then 1 else 0 end as a9_flag
,case when x_bidout = true then 1 else 0 end as bidout_flag
,case when x_bidder_elig = true then 1 else 0 end as bidder_flag
,sum(case when x_price_won = '1' then tot_usd_a_spend else 0 end) as tot_fpa_spend
,sum(tot_usd_a_spend) as tot_usd_a_spend
,sum(case when a.advertiser_account_nk = '537073246' then tot_usd_a_spend else 0 end) as tot_dbm_spend_usd
,sum(case when a.advertiser_account_nk = '537073292' then tot_usd_a_spend else 0 end) as tot_ttd_spend_usd
,sum(case when a.advertiser_account_nk = '537148859' then tot_usd_a_spend else 0 end) as tot_adobe_spend_usd
,sum(case when a.advertiser_account_nk = '537073301' then tot_usd_a_spend else 0 end) as tot_oath_spend_usd
,sum(case when a.advertiser_account_nk = '537073294' then tot_usd_a_spend else 0 end) as tot_amobee_spend_usd
,sum(case when a.advertiser_account_nk = '537073399' then tot_usd_a_spend else 0 end) as tot_mm_spend_usd
,sum(case when a.advertiser_account_nk = '537073219' then tot_usd_a_spend else 0 end) as tot_a9_spend_usd
,sum(case when a.advertiser_account_nk = '537073401' then tot_usd_a_spend else 0 end) as tot_criteo_spend_usd
,sum(case when a.advertiser_account_nk = '537073286' then tot_usd_a_spend else 0 end) as tot_smplfi_spend_usd
--,sum(case when a.advertiser_account_nk = '537073283' then tot_usd_a_spend else 0 end) as tot_sizmek_spend_usd
,sum(case when a.advertiser_account_nk = '537073287' then tot_usd_a_spend else 0 end) as tot_basis_spend_usd
,sum(case when a.advertiser_account_nk = '537105068' then tot_usd_a_spend else 0 end) as tot_adroll_spend_usd
,sum(case when a.advertiser_account_nk = '537125689' then tot_usd_a_spend else 0 end) as tot_beeswax_spend_usd
,sum(case when a.advertiser_account_nk = '537073277' then tot_usd_a_spend else 0 end) as tot_quantcast_spend_usd
,sum(case when a.advertiser_account_nk = '537073241' then tot_usd_a_spend else 0 end) as tot_dataxu_spend_usd
,sum(case when a.advertiser_account_nk = '537113485' then tot_usd_a_spend else 0 end) as tot_adform_spend_usd
,sum(case when a.advertiser_account_nk = '537077955' then tot_usd_a_spend else 0 end) as tot_adtheorent_spend_usd
,sum(case when a.advertiser_account_nk = '537073256' then tot_usd_a_spend else 0 end) as tot_bidswitch_spend_usd
,sum(case when a.advertiser_account_nk = '537073245' then tot_usd_a_spend else 0 end) as tot_conversant_spend_usd
,sum(case when a.advertiser_account_nk = '537139443' then tot_usd_a_spend else 0 end) as tot_liftoff_spend_usd

from mstr_datamart.ox_transaction_sum_monthly_fact as a
left join mstr_datamart.dim_sites_to_owners b on a.site_nk = b.site_nk
--left join mstr_datamart.advertiser_dim c on a.advertiser_account_nk = c.advertiser_account_nk

where utc_month_sid = (date) --'20190601'
and is_mkt = TRUE

group by 1,2,3,4,5,6,7,8,9,10 having sum(tot_usd_a_spend) > 10
order by 12 desc;"

# get last month
last_month <- format(floor_date(Sys.Date(), "month") - months(1), "%Y%m%d")
query <- gsub("date", last_month, query)

data <- dbGetQuery(snowflake, query)
names(data) <- tolower(names(data))

# network categorization for publishers
network_list <- read.csv("C:/Users/deven.choi/Desktop/network_list.csv", stringsAsFactors = F)
names(network_list) <- tolower(names(network_list))

# add in network flag
overall <- left_join(data, network_list, by = "sf_account_name")
overall$network_flag <- ifelse(overall$network_direct == "network", 1, 0)

# clean up data and move columns around
overall$sf_account_id <- NULL
overall$network_direct <- NULL
overall$utc_month_sid <- NULL
overall$sf_account_name <- NULL
overall <- overall[,c(1:8,30,9:29)]
overall$network_flag[is.na(overall$network_flag)] <- 0

#rm(data, network_list)

#overall$site_name <- NULL

#create dsp specific spend flags
overall$dbm_spend_flag <- ifelse(overall$tot_dbm_spend_usd > 0, 0, 1)
overall$ttd_spend_flag <- ifelse(overall$tot_ttd_spend_usd > 0, 0, 1)
overall$adobe_spend_flag <- ifelse(overall$tot_adobe_spend_usd > 0, 0, 1)
overall$oath_spend_flag <- ifelse(overall$tot_oath_spend_usd > 0, 0, 1)
overall$amobee_spend_flag <- ifelse(overall$tot_amobee_spend_usd > 0, 0, 1)
overall$mm_spend_flag <- ifelse(overall$tot_mm_spend_usd > 0, 0, 1)
overall$a9_spend_flag <- ifelse(overall$tot_a9_spend_usd > 0, 0, 1)
overall$criteo_spend_flag <- ifelse(overall$tot_criteo_spend_usd > 0, 0, 1)
overall$smplfi_spend_flag <- ifelse(overall$tot_smplfi_spend_usd > 0, 0, 1)
#overall$sizmek_spend_flag <- ifelse(overall$tot_sizmek_spend_usd > 0, 0, 1)
overall$basis_spend_flag <- ifelse(overall$tot_basis_spend_usd > 0, 0, 1)
overall$adroll_spend_flag <- ifelse(overall$tot_adroll_spend_usd > 0, 0, 1)
overall$beeswax_spend_flag <- ifelse(overall$tot_beeswax_spend_usd > 0, 0, 1)
overall$quantcast_spend_flag <- ifelse(overall$tot_quantcast_spend_usd > 0, 0, 1)
overall$dataxu_spend_flag <- ifelse(overall$tot_dataxu_spend_usd > 0, 0, 1)
overall$adform_spend_flag <- ifelse(overall$tot_adform_spend_usd > 0, 0, 1)
overall$adtheorent_spend_flag <- ifelse(overall$tot_adtheorent_spend_usd > 0, 0, 1)
overall$bidswitch_spend_flag <- ifelse(overall$tot_bidswitch_spend_usd > 0, 0, 1)
overall$conversant_spend_flag <- ifelse(overall$tot_conversant_spend_usd > 0, 0, 1)
overall$liftoff_spend_flag <- ifelse(overall$tot_liftoff_spend_usd > 0, 0, 1)

# scale spend numbers for better fit with model
overall$tot_usd_a_spend <- (overall$tot_usd_a_spend - mean(overall$tot_usd_a_spend))/sd(overall$tot_usd_a_spend)
overall$tot_fpa_spend <- (overall$tot_fpa_spend - mean(overall$tot_fpa_spend))/sd(overall$tot_fpa_spend)
overall$tot_dbm_spend_usd <- (overall$tot_dbm_spend_usd - mean(overall$tot_dbm_spend_usd))/sd(overall$tot_dbm_spend_usd)
overall$tot_ttd_spend_usd <- (overall$tot_ttd_spend_usd - mean(overall$tot_ttd_spend_usd))/sd(overall$tot_ttd_spend_usd)
overall$tot_adobe_spend_usd <- (overall$tot_adobe_spend_usd - mean(overall$tot_adobe_spend_usd))/sd(overall$tot_adobe_spend_usd)
overall$tot_oath_spend_usd <- (overall$tot_oath_spend_usd - mean(overall$tot_oath_spend_usd))/sd(overall$tot_oath_spend_usd)
overall$tot_amobee_spend_usd <- (overall$tot_amobee_spend_usd - mean(overall$tot_amobee_spend_usd))/sd(overall$tot_amobee_spend_usd)
overall$tot_mm_spend_usd <- (overall$tot_mm_spend_usd - mean(overall$tot_mm_spend_usd))/sd(overall$tot_mm_spend_usd)
overall$tot_a9_spend_usd <- (overall$tot_a9_spend_usd - mean(overall$tot_a9_spend_usd))/sd(overall$tot_a9_spend_usd)
overall$tot_criteo_spend_usd <- (overall$tot_criteo_spend_usd - mean(overall$tot_criteo_spend_usd))/sd(overall$tot_criteo_spend_usd)
overall$tot_smplfi_spend_usd <- (overall$tot_smplfi_spend_usd - mean(overall$tot_smplfi_spend_usd))/sd(overall$tot_smplfi_spend_usd)
#overall$tot_sizmek_spend_usd <- (overall$tot_sizmek_spend_usd - mean(overall$tot_sizmek_spend_usd))/sd(overall$tot_sizmek_spend_usd)
overall$tot_basis_spend_usd <- (overall$tot_basis_spend_usd - mean(overall$tot_basis_spend_usd))/sd(overall$tot_basis_spend_usd)
overall$tot_adroll_spend_usd <- (overall$tot_adroll_spend_usd - mean(overall$tot_adroll_spend_usd))/sd(overall$tot_adroll_spend_usd)
overall$tot_beeswax_spend_usd <- (overall$tot_beeswax_spend_usd - mean(overall$tot_beeswax_spend_usd))/sd(overall$tot_beeswax_spend_usd)
overall$tot_quantcast_spend_usd <- (overall$tot_quantcast_spend_usd - mean(overall$tot_quantcast_spend_usd))/sd(overall$tot_quantcast_spend_usd)
overall$tot_dataxu_spend_usd <- (overall$tot_dataxu_spend_usd - mean(overall$tot_dataxu_spend_usd))/sd(overall$tot_dataxu_spend_usd)
overall$tot_adform_spend_usd <- (overall$tot_adform_spend_usd - mean(overall$tot_adform_spend_usd))/sd(overall$tot_adform_spend_usd)
overall$tot_adtheorent_spend_usd <- (overall$tot_adtheorent_spend_usd - mean(overall$tot_adtheorent_spend_usd))/sd(overall$tot_adtheorent_spend_usd)
overall$tot_bidswitch_spend_usd <- (overall$tot_bidswitch_spend_usd - mean(overall$tot_bidswitch_spend_usd))/sd(overall$tot_bidswitch_spend_usd)
overall$tot_conversant_spend_usd <- (overall$tot_conversant_spend_usd - mean(overall$tot_conversant_spend_usd))/sd(overall$tot_conversant_spend_usd)
overall$tot_liftoff_spend_usd <- (overall$tot_liftoff_spend_usd - mean(overall$tot_liftoff_spend_usd))/sd(overall$tot_liftoff_spend_usd)

#########################################
# training the model
#########################################

#####################
# Adobe training data
#####################

adobe <- overall

# remove Adobe spend numbers
adobe$tot_adobe_spend_usd <- NULL

# temp remove site name
adobe$site_name <- NULL
adobe$site_nk <- NULL

# create training and testing data sets
set.seed(123456)
training_model <- createDataPartition(y = adobe$adobe_spend_flag, p=0.6, list=FALSE)

training <- adobe[training_model,]
testing <- adobe[-training_model,]

# output dimension counts to make sure they match
dim(training); dim(testing)

set.seed(123456)
registerDoParallel()

# random forest model
col_to_remove <- "adobe_spend_flag"

x <- training[, !(colnames(training) %in% col_to_remove)]
y <- training$adobe_spend_flag

rf <- foreach(ntree = rep(40, 6), .combine = randomForest::combine, .packages = 'randomForest') %dopar% {
    randomForest(x, y, ntree = ntree)}

# training RMSE
training$predictions <- round(predict(rf, newdata = training), 2)
adobe_training_accuracy <- accuracy(training$predictions, training$adobe_spend_flag)

# testing RMSE
testing$predictions <- round(predict(rf, newdata = testing), 2)
adobe_testing_accuracy <- accuracy(testing$predictions, testing$adobe_spend_flag)

# gbm model
#gbm <- train(y ~ ., method = "gbm",data=training)

# ROC Curve Plot
pred <- prediction(testing$predictions, testing$adobe_spend_flag)
perf <- performance(pred, "tpr", "fpr" )

png(filename = "~/r_files/spo_machine_learning_models/plot_outputs/adobe roc plot v1.png", width = 480, height = 480, units = "px", bg = "white")
plot(perf, main = 'Adobe ROC Plot', col = 'green', lwd = 3)
abline(0,1)
dev.off()

adobe_tmp <- adobe
adobe_tmp$spo_probability <- round(predict(rf, newdata = adobe_tmp), 2)

# put site name back in
site_names <- overall
site_names <- site_names[,c(1:2)]

adobe_tmp <- cbind(site_names, adobe_tmp)

# threshold test
adobe_threshold_test <- adobe_tmp
adobe_threshold_test <- adobe_threshold_test[which(adobe_threshold_test$spo_probability > threshold_number), ]
adobe_threshold_test$count <- 1
adobe_test <- sum(adobe_threshold_test$adobe_spend_flag) / sum(adobe_threshold_test$count)

# save RF model 
saveRDS(rf, file = "~/r_files/spo_machine_learning_models/rf_rds_files/rf_adobe.RDS")

#rm(adove_temp, site_names, adobe_threshold_test)

####################
# DBM training data
####################

dbm <- overall

# remove DBM spend numbers
dbm$tot_dbm_spend_usd <- NULL

# temp remove site name
dbm$site_name <- NULL
dbm$site_nk <- NULL

# create training and testing data sets
set.seed(123456)
training_model <- createDataPartition(y = dbm$dbm_spend_flag, p=0.6, list=FALSE)

training <- dbm[training_model,]
testing <- dbm[-training_model,]

# output dimension counts to make sure they match
dim(training); dim(testing)

set.seed(123456)
registerDoParallel()

# random forest model
col_to_remove <- "dbm_spend_flag"

x <- training[, !(colnames(training) %in% col_to_remove)]
y <- training$dbm_spend_flag

rf <- foreach(ntree = rep(40, 6), .combine = randomForest::combine, .packages = 'randomForest') %dopar% {
    randomForest(x, y, ntree = ntree)}

# training RMSE
training$predictions <- round(predict(rf, newdata = training), 2)
dbm_training_accuracy <- accuracy(training$predictions, training$dbm_spend_flag)

# testing RMSE
testing$predictions <- round(predict(rf, newdata = testing), 2)
dbm_testing_accuracy <- accuracy(testing$predictions, testing$dbm_spend_flag)

# gbm model
#gbm <- train(y ~ ., method = "gbm",data=training)

# ROC Curve Plot
pred <- prediction(testing$predictions, testing$dbm_spend_flag)
perf <- performance(pred, "tpr", "fpr" )

png(filename = "~/r_files/spo_machine_learning_models/plot_outputs/DBM roc plot v1.png", width = 480, height = 480, units = "px", bg = "white")
plot(perf, main = 'DBM ROC Plot', col = 'green', lwd = 3)
abline(0,1)
dev.off()

dbm_tmp <- dbm
dbm_tmp$spo_probability <- round(predict(rf, newdata = dbm_tmp), 2)

# put site name back in
site_names <- overall
site_names <- site_names[,c(1:2)]

dbm_tmp <- cbind(site_names, dbm_tmp)

# threshold test
dbm_threshold_test <- dbm_tmp
dbm_threshold_test <- dbm_threshold_test[which(dbm_threshold_test$spo_probability > threshold_number), ]
dbm_threshold_test$count <- 1
dbm_test <- sum(dbm_threshold_test$dbm_spend_flag) / sum(dbm_threshold_test$count)

# save RF model 
saveRDS(rf, file = "~/r_files/spo_machine_learning_models/rf_rds_files/rf_dbm.RDS")


####################
# TTD training data
####################

ttd <- overall

# remove TTD spend numbers
ttd$tot_ttd_spend_usd <- NULL

# temp remove site name
ttd$site_name <- NULL
ttd$site_nk <- NULL

# create training and testing data sets
set.seed(123456)
training_model <- createDataPartition(y = ttd$ttd_spend_flag, p=0.6, list=FALSE)

training <- ttd[training_model,]
testing <- ttd[-training_model,]

# output dimension counts to make sure they match
dim(training); dim(testing)

set.seed(123456)
registerDoParallel()

# random forest model
col_to_remove <- "ttd_spend_flag"

x <- training[, !(colnames(training) %in% col_to_remove)]
y <- training$ttd_spend_flag

rf <- foreach(ntree = rep(40, 6), .combine = randomForest::combine, .packages = 'randomForest') %dopar% {
    randomForest(x, y, ntree = ntree)}

# training RMSE
training$predictions <- round(predict(rf, newdata = training), 2)
ttd_training_accuracy <- accuracy(training$predictions, training$ttd_spend_flag)

# testing RMSE
testing$predictions <- round(predict(rf, newdata = testing), 2)
ttd_testing_accuracy <- accuracy(testing$predictions, testing$ttd_spend_flag)

# gbm model
#gbm <- train(y ~ ., method = "gbm",data=training)

# ROC Curve Plot
pred <- prediction(testing$predictions, testing$ttd_spend_flag)
perf <- performance(pred, "tpr", "fpr" )

png(filename = "~/r_files/spo_machine_learning_models/plot_outputs/TTD roc plot v1.png", width = 480, height = 480, units = "px", bg = "white")
plot(perf, main = 'TTD ROC Plot', col = 'green', lwd = 3)
abline(0,1)
dev.off()

ttd_tmp <- ttd
ttd_tmp$spo_probability <- round(predict(rf, newdata = ttd_tmp), 2)

# put site name back in
site_names <- overall
site_names <- site_names[,c(1:2)]

ttd_tmp <- cbind(site_names, ttd_tmp)

# threshold test
ttd_threshold_test <- ttd_tmp
ttd_threshold_test <- ttd_threshold_test[which(ttd_threshold_test$spo_probability > threshold_number), ]
ttd_threshold_test$count <- 1
ttd_test <- sum(ttd_threshold_test$ttd_spend_flag) / sum(ttd_threshold_test$count)

# save RF model 
saveRDS(rf, file = "~/r_files/spo_machine_learning_models/rf_rds_files/rf_ttd.RDS")

####################
# Oath training data
####################

oath <- overall

# remove Oath spend numbers
oath$tot_oath_spend_usd <- NULL

# temp remove site name
oath$site_name <- NULL
oath$site_nk <- NULL

# create training and testing data sets
set.seed(123456)
training_model <- createDataPartition(y = oath$oath_spend_flag, p=0.6, list=FALSE)

training <- oath[training_model,]
testing <- oath[-training_model,]

# output dimension counts to make sure they match
dim(training); dim(testing)

set.seed(123456)
registerDoParallel()

# random forest model
col_to_remove <- "oath_spend_flag"

x <- training[, !(colnames(training) %in% col_to_remove)]
y <- training$oath_spend_flag

rf <- foreach(ntree = rep(40, 6), .combine = randomForest::combine, .packages = 'randomForest') %dopar% {
    randomForest(x, y, ntree = ntree)}

# training RMSE
training$predictions <- round(predict(rf, newdata = training), 2)
oath_training_accuracy <- accuracy(training$predictions, training$oath_spend_flag)

# testing RMSE
testing$predictions <- round(predict(rf, newdata = testing), 2)
oath_testing_accuracy <- accuracy(testing$predictions, testing$oath_spend_flag)

# gbm model
#gbm <- train(y ~ ., method = "gbm",data=training)

# ROC Curve Plot
pred <- prediction(testing$predictions, testing$oath_spend_flag)
perf <- performance(pred, "tpr", "fpr" )

png(filename = "~/r_files/spo_machine_learning_models/plot_outputs/Oath roc plot v1.png", width = 480, height = 480, units = "px", bg = "white")
plot(perf, main = 'Oath ROC Plot', col = 'green', lwd = 3)
abline(0,1)
dev.off()

oath_tmp <- oath
oath_tmp$spo_probability <- round(predict(rf, newdata = oath_tmp), 2)

# put site name back in
site_names <- overall
site_names <- site_names[,c(1:2)]

oath_tmp <- cbind(site_names, oath_tmp)

# threshold test
oath_threshold_test <- oath_tmp
oath_threshold_test <- oath_threshold_test[which(oath_threshold_test$spo_probability > threshold_number), ]
oath_threshold_test$count <- 1
oath_test <- sum(oath_threshold_test$oath_spend_flag) / sum(oath_threshold_test$count)

# save RF model 
saveRDS(rf, file = "~/r_files/spo_machine_learning_models/rf_rds_files/rf_oath.RDS")

####################
# Amobee training data
####################

amobee <- overall

# remove Amobee spend numbers
amobee$tot_amobee_spend_usd <- NULL

# temp remove site name
amobee$site_name <- NULL
amobee$site_nk <- NULL

# create training and testing data sets
set.seed(123456)
training_model <- createDataPartition(y = amobee$amobee_spend_flag, p=0.6, list=FALSE)

training <- amobee[training_model,]
testing <- amobee[-training_model,]

# output dimension counts to make sure they match
dim(training); dim(testing)

set.seed(123456)
registerDoParallel()

# random forest model
col_to_remove <- "amobee_spend_flag"

x <- training[, !(colnames(training) %in% col_to_remove)]
y <- training$amobee_spend_flag

rf <- foreach(ntree = rep(40, 6), .combine = randomForest::combine, .packages = 'randomForest') %dopar% {
    randomForest(x, y, ntree = ntree)}

# training RMSE
training$predictions <- round(predict(rf, newdata = training), 2)
amobee_training_accuracy <- accuracy(training$predictions, training$amobee_spend_flag)

# testing RMSE
testing$predictions <- round(predict(rf, newdata = testing), 2)
amobee_testing_accuracy <- accuracy(testing$predictions, testing$amobee_spend_flag)

# gbm model
#gbm <- train(y ~ ., method = "gbm",data=training)

# ROC Curve Plot
pred <- prediction(testing$predictions, testing$amobee_spend_flag)
perf <- performance(pred, "tpr", "fpr" )

png(filename = "~/r_files/spo_machine_learning_models/plot_outputs/Amobee roc plot v1.png", width = 480, height = 480, units = "px", bg = "white")
plot(perf, main = 'Amobee ROC Plot', col = 'green', lwd = 3)
abline(0,1)
dev.off()

amobee_tmp <- amobee
amobee_tmp$spo_probability <- round(predict(rf, newdata = amobee_tmp), 2)

# put site name back in
site_names <- overall
site_names <- site_names[,c(1:2)]

amobee_tmp <- cbind(site_names, amobee_tmp)

# threshold test
amobee_threshold_test <- amobee_tmp
amobee_threshold_test <- amobee_threshold_test[which(amobee_threshold_test$spo_probability > threshold_number), ]
amobee_threshold_test$count <- 1
amobee_test <- sum(amobee_threshold_test$amobee_spend_flag) / sum(amobee_threshold_test$count)

# save RF model 
saveRDS(rf, file = "~/r_files/spo_machine_learning_models/rf_rds_files/rf_amobee.RDS")

####################
# MediaMath training data
####################

mm <- overall

# remove MediaMath spend numbers
mm$tot_mm_spend_usd <- NULL

# temp remove site name
mm$site_name <- NULL
mm$site_nk <- NULL

# create training and testing data sets
set.seed(123456)
training_model <- createDataPartition(y = mm$mm_spend_flag, p=0.6, list=FALSE)

training <- mm[training_model,]
testing <- mm[-training_model,]

# output dimension counts to make sure they match
dim(training); dim(testing)

set.seed(123456)
registerDoParallel()

# random forest model
col_to_remove <- "mm_spend_flag"

x <- training[, !(colnames(training) %in% col_to_remove)]
y <- training$mm_spend_flag

rf <- foreach(ntree = rep(40, 6), .combine = randomForest::combine, .packages = 'randomForest') %dopar% {
    randomForest(x, y, ntree = ntree)}

# training RMSE
training$predictions <- round(predict(rf, newdata = training), 2)
mm_training_accuracy <- accuracy(training$predictions, training$mm_spend_flag)

# testing RMSE
testing$predictions <- round(predict(rf, newdata = testing), 2)
mm_testing_accuracy <- accuracy(testing$predictions, testing$mm_spend_flag)

# gbm model
#gbm <- train(y ~ ., method = "gbm",data=training)

# ROC Curve Plot
pred <- prediction(testing$predictions, testing$mm_spend_flag)
perf <- performance(pred, "tpr", "fpr" )

png(filename = "~/r_files/spo_machine_learning_models/plot_outputs/mm roc plot v1.png", width = 480, height = 480, units = "px", bg = "white")
plot(perf, main = 'Media Math ROC Plot', col = 'green', lwd = 3)
abline(0,1)
dev.off()

mm_tmp <- mm
mm_tmp$spo_probability <- round(predict(rf, newdata = mm_tmp), 2)

# put site name back in
site_names <- overall
site_names <- site_names[,c(1:2)]

mm_tmp <- cbind(site_names, mm_tmp)

# threshold test
mm_threshold_test <- mm_tmp
mm_threshold_test <- mm_threshold_test[which(mm_threshold_test$spo_probability > threshold_number), ]
mm_threshold_test$count <- 1
mm_test <- sum(mm_threshold_test$mm_spend_flag) / sum(mm_threshold_test$count)

# save RF model 
saveRDS(rf, file = "~/r_files/spo_machine_learning_models/rf_rds_files/rf_mm.RDS")

####################
# A9 training data
####################

a9 <- overall

# remove A9 spend numbers
a9$tot_a9_spend_usd <- NULL

# temp remove site name
a9$site_name <- NULL
a9$site_nk <- NULL

# create training and testing data sets
set.seed(123456)
training_model <- createDataPartition(y = a9$a9_spend_flag, p=0.6, list=FALSE)

training <- a9[training_model,]
testing <- a9[-training_model,]

# output dimension counts to make sure they match
dim(training); dim(testing)

set.seed(123456)
registerDoParallel()

# random forest model
col_to_remove <- "a9_spend_flag"

x <- training[, !(colnames(training) %in% col_to_remove)]
y <- training$a9_spend_flag

rf <- foreach(ntree = rep(40, 6), .combine = randomForest::combine, .packages = 'randomForest') %dopar% {
    randomForest(x, y, ntree = ntree)}

# training RMSE
training$predictions <- round(predict(rf, newdata = training), 2)
a9_training_accuracy <- accuracy(training$predictions, training$a9_spend_flag)

# testing RMSE
testing$predictions <- round(predict(rf, newdata = testing), 2)
a9_testing_accuracy <- accuracy(testing$predictions, testing$a9_spend_flag)

# gbm model
#gbm <- train(y ~ ., method = "gbm",data=training)

# ROC Curve Plot
pred <- prediction(testing$predictions, testing$a9_spend_flag)
perf <- performance(pred, "tpr", "fpr" )

png(filename = "~/r_files/spo_machine_learning_models/plot_outputs/A9 roc plot v1.png", width = 480, height = 480, units = "px", bg = "white")
plot(perf, main = 'A9 ROC Plot', col = 'green', lwd = 3)
abline(0,1)
dev.off()

a9_tmp <- a9
a9_tmp$spo_probability <- round(predict(rf, newdata = a9_tmp), 2)

# put site name back in
site_names <- overall
site_names <- site_names[,c(1:2)]

a9_tmp <- cbind(site_names, a9_tmp)

# threshold test
a9_threshold_test <- a9_tmp
a9_threshold_test <- a9_threshold_test[which(a9_threshold_test$spo_probability > threshold_number), ]
a9_threshold_test$count <- 1
a9_test <- sum(a9_threshold_test$a9_spend_flag) / sum(a9_threshold_test$count)

# save RF model 
saveRDS(rf, file = "~/r_files/spo_machine_learning_models/rf_rds_files/rf_a9.RDS")

####################
# Criteo training data
####################

criteo <- overall

# remove Criteo spend numbers
criteo$tot_criteo_spend_usd <- NULL

# temp remove site name
criteo$site_name <- NULL
criteo$site_nk <- NULL

# create training and testing data sets
set.seed(123456)
training_model <- createDataPartition(y = criteo$criteo_spend_flag, p=0.6, list=FALSE)

training <- criteo[training_model,]
testing <- criteo[-training_model,]

# output dimension counts to make sure they match
dim(training); dim(testing)

set.seed(123456)
registerDoParallel()

# random forest model
col_to_remove <- "criteo_spend_flag"

x <- training[, !(colnames(training) %in% col_to_remove)]
y <- training$criteo_spend_flag

rf <- foreach(ntree = rep(40, 6), .combine = randomForest::combine, .packages = 'randomForest') %dopar% {
    randomForest(x, y, ntree = ntree)}

# training RMSE
training$predictions <- round(predict(rf, newdata = training), 2)
criteo_training_accuracy <- accuracy(training$predictions, training$criteo_spend_flag)

# testing RMSE
testing$predictions <- round(predict(rf, newdata = testing), 2)
criteo_testing_accuracy <- accuracy(testing$predictions, testing$criteo_spend_flag)

# gbm model
#gbm <- train(y ~ ., method = "gbm",data=training)

# ROC Curve Plot
pred <- prediction(testing$predictions, testing$criteo_spend_flag)
perf <- performance(pred, "tpr", "fpr" )

png(filename = "~/r_files/spo_machine_learning_models/plot_outputs/Criteo roc plot v1.png", width = 480, height = 480, units = "px", bg = "white")
plot(perf, main = 'Criteo ROC Plot', col = 'green', lwd = 3)
abline(0,1)
dev.off()

criteo_tmp <- criteo
criteo_tmp$spo_probability <- round(predict(rf, newdata = criteo_tmp), 2)

# put site name back in
site_names <- overall
site_names <- site_names[,c(1:2)]

criteo_tmp <- cbind(site_names, criteo_tmp)

# threshold test
criteo_threshold_test <- criteo_tmp
criteo_threshold_test <- criteo_threshold_test[which(criteo_threshold_test$spo_probability > threshold_number), ]
criteo_threshold_test$count <- 1
criteo_test <- sum(criteo_threshold_test$criteo_spend_flag) / sum(criteo_threshold_test$count)

# save RF model 
saveRDS(rf, file = "~/r_files/spo_machine_learning_models/rf_rds_files/rf_criteo.RDS")

####################
# SimpliFi training data
####################

smplfi <- overall

# remove SimpliFi spend numbers
smplfi$tot_smplfi_spend_usd <- NULL

# temp remove site name
smplfi$site_name <- NULL
smplfi$site_nk <- NULL

# create training and testing data sets
set.seed(123456)
training_model <- createDataPartition(y = smplfi$smplfi_spend_flag, p=0.6, list=FALSE)

training <- smplfi[training_model,]
testing <- smplfi[-training_model,]

# output dimension counts to make sure they match
dim(training); dim(testing)

set.seed(123456)
registerDoParallel()

# random forest model
col_to_remove <- "smplfi_spend_flag"

x <- training[, !(colnames(training) %in% col_to_remove)]
y <- training$smplfi_spend_flag

rf <- foreach(ntree = rep(40, 6), .combine = randomForest::combine, .packages = 'randomForest') %dopar% {
    randomForest(x, y, ntree = ntree)}

# training RMSE
training$predictions <- round(predict(rf, newdata = training), 2)
smplfi_training_accuracy <- accuracy(training$predictions, training$smplfi_spend_flag)

# testing RMSE
testing$predictions <- round(predict(rf, newdata = testing), 2)
smplfi_testing_accuracy <- accuracy(testing$predictions, testing$smplfi_spend_flag)

# gbm model
#gbm <- train(y ~ ., method = "gbm",data=training)

# ROC Curve Plot
pred <- prediction(testing$predictions, testing$smplfi_spend_flag)
perf <- performance(pred, "tpr", "fpr" )

png(filename = "~/r_files/spo_machine_learning_models/plot_outputs/Simplifi roc plot v1.png", width = 480, height = 480, units = "px", bg = "white")
plot(perf, main = 'SimpliFi ROC Plot', col = 'green', lwd = 3)
abline(0,1)
dev.off()

smplfi_tmp <- smplfi
smplfi_tmp$spo_probability <- round(predict(rf, newdata = smplfi_tmp), 2)

# put site name back in
site_names <- overall
site_names <- site_names[,c(1:2)]

smplfi_tmp <- cbind(site_names, smplfi_tmp)

# threshold test
smplfi_threshold_test <- smplfi_tmp
smplfi_threshold_test <- smplfi_threshold_test[which(smplfi_threshold_test$spo_probability > threshold_number), ]
smplfi_threshold_test$count <- 1
smplfi_test <- sum(smplfi_threshold_test$smplfi_spend_flag) / sum(smplfi_threshold_test$count)

# save RF model 
saveRDS(rf, file = "~/r_files/spo_machine_learning_models/rf_rds_files/rf_smplfi.RDS")

#####################
# Basis training data
#####################

basis <- overall

# remove Basis spend numbers
basis$tot_basis_spend_usd <- NULL

# temp remove site name
basis$site_name <- NULL
basis$site_nk <- NULL

# create training and testing data sets
set.seed(123456)
training_model <- createDataPartition(y = basis$basis_spend_flag, p=0.6, list=FALSE)

training <- basis[training_model,]
testing <- basis[-training_model,]

# output dimension counts to make sure they match
dim(training); dim(testing)

set.seed(123456)
registerDoParallel()

# random forest model
col_to_remove <- "basis_spend_flag"

x <- training[, !(colnames(training) %in% col_to_remove)]
y <- training$basis_spend_flag

rf <- foreach(ntree = rep(40, 6), .combine = randomForest::combine, .packages = 'randomForest') %dopar% {
    randomForest(x, y, ntree = ntree)}

# training RMSE
training$predictions <- round(predict(rf, newdata = training), 2)
basis_training_accuracy <- accuracy(training$predictions, training$basis_spend_flag)

# testing RMSE
testing$predictions <- round(predict(rf, newdata = testing), 2)
basis_testing_accuracy <- accuracy(testing$predictions, testing$basis_spend_flag)

# gbm model
#gbm <- train(y ~ ., method = "gbm",data=training)

# ROC Curve Plot
pred <- prediction(testing$predictions, testing$basis_spend_flag)
perf <- performance(pred, "tpr", "fpr" )

png(filename = "~/r_files/spo_machine_learning_models/plot_outputs/Basis roc plot v1.png", width = 480, height = 480, units = "px", bg = "white")
plot(perf, main = 'Basis ROC Plot', col = 'green', lwd = 3)
abline(0,1)
dev.off()

basis_tmp <- basis
basis_tmp$spo_probability <- round(predict(rf, newdata = basis_tmp), 2)

# put site name back in
site_names <- overall
site_names <- site_names[,c(1:2)]

basis_tmp <- cbind(site_names, basis_tmp)

# threshold test
basis_threshold_test <- basis_tmp
basis_threshold_test <- basis_threshold_test[which(basis_threshold_test$spo_probability > threshold_number), ]
basis_threshold_test$count <- 1
basis_test <- sum(basis_threshold_test$basis_spend_flag) / sum(basis_threshold_test$count)

# save RF model 
saveRDS(rf, file = "~/r_files/spo_machine_learning_models/rf_rds_files/rf_basis.RDS")

#####################
# AdRoll training data
#####################

adroll <- overall

# remove AdRoll spend numbers
adroll$tot_adroll_spend_usd <- NULL

# temp remove site name
adroll$site_name <- NULL
adroll$site_nk <- NULL

# create training and testing data sets
set.seed(123456)
training_model <- createDataPartition(y = adroll$adroll_spend_flag, p=0.6, list=FALSE)

training <- adroll[training_model,]
testing <- adroll[-training_model,]

# output dimension counts to make sure they match
dim(training); dim(testing)

set.seed(123456)
registerDoParallel()

# random forest model
col_to_remove <- "adroll_spend_flag"

x <- training[, !(colnames(training) %in% col_to_remove)]
y <- training$adroll_spend_flag

rf <- foreach(ntree = rep(40, 6), .combine = randomForest::combine, .packages = 'randomForest') %dopar% {
    randomForest(x, y, ntree = ntree)}

# training RMSE
training$predictions <- round(predict(rf, newdata = training), 2)
adroll_training_accuracy <- accuracy(training$predictions, training$adroll_spend_flag)

# testing RMSE
testing$predictions <- round(predict(rf, newdata = testing), 2)
adroll_testing_accuracy <- accuracy(testing$predictions, testing$adroll_spend_flag)

# gbm model
#gbm <- train(y ~ ., method = "gbm",data=training)

# ROC Curve Plot
pred <- prediction(testing$predictions, testing$adroll_spend_flag)
perf <- performance(pred, "tpr", "fpr" )

png(filename = "~/r_files/spo_machine_learning_models/plot_outputs/Adroll roc plot v1.png", width = 480, height = 480, units = "px", bg = "white")
plot(perf, main = 'AdRoll ROC Plot', col = 'green', lwd = 3)
abline(0,1)
dev.off()

adroll_tmp <- adroll
adroll_tmp$spo_probability <- round(predict(rf, newdata = adroll_tmp), 2)

# put site name back in
site_names <- overall
site_names <- site_names[,c(1:2)]

adroll_tmp <- cbind(site_names, adroll_tmp)

# threshold test
adroll_threshold_test <- adroll_tmp
adroll_threshold_test <- adroll_threshold_test[which(adroll_threshold_test$spo_probability > threshold_number), ]
adroll_threshold_test$count <- 1
adroll_test <- sum(adroll_threshold_test$adroll_spend_flag) / sum(adroll_threshold_test$count)

# save RF model 
saveRDS(rf, file = "~/r_files/spo_machine_learning_models/rf_rds_files/rf_adroll.RDS")

#####################
# Beeswax training data
#####################

beeswax <- overall

# remove Beeswax spend numbers
beeswax$tot_beeswax_spend_usd <- NULL

# temp remove site name
beeswax$site_name <- NULL
beeswax$site_nk <- NULL

# create training and testing data sets
set.seed(123456)
training_model <- createDataPartition(y = beeswax$beeswax_spend_flag, p=0.6, list=FALSE)

training <- beeswax[training_model,]
testing <- beeswax[-training_model,]

# output dimension counts to make sure they match
dim(training); dim(testing)

set.seed(123456)
registerDoParallel()

# random forest model
col_to_remove <- "beeswax_spend_flag"

x <- training[, !(colnames(training) %in% col_to_remove)]
y <- training$beeswax_spend_flag

rf <- foreach(ntree = rep(40, 6), .combine = randomForest::combine, .packages = 'randomForest') %dopar% {
    randomForest(x, y, ntree = ntree)}

# training RMSE
training$predictions <- round(predict(rf, newdata = training), 2)
beeswax_training_accuracy <- accuracy(training$predictions, training$beeswax_spend_flag)

# testing RMSE
testing$predictions <- round(predict(rf, newdata = testing), 2)
beeswax_testing_accuracy <- accuracy(testing$predictions, testing$beeswax_spend_flag)

# gbm model
#gbm <- train(y ~ ., method = "gbm",data=training)

# ROC Curve Plot
pred <- prediction(testing$predictions, testing$beeswax_spend_flag)
perf <- performance(pred, "tpr", "fpr" )

png(filename = "~/r_files/spo_machine_learning_models/plot_outputs/Beeswax roc plot v1.png", width = 480, height = 480, units = "px", bg = "white")
plot(perf, main = 'Beeswax ROC Plot', col = 'green', lwd = 3)
abline(0,1)
dev.off()

beeswax_tmp <- beeswax
beeswax_tmp$spo_probability <- round(predict(rf, newdata = beeswax_tmp), 2)

# put site name back in
site_names <- overall
site_names <- site_names[,c(1:2)]

beeswax_tmp <- cbind(site_names, beeswax_tmp)

# threshold test
beeswax_threshold_test <- beeswax_tmp
beeswax_threshold_test <- beeswax_threshold_test[which(beeswax_threshold_test$spo_probability > threshold_number), ]
beeswax_threshold_test$count <- 1
beeswax_test <- sum(beeswax_threshold_test$beeswax_spend_flag) / sum(beeswax_threshold_test$count)

# save RF model 
saveRDS(rf, file = "~/r_files/spo_machine_learning_models/rf_rds_files/rf_beeswax.RDS")

#####################
# Quantcast training data
#####################

quantcast <- overall

# remove Quantcast spend numbers
quantcast$tot_quantcast_spend_usd <- NULL

# temp remove site name
quantcast$site_name <- NULL
quantcast$site_nk <- NULL

# create training and testing data sets
set.seed(123456)
training_model <- createDataPartition(y = quantcast$quantcast_spend_flag, p=0.6, list=FALSE)

training <- quantcast[training_model,]
testing <- quantcast[-training_model,]

# output dimension counts to make sure they match
dim(training); dim(testing)

set.seed(123456)
registerDoParallel()

# random forest model
col_to_remove <- "quantcast_spend_flag"

x <- training[, !(colnames(training) %in% col_to_remove)]
y <- training$quantcast_spend_flag

rf <- foreach(ntree = rep(40, 6), .combine = randomForest::combine, .packages = 'randomForest') %dopar% {
    randomForest(x, y, ntree = ntree)}

# training RMSE
training$predictions <- round(predict(rf, newdata = training), 2)
quantcast_training_accuracy <- accuracy(training$predictions, training$quantcast_spend_flag)

# testing RMSE
testing$predictions <- round(predict(rf, newdata = testing), 2)
quantcast_testing_accuracy <- accuracy(testing$predictions, testing$quantcast_spend_flag)

# gbm model
#gbm <- train(y ~ ., method = "gbm",data=training)

# ROC Curve Plot
pred <- prediction(testing$predictions, testing$quantcast_spend_flag)
perf <- performance(pred, "tpr", "fpr" )

png(filename = "~/r_files/spo_machine_learning_models/plot_outputs/Quantcast roc plot v1.png", width = 480, height = 480, units = "px", bg = "white")
plot(perf, main = 'Quantcast ROC Plot', col = 'green', lwd = 3)
abline(0,1)
dev.off()

quantcast_tmp <- quantcast
quantcast_tmp$spo_probability <- round(predict(rf, newdata = quantcast_tmp), 2)

# put site name back in
site_names <- overall
site_names <- site_names[,c(1:2)]

quantcast_tmp <- cbind(site_names, quantcast_tmp)

# threshold test
quantcast_threshold_test <- quantcast_tmp
quantcast_threshold_test <- quantcast_threshold_test[which(quantcast_threshold_test$spo_probability > threshold_number), ]
quantcast_threshold_test$count <- 1
quantcast_test <- sum(quantcast_threshold_test$quantcast_spend_flag) / sum(quantcast_threshold_test$count)

# save RF model 
saveRDS(rf, file = "~/r_files/spo_machine_learning_models/rf_rds_files/rf_quantcast.RDS")

#####################
# DataXu training data
#####################

dataxu <- overall

# remove DataXu spend numbers
dataxu$tot_dataxu_spend_usd <- NULL

# temp remove site name
dataxu$site_name <- NULL
dataxu$site_nk <- NULL

# create training and testing data sets
set.seed(123456)
training_model <- createDataPartition(y = dataxu$dataxu_spend_flag, p=0.6, list=FALSE)

training <- dataxu[training_model,]
testing <- dataxu[-training_model,]

# output dimension counts to make sure they match
dim(training); dim(testing)

set.seed(123456)
registerDoParallel()

# random forest model
col_to_remove <- "dataxu_spend_flag"

x <- training[, !(colnames(training) %in% col_to_remove)]
y <- training$dataxu_spend_flag

rf <- foreach(ntree = rep(40, 6), .combine = randomForest::combine, .packages = 'randomForest') %dopar% {
    randomForest(x, y, ntree = ntree)}

# training RMSE
training$predictions <- round(predict(rf, newdata = training), 2)
dataxu_training_accuracy <- accuracy(training$predictions, training$dataxu_spend_flag)

# testing RMSE
testing$predictions <- round(predict(rf, newdata = testing), 2)
dataxu_testing_accuracy <- accuracy(testing$predictions, testing$dataxu_spend_flag)

# gbm model
#gbm <- train(y ~ ., method = "gbm",data=training)

# ROC Curve Plot
pred <- prediction(testing$predictions, testing$dataxu_spend_flag)
perf <- performance(pred, "tpr", "fpr" )

png(filename = "~/r_files/spo_machine_learning_models/plot_outputs/Dataxy roc plot v1.png", width = 480, height = 480, units = "px", bg = "white")
plot(perf, main = 'DataXu ROC Plot', col = 'green', lwd = 3)
abline(0,1)
dev.off()

dataxu_tmp <- dataxu
dataxu_tmp$spo_probability <- round(predict(rf, newdata = dataxu_tmp), 2)

# put site name back in
site_names <- overall
site_names <- site_names[,c(1:2)]

dataxu_tmp <- cbind(site_names, dataxu_tmp)

# threshold test
dataxu_threshold_test <- dataxu_tmp
dataxu_threshold_test <- dataxu_threshold_test[which(dataxu_threshold_test$spo_probability > threshold_number), ]
dataxu_threshold_test$count <- 1
dataxu_test <- sum(dataxu_threshold_test$dataxu_spend_flag) / sum(dataxu_threshold_test$count)

# save RF model 
saveRDS(rf, file = "~/r_files/spo_machine_learning_models/rf_rds_files/rf_dataxu.RDS")

#####################
# AdForm training data
#####################

adform <- overall

# remove AdForm spend numbers
adform$tot_adform_spend_usd <- NULL

# temp remove site name
adform$site_name <- NULL
adform$site_nk <- NULL

# create training and testing data sets
set.seed(123456)
training_model <- createDataPartition(y = adform$adform_spend_flag, p=0.6, list=FALSE)

training <- adform[training_model,]
testing <- adform[-training_model,]

# output dimension counts to make sure they match
dim(training); dim(testing)

set.seed(123456)
registerDoParallel()

# random forest model
col_to_remove <- "adform_spend_flag"

x <- training[, !(colnames(training) %in% col_to_remove)]
y <- training$adform_spend_flag

rf <- foreach(ntree = rep(40, 6), .combine = randomForest::combine, .packages = 'randomForest') %dopar% {
    randomForest(x, y, ntree = ntree)}

# training RMSE
training$predictions <- round(predict(rf, newdata = training), 2)
adform_training_accuracy <- accuracy(training$predictions, training$adform_spend_flag)

# testing RMSE
testing$predictions <- round(predict(rf, newdata = testing), 2)
adform_testing_accuracy <- accuracy(testing$predictions, testing$adform_spend_flag)

# gbm model
#gbm <- train(y ~ ., method = "gbm",data=training)

# ROC Curve Plot
pred <- prediction(testing$predictions, testing$adform_spend_flag)
perf <- performance(pred, "tpr", "fpr" )

png(filename = "~/r_files/spo_machine_learning_models/plot_outputs/Adform roc plot v1.png", width = 480, height = 480, units = "px", bg = "white")
plot(perf, main = 'AdForm ROC Plot', col = 'green', lwd = 3)
abline(0,1)
dev.off()

adform_tmp <- adform
adform_tmp$spo_probability <- round(predict(rf, newdata = adform_tmp), 2)

# put site name back in
site_names <- overall
site_names <- site_names[,c(1:2)]

adform_tmp <- cbind(site_names, adform_tmp)

# threshold test
adform_threshold_test <- adform_tmp
adform_threshold_test <- adform_threshold_test[which(adform_threshold_test$spo_probability > threshold_number), ]
adform_threshold_test$count <- 1
adform_test <- sum(adform_threshold_test$adform_spend_flag) / sum(adform_threshold_test$count)

# save RF model 
saveRDS(rf, file = "~/r_files/spo_machine_learning_models/rf_rds_files/rf_adform.RDS")

#####################
# AdTheorent training data
#####################

adtheorent <- overall

# remove AdTheorent spend numbers
adtheorent$tot_adtheorent_spend_usd <- NULL

# temp remove site name
adtheorent$site_name <- NULL
adtheorent$site_nk <- NULL

# create training and testing data sets
set.seed(123456)
training_model <- createDataPartition(y = adtheorent$adtheorent_spend_flag, p=0.6, list=FALSE)

training <- adtheorent[training_model,]
testing <- adtheorent[-training_model,]

# output dimension counts to make sure they match
dim(training); dim(testing)

set.seed(123456)
registerDoParallel()

# random forest model
col_to_remove <- "adtheorent_spend_flag"

x <- training[, !(colnames(training) %in% col_to_remove)]
y <- training$adtheorent_spend_flag

rf <- foreach(ntree = rep(40, 6), .combine = randomForest::combine, .packages = 'randomForest') %dopar% {
    randomForest(x, y, ntree = ntree)}

# training RMSE
training$predictions <- round(predict(rf, newdata = training), 2)
adtheorent_training_accuracy <- accuracy(training$predictions, training$adtheorent_spend_flag)

# testing RMSE
testing$predictions <- round(predict(rf, newdata = testing), 2)
adtheorent_testing_accuracy <- accuracy(testing$predictions, testing$adtheorent_spend_flag)

# gbm model
#gbm <- train(y ~ ., method = "gbm",data=training)

# ROC Curve Plot
pred <- prediction(testing$predictions, testing$adtheorent_spend_flag)
perf <- performance(pred, "tpr", "fpr" )

png(filename = "~/r_files/spo_machine_learning_models/plot_outputs/Adtheorent roc plot v1.png", width = 480, height = 480, units = "px", bg = "white")
plot(perf, main = 'AdTheorent ROC Plot', col = 'green', lwd = 3)
abline(0,1)
dev.off()

adtheorent_tmp <- adtheorent
adtheorent_tmp$spo_probability <- round(predict(rf, newdata = adtheorent_tmp), 2)

# put site name back in
site_names <- overall
site_names <- site_names[,c(1:2)]

adtheorent_tmp <- cbind(site_names, adtheorent_tmp)

# threshold test
adtheorent_threshold_test <- adtheorent_tmp
adtheorent_threshold_test <- adtheorent_threshold_test[which(adtheorent_threshold_test$spo_probability > threshold_number), ]
adtheorent_threshold_test$count <- 1
adtheorent_test <- sum(adtheorent_threshold_test$adtheorent_spend_flag) / sum(adtheorent_threshold_test$count)

# save RF model 
saveRDS(rf, file = "~/r_files/spo_machine_learning_models/rf_rds_files/rf_adtheorent.RDS")

#####################
# Bidswitch training data
#####################

bidswitch <- overall

# remove Bidswitch spend numbers
bidswitch$tot_bidswitch_spend_usd <- NULL

# temp remove site name
bidswitch$site_name <- NULL
bidswitch$site_nk <- NULL

# create training and testing data sets
set.seed(123456)
training_model <- createDataPartition(y = bidswitch$bidswitch_spend_flag, p=0.6, list=FALSE)

training <- bidswitch[training_model,]
testing <- bidswitch[-training_model,]

# output dimension counts to make sure they match
dim(training); dim(testing)

set.seed(123456)
registerDoParallel()

# random forest model
col_to_remove <- "bidswitch_spend_flag"

x <- training[, !(colnames(training) %in% col_to_remove)]
y <- training$bidswitch_spend_flag

rf <- foreach(ntree = rep(40, 6), .combine = randomForest::combine, .packages = 'randomForest') %dopar% {
    randomForest(x, y, ntree = ntree)}

# training RMSE
training$predictions <- round(predict(rf, newdata = training), 2)
bidswitch_training_accuracy <- accuracy(training$predictions, training$bidswitch_spend_flag)

# testing RMSE
testing$predictions <- round(predict(rf, newdata = testing), 2)
bidswitch_testing_accuracy <- accuracy(testing$predictions, testing$bidswitch_spend_flag)

# gbm model
#gbm <- train(y ~ ., method = "gbm",data=training)

# ROC Curve Plot
pred <- prediction(testing$predictions, testing$bidswitch_spend_flag)
perf <- performance(pred, "tpr", "fpr" )

png(filename = "~/r_files/spo_machine_learning_models/plot_outputs/Bidswitch roc plot v1.png", width = 480, height = 480, units = "px", bg = "white")
plot(perf, main = 'Bidswitch ROC Plot', col = 'green', lwd = 3)
abline(0,1)
dev.off()

bidswitch_tmp <- bidswitch
bidswitch_tmp$spo_probability <- round(predict(rf, newdata = bidswitch_tmp), 2)

# put site name back in
site_names <- overall
site_names <- site_names[,c(1:2)]

bidswitch_tmp <- cbind(site_names, bidswitch_tmp)

# threshold test
bidswitch_threshold_test <- bidswitch_tmp
bidswitch_threshold_test <- bidswitch_threshold_test[which(bidswitch_threshold_test$spo_probability > threshold_number), ]
bidswitch_threshold_test$count <- 1
bidswitch_test <- sum(bidswitch_threshold_test$bidswitch_spend_flag) / sum(bidswitch_threshold_test$count)


# save RF model 
saveRDS(rf, file = "~/r_files/spo_machine_learning_models/rf_rds_files/rf_bidswitch.RDS")

#####################
# Conversant training data
#####################

conversant <- overall

# remove Conversant spend numbers
conversant$tot_conversant_spend_usd <- NULL

# temp remove site name
conversant$site_name <- NULL
conversant$site_nk <- NULL

# create training and testing data sets
set.seed(123456)
training_model <- createDataPartition(y = conversant$conversant_spend_flag, p=0.6, list=FALSE)

training <- conversant[training_model,]
testing <- conversant[-training_model,]

# output dimension counts to make sure they match
dim(training); dim(testing)

set.seed(123456)
registerDoParallel()

# random forest model
col_to_remove <- "conversant_spend_flag"

x <- training[, !(colnames(training) %in% col_to_remove)]
y <- training$conversant_spend_flag

rf <- foreach(ntree = rep(40, 6), .combine = randomForest::combine, .packages = 'randomForest') %dopar% {
    randomForest(x, y, ntree = ntree)}

# training RMSE
training$predictions <- round(predict(rf, newdata = training), 2)
conversant_training_accuracy <- accuracy(training$predictions, training$conversant_spend_flag)

# testing RMSE
testing$predictions <- round(predict(rf, newdata = testing), 2)
conversant_testing_accuracy <- accuracy(testing$predictions, testing$conversant_spend_flag)

# gbm model
#gbm <- train(y ~ ., method = "gbm",data=training)

# ROC Curve Plot
pred <- prediction(testing$predictions, testing$conversant_spend_flag)
perf <- performance(pred, "tpr", "fpr" )

png(filename = "~/r_files/spo_machine_learning_models/plot_outputs/Conversant roc plot v1.png", width = 480, height = 480, units = "px", bg = "white")
plot(perf, main = 'Conversant ROC Plot', col = 'green', lwd = 3)
abline(0,1)
dev.off()

conversant_tmp <- conversant
conversant_tmp$spo_probability <- round(predict(rf, newdata = conversant_tmp), 2)

# put site name back in
site_names <- overall
site_names <- site_names[,c(1:2)]

conversant_tmp <- cbind(site_names, conversant_tmp)

# threshold test
conversant_threshold_test <- conversant_tmp
conversant_threshold_test <- conversant_threshold_test[which(conversant_threshold_test$spo_probability > threshold_number), ]
conversant_threshold_test$count <- 1
conversant_test <- sum(conversant_threshold_test$conversant_spend_flag) / sum(conversant_threshold_test$count)

# save RF model 
saveRDS(rf, file = "~/r_files/spo_machine_learning_models/rf_rds_files/rf_conversant.RDS")

########################
# Liftoff training data
########################

liftoff <- overall

# remove Liftoff spend numbers
liftoff$tot_liftoff_spend_usd <- NULL

# temp remove site name
liftoff$site_name <- NULL
liftoff$site_nk <- NULL

# create training and testing data sets
set.seed(123456)
training_model <- createDataPartition(y = liftoff$liftoff_spend_flag, p=0.6, list=FALSE)

training <- liftoff[training_model,]
testing <- liftoff[-training_model,]

# output dimension counts to make sure they match
dim(training); dim(testing)

set.seed(123456)
registerDoParallel()

# random forest model
col_to_remove <- "liftoff_spend_flag"

x <- training[, !(colnames(training) %in% col_to_remove)]
y <- training$liftoff_spend_flag

rf <- foreach(ntree = rep(40, 6), .combine = randomForest::combine, .packages = 'randomForest') %dopar% {
    randomForest(x, y, ntree = ntree)}

# training RMSE
training$predictions <- round(predict(rf, newdata = training), 2)
liftoff_training_accuracy <- accuracy(training$predictions, training$liftoff_spend_flag)

# testing RMSE
testing$predictions <- round(predict(rf, newdata = testing), 2)
liftoff_testing_accuracy <- accuracy(testing$predictions, testing$liftoff_spend_flag)

# gbm model
#gbm <- train(y ~ ., method = "gbm",data=training)

# ROC Curve Plot
pred <- prediction(testing$predictions, testing$liftoff_spend_flag)
perf <- performance(pred, "tpr", "fpr" )

png(filename = "~/r_files/spo_machine_learning_models/plot_outputs/Liftoff roc plot v1.png", width = 480, height = 480, units = "px", bg = "white")
plot(perf, main = 'Liftoff ROC Plot', col = 'green', lwd = 3)
abline(0,1)
dev.off()

liftoff_tmp <- liftoff
liftoff_tmp$spo_probability <- round(predict(rf, newdata = liftoff_tmp), 2)

# put site name back in
site_names <- overall
site_names <- site_names[,c(1:2)]

liftoff_tmp <- cbind(site_names, liftoff_tmp)

# threshold test
liftoff_threshold_test <- liftoff_tmp
liftoff_threshold_test <- liftoff_threshold_test[which(liftoff_threshold_test$spo_probability > threshold_number), ]
liftoff_threshold_test$count <- 1
liftoff_test <- sum(liftoff_threshold_test$liftoff_spend_flag) / sum(liftoff_threshold_test$count)

# save RF model 
saveRDS(rf, file = "~/r_files/spo_machine_learning_models/rf_rds_files/rf_liftoff.RDS")

####creating new summary table
combined <- as.matrix(data[,c(2:4,12)])

a9_spo_probability <- a9_tmp[,c(49)]
adform_spo_probability <- adform_tmp[,c(49)]
adobe_spo_probability <- adobe_tmp[,49]
adroll_spo_probability <- adroll_tmp[,49]
adtheorent_spo_probability <- adtheorent_tmp[,49]
amobee_spo_probability <- amobee_tmp[,49]
basis_spo_probability <- basis_tmp[,49]
beeswax_spo_probability <- beeswax_tmp[,49]
bidswitch_spo_probability <- bidswitch_tmp[,49]
conversant_spo_probability <- conversant_tmp[,49]
criteo_spo_probability <- criteo_tmp[,49]
dataxu_spo_probability <- dataxu_tmp[,49]
dbm_spo_probability <- dbm_tmp[,49]
liftoff_spo_probability <- liftoff_tmp[,49]
mm_spo_probability <- mm_tmp[,49]
oath_spo_probability <- oath_tmp[,49]
quantcast_spo_probability <- quantcast_tmp[,49]
smplfi_spo_probability <- smplfi_tmp[,49]
ttd_spo_probability <- ttd_tmp[,49]

# create table with spo probabilities by DSP and site name
combined <- cbind(combined, a9_spo_probability, adform_spo_probability, adobe_spo_probability, adroll_spo_probability, adtheorent_spo_probability, 
                  amobee_spo_probability, basis_spo_probability, beeswax_spo_probability, bidswitch_spo_probability, conversant_spo_probability, 
                  criteo_spo_probability, dataxu_spo_probability, dbm_spo_probability, liftoff_spo_probability, mm_spo_probability, oath_spo_probability, 
                  quantcast_spo_probability, smplfi_spo_probability, ttd_spo_probability)

combined <- as.data.frame(combined, stringsAsFactors = F)
combined[,c(2,4:23)] <- sapply(combined[,c(2,4:23)], as.numeric)

combined$average_spo_probability <- rowMeans(combined[,c(5:ncol(combined))])

combined$category <- ifelse(combined$average_spo_probability < .50, "Low", 
                            ifelse(combined$average_spo_probability >= .50 & combined$average_spo_probability <= .75, "Med", "High"))

combined$count <- rowSums(combined[,c(3:21)] > .80)

# create table with threshold test numbers
thresholds <- rbind(a9_test, adform_test, adobe_test, adroll_test, adtheorent_test, amobee_test,
           basis_test, beeswax_test, bidswitch_test, conversant_test, criteo_test, dataxu_test,
           dbm_test, liftoff_test, mm_test, oath_test, quantcast_test, smplfi_test, ttd_test)

thresholds <- as.data.frame(thresholds, stringsAsFactors = F)

write.csv(combined, file = paste0("~/r_files/spo_machine_learning_models/data_outputs/", last_month, "_", "results_table.csv"), row.names = F)
write.csv(thresholds, file = paste0("~/r_files/spo_machine_learning_models/data_outputs/", last_month, "_", "threshold_results_table.csv"))

# #########################################################################
# # testing accuracy of model with prior month data
# 
# query <- "
# select 
# utc_month_sid
# ,b.account_name as sf_account_name
# ,b.site_name as site_name
# ,case when p_mobl_app_bundle is not null then 1 else 0 end as app_flag
# ,case when p_mapped_adunit_type = 'VIDEO' then 1 else 0 end as video_flag
# ,case when b.business_client = 'google-bidout' then 1 else 0 end as eb_flag
# ,case when b.site_name like ('%A9%') then 1 else 0 end as a9_flag
# ,case when x_bidout = true then 1 else 0 end as bidout_flag
# ,case when x_bidder_elig = true then 1 else 0 end as bidder_flag
# ,sum(case when x_price_won = '1' then tot_usd_a_spend else 0 end) as tot_fpa_spend
# ,sum(tot_usd_a_spend) as tot_usd_a_spend
# ,sum(case when c.advertiser_account_name = 'DoubleClick Bid Manager - RTB' then tot_usd_a_spend else 0 end) as tot_dbm_spend_usd
# ,sum(case when c.advertiser_account_name = 'The Trade Desk - RTB' then tot_usd_a_spend else 0 end) as tot_ttd_spend_usd
# ,sum(case when c.advertiser_account_name = 'Adobe Advertising Cloud - RTB' then tot_usd_a_spend else 0 end) as tot_adobe_spend_usd
# ,sum(case when c.advertiser_account_name = 'Oath Ad Platforms DSP - RTB' then tot_usd_a_spend else 0 end) as tot_oath_spend_usd
# ,sum(case when c.advertiser_account_name = 'Amobee - RTB' then tot_usd_a_spend else 0 end) as tot_amobee_spend_usd
# ,sum(case when c.advertiser_account_name = 'Media Math - RTB' then tot_usd_a_spend else 0 end) as tot_mm_spend_usd
# ,sum(case when c.advertiser_account_name = 'A9 - RTB' then tot_usd_a_spend else 0 end) as tot_a9_spend_usd
# ,sum(case when c.advertiser_account_name = 'Criteo - RTB' then tot_usd_a_spend else 0 end) as tot_criteo_spend_usd
# ,sum(case when c.advertiser_account_name = 'Simpli.fi - RTB' then tot_usd_a_spend else 0 end) as tot_smplfi_spend_usd
# ,sum(case when c.advertiser_account_name = 'Sizmek - RTB' then tot_usd_a_spend else 0 end) as tot_sizmek_spend_usd
# ,sum(case when c.advertiser_account_name = 'Basis DSP - RTB' then tot_usd_a_spend else 0 end) as tot_basis_spend_usd
# ,sum(case when c.advertiser_account_name = 'AdRoll - RTB' then tot_usd_a_spend else 0 end) as tot_adroll_spend_usd
# ,sum(case when c.advertiser_account_name = 'Beeswax - RTB' then tot_usd_a_spend else 0 end) as tot_beeswax_spend_usd
# ,sum(case when c.advertiser_account_name = 'Quantcast - RTB' then tot_usd_a_spend else 0 end) as tot_quantcast_spend_usd
# ,sum(case when c.advertiser_account_name = 'DataXu - RTB' then tot_usd_a_spend else 0 end) as tot_dataxu_spend_usd
# ,sum(case when c.advertiser_account_name = 'Adform - RTB' then tot_usd_a_spend else 0 end) as tot_adform_spend_usd
# ,sum(case when c.advertiser_account_name = 'AdTheorent - RTB' then tot_usd_a_spend else 0 end) as tot_adtheorent_spend_usd
# ,sum(case when c.advertiser_account_name = 'Bidswitch - RTB' then tot_usd_a_spend else 0 end) as tot_bidswitch_spend_usd
# ,sum(case when c.advertiser_account_name = 'Conversant - RTB' then tot_usd_a_spend else 0 end) as tot_conversant_spend_usd
# ,sum(case when c.advertiser_account_name = 'Liftoff - RTB' then tot_usd_a_spend else 0 end) as tot_liftoff_spend_usd
# 
# from mstr_datamart.ox_transaction_sum_monthly_fact as a
# left join mstr_datamart.dim_sites_to_owners b on a.site_nk = b.site_nk
# left join mstr_datamart.advertiser_dim c on a.advertiser_account_nk = c.advertiser_account_nk
# 
# where utc_month_sid = '20181101'
# and is_mkt = TRUE
# 
# group by 1,2,3,4,5,6,7,8,9 having sum(tot_usd_a_spend) > 10
# order by 10 desc;"
# 
# older_data <- dbGetQuery(snowflake, query)
# names(older_data) <- tolower(names(older_data))
# 
# # add in network flag
# older_data <- left_join(older_data, network_list, by = "sf_account_name")
# older_data$network_flag <- ifelse(older_data$network_direct == "network", 1, 0)
# 
# # clean up data and move columns around
# older_data$sf_account_id <- NULL
# older_data$network_direct <- NULL
# older_data$utc_month_sid <- NULL
# older_data$sf_account_name <- NULL
# older_data <- older_data[,c(1:7,30,8:29)]
# older_data$network_flag[is.na(older_data$network_flag)] <- 0
# 
# #older_data$site_name <- NULL
# 
# # create dsp specific spend flags
# older_data$dbm_spend_flag <- ifelse(older_data$tot_dbm_spend_usd > 0, 0, 1)
# older_data$ttd_spend_flag <- ifelse(older_data$tot_ttd_spend_usd > 0, 0, 1)
# older_data$adobe_spend_flag <- ifelse(older_data$tot_adobe_spend_usd > 0, 0, 1)
# older_data$oath_spend_flag <- ifelse(older_data$tot_oath_spend_usd > 0, 0, 1)
# older_data$amobee_spend_flag <- ifelse(older_data$tot_amobee_spend_usd > 0, 0, 1)
# older_data$mm_spend_flag <- ifelse(older_data$tot_mm_spend_usd > 0, 0, 1)
# older_data$a9_spend_flag <- ifelse(older_data$tot_a9_spend_usd > 0, 0, 1)
# older_data$criteo_spend_flag <- ifelse(older_data$tot_criteo_spend_usd > 0, 0, 1)
# older_data$smplfi_spend_flag <- ifelse(older_data$tot_smplfi_spend_usd > 0, 0, 1)
# older_data$sizmek_spend_flag <- ifelse(older_data$tot_sizmek_spend_usd > 0, 0, 1)
# older_data$basis_spend_flag <- ifelse(older_data$tot_basis_spend_usd > 0, 0, 1)
# older_data$adroll_spend_flag <- ifelse(older_data$tot_adroll_spend_usd > 0, 0, 1)
# older_data$beeswax_spend_flag <- ifelse(older_data$tot_beeswax_spend_usd > 0, 0, 1)
# older_data$quantcast_spend_flag <- ifelse(older_data$tot_quantcast_spend_usd > 0, 0, 1)
# older_data$dataxu_spend_flag <- ifelse(older_data$tot_dataxu_spend_usd > 0, 0, 1)
# older_data$adform_spend_flag <- ifelse(older_data$tot_adform_spend_usd > 0, 0, 1)
# older_data$adtheorent_spend_flag <- ifelse(older_data$tot_adtheorent_spend_usd > 0, 0, 1)
# older_data$bidswitch_spend_flag <- ifelse(older_data$tot_bidswitch_spend_usd > 0, 0, 1)
# older_data$conversant_spend_flag <- ifelse(older_data$tot_conversant_spend_usd > 0, 0, 1)
# older_data$liftoff_spend_flag <- ifelse(older_data$tot_liftoff_spend_usd > 0, 0, 1)
# 
# # scale spend numbers for better fit with model
# older_data$tot_usd_a_spend <- (older_data$tot_usd_a_spend - mean(older_data$tot_usd_a_spend))/sd(older_data$tot_usd_a_spend)
# older_data$tot_fpa_spend <- (older_data$tot_fpa_spend - mean(older_data$tot_fpa_spend))/sd(older_data$tot_fpa_spend)
# older_data$tot_dbm_spend_usd <- (older_data$tot_dbm_spend_usd - mean(older_data$tot_dbm_spend_usd))/sd(older_data$tot_dbm_spend_usd)
# older_data$tot_ttd_spend_usd <- (older_data$tot_ttd_spend_usd - mean(older_data$tot_ttd_spend_usd))/sd(older_data$tot_ttd_spend_usd)
# older_data$tot_adobe_spend_usd <- (older_data$tot_adobe_spend_usd - mean(older_data$tot_adobe_spend_usd))/sd(older_data$tot_adobe_spend_usd)
# older_data$tot_oath_spend_usd <- (older_data$tot_oath_spend_usd - mean(older_data$tot_oath_spend_usd))/sd(older_data$tot_oath_spend_usd)
# older_data$tot_amobee_spend_usd <- (older_data$tot_amobee_spend_usd - mean(older_data$tot_amobee_spend_usd))/sd(older_data$tot_amobee_spend_usd)
# older_data$tot_mm_spend_usd <- (older_data$tot_mm_spend_usd - mean(older_data$tot_mm_spend_usd))/sd(older_data$tot_mm_spend_usd)
# older_data$tot_a9_spend_usd <- (older_data$tot_a9_spend_usd - mean(older_data$tot_a9_spend_usd))/sd(older_data$tot_a9_spend_usd)
# older_data$tot_criteo_spend_usd <- (older_data$tot_criteo_spend_usd - mean(older_data$tot_criteo_spend_usd))/sd(older_data$tot_criteo_spend_usd)
# older_data$tot_smplfi_spend_usd <- (older_data$tot_smplfi_spend_usd - mean(older_data$tot_smplfi_spend_usd))/sd(older_data$tot_smplfi_spend_usd)
# older_data$tot_sizmek_spend_usd <- (older_data$tot_sizmek_spend_usd - mean(older_data$tot_sizmek_spend_usd))/sd(older_data$tot_sizmek_spend_usd)
# older_data$tot_basis_spend_usd <- (older_data$tot_basis_spend_usd - mean(older_data$tot_basis_spend_usd))/sd(older_data$tot_basis_spend_usd)
# older_data$tot_adroll_spend_usd <- (older_data$tot_adroll_spend_usd - mean(older_data$tot_adroll_spend_usd))/sd(older_data$tot_adroll_spend_usd)
# older_data$tot_beeswax_spend_usd <- (older_data$tot_beeswax_spend_usd - mean(older_data$tot_beeswax_spend_usd))/sd(older_data$tot_beeswax_spend_usd)
# older_data$tot_quantcast_spend_usd <- (older_data$tot_quantcast_spend_usd - mean(older_data$tot_quantcast_spend_usd))/sd(older_data$tot_quantcast_spend_usd)
# older_data$tot_dataxu_spend_usd <- (older_data$tot_dataxu_spend_usd - mean(older_data$tot_dataxu_spend_usd))/sd(older_data$tot_dataxu_spend_usd)
# older_data$tot_adform_spend_usd <- (older_data$tot_adform_spend_usd - mean(older_data$tot_adform_spend_usd))/sd(older_data$tot_adform_spend_usd)
# older_data$tot_adtheorent_spend_usd <- (older_data$tot_adtheorent_spend_usd - mean(older_data$tot_adtheorent_spend_usd))/sd(older_data$tot_adtheorent_spend_usd)
# older_data$tot_bidswitch_spend_usd <- (older_data$tot_bidswitch_spend_usd - mean(older_data$tot_bidswitch_spend_usd))/sd(older_data$tot_bidswitch_spend_usd)
# older_data$tot_conversant_spend_usd <- (older_data$tot_conversant_spend_usd - mean(older_data$tot_conversant_spend_usd))/sd(older_data$tot_conversant_spend_usd)
# older_data$tot_liftoff_spend_usd <- (older_data$tot_liftoff_spend_usd - mean(older_data$tot_liftoff_spend_usd))/sd(older_data$tot_liftoff_spend_usd)
# 
# # filter older data for sites that are not in overall data
# #older_data <- older_data[older_data$site_name %in% overall$site_name,]
# 
# 
#
# # Adobe testing data set
#
# 
# # sites adobe is not spending on in newer data set
# adobe_2019 <- overall[which(overall$adobe_spend_flag == 1),]
# adobe_2019 <- adobe_2019$site_name
# adobe_2019 <- as.list(adobe_2019)
# 
# # testing data set
# adobe_test <- older_data
# 
# # load Adobe RF for testing
# rf <- readRDS(file = "~/r_files/spo_machine_learning_models/rf_rds_files/rf_adobe.RDS")
# 
# adobe_test$site_name <- NULL
# adobe_test$tot_adobe_spend_usd <- NULL
# 
# # run model on old data
# adobe_test$spo_probability <- round(predict(rf, newdata = adobe_test), 2)
# 
# # put site name back in
# site_names <- older_data
# site_names <- site_names[,c(1)]
# adobe_test <- cbind(adobe_test, site_names)
# 
# # threshold test
# adobe_threshold_test <- adobe_test
# adobe_threshold_test <- adobe_threshold_test[which(adobe_threshold_test$spo_probability > .80), ]
# adobe_threshold_test$count <- 1
# 
# # add in column to see if sites in old data wound up going away in new data
# adobe_threshold_test$exists_flag <- ifelse(adobe_threshold_test$site_name %in% adobe_2019, 0, 1)
# 
# # get percentage of sites that model predicted correctly at threshold level
# adobe_threshold_test_number <- (sum(adobe_threshold_test$adobe_spend_flag) / sum(adobe_threshold_test$count))
# 
# # get percentage of sites that model predicted correctly based on dsp specific actual data
# adobe_actual_test_number <- round((sum(adobe_threshold_test$exists_flag) / sum(adobe_threshold_test$count)), 3)
# 
# # get percentage of sites that model predicted correctly based on entire actual data (random probability)
# adobe_overall_actual_test_number <- (sum(adobe_threshold_test$exists_flag) / length(older_data$site_name))
# 
# # ratio
# adobe_ratio <- adobe_actual_test_number / adobe_overall_actual_test_number
# 
# # combine fields
# adobe_data <- c("adobe", adobe_threshold_test_number, adobe_actual_test_number, adobe_overall_actual_test_number, adobe_ratio)
# 
# rm(adobe_2019, adobe_test, rf, site_names, adobe_threshold_test, adobe_threshold_test_number, adobe_actual_test_number, 
#    adobe_overall_actual_test_number, adobe_ratio)
# 
# 
# # DBM testing data set
# 
# 
# # sites dbm is not spending on in newer data set
# dbm_2019 <- overall[which(overall$dbm_spend_flag == 1),]
# dbm_2019 <- dbm_2019$site_name
# dbm_2019 <- as.list(dbm_2019)
# 
# # testing data set
# dbm_test <- older_data
# 
# # load dbm RF for testing
# rf <- readRDS(file = "~/r_files/spo_machine_learning_models/rf_rds_files/rf_dbm.RDS")
# 
# dbm_test$site_name <- NULL
# dbm_test$tot_dbm_spend_usd <- NULL
# 
# # run model on old data
# dbm_test$spo_probability <- round(predict(rf, newdata = dbm_test), 2)
# 
# # put site name back in
# site_names <- older_data
# site_names <- site_names[,c(1)]
# dbm_test <- cbind(dbm_test, site_names)
# 
# # threshold test
# dbm_threshold_test <- dbm_test
# dbm_threshold_test <- dbm_threshold_test[which(dbm_threshold_test$spo_probability > .50), ]
# dbm_threshold_test$count <- 1
# 
# # add in column to see if sites in old data wound up going away in new data
# dbm_threshold_test$exists_flag <- ifelse(dbm_threshold_test$site_name %in% dbm_2019, 0, 1)
# 
# # get percentage of sites that model predicted correctly at threshold level
# dbm_threshold_test_number <- (sum(dbm_threshold_test$dbm_spend_flag) / sum(dbm_threshold_test$count))
# 
# # get percentage of sites that model predicted correctly based on dsp specific actual data
# dbm_actual_test_number <- round((sum(dbm_threshold_test$exists_flag) / sum(dbm_threshold_test$count)), 3)
# 
# # get percentage of sites that model predicted correctly based on entire actual data (random probability)
# dbm_overall_actual_test_number <- (sum(dbm_threshold_test$exists_flag) / length(older_data$site_name))
# 
# # ratio
# dbm_ratio <- dbm_actual_test_number / dbm_overall_actual_test_number
# 
# # combine fields
# dbm_data <- c("dbm", dbm_threshold_test_number, dbm_actual_test_number, dbm_overall_actual_test_number, dbm_ratio)
# 
# rm(dbm_2019, dbm_test, rf, site_names, dbm_threshold_test, dbm_threshold_test_number, dbm_actual_test_number, 
#    dbm_overall_actual_test_number, dbm_ratio)
# 
# 
# # TTD testing data set
# 
# 
# # sites ttd is not spending on in newer data set
# ttd_2019 <- overall[which(overall$ttd_spend_flag == 1),]
# ttd_2019 <- ttd_2019$site_name
# ttd_2019 <- as.list(ttd_2019)
# 
# # testing data set
# ttd_test <- older_data
# 
# # load ttd RF for testing
# rf <- readRDS(file = "~/r_files/spo_machine_learning_models/rf_rds_files/rf_ttd.RDS")
# 
# ttd_test$site_name <- NULL
# ttd_test$tot_ttd_spend_usd <- NULL
# 
# # run model on old data
# ttd_test$spo_probability <- round(predict(rf, newdata = ttd_test), 2)
# 
# # put site name back in
# site_names <- older_data
# site_names <- site_names[,c(1)]
# ttd_test <- cbind(ttd_test, site_names)
# 
# # threshold test
# ttd_threshold_test <- ttd_test
# ttd_threshold_test <- ttd_threshold_test[which(ttd_threshold_test$spo_probability > .80), ]
# ttd_threshold_test$count <- 1
# 
# # add in column to see if sites in old data wound up going away in new data
# ttd_threshold_test$exists_flag <- ifelse(ttd_threshold_test$site_name %in% ttd_2019, 0, 1)
# 
# # get percentage of sites that model predicted correctly at threshold level
# ttd_threshold_test_number <- (sum(ttd_threshold_test$ttd_spend_flag) / sum(ttd_threshold_test$count))
# 
# # get percentage of sites that model predicted correctly based on dsp specific actual data
# ttd_actual_test_number <- round((sum(ttd_threshold_test$exists_flag) / sum(ttd_threshold_test$count)), 3)
# 
# # get percentage of sites that model predicted correctly based on entire actual data (random probability)
# ttd_overall_actual_test_number <- (sum(ttd_threshold_test$exists_flag) / length(older_data$site_name))
# 
# # ratio
# ttd_ratio <- ttd_actual_test_number / ttd_overall_actual_test_number
# 
# # combine fields
# ttd_data <- c("ttd", ttd_threshold_test_number, ttd_actual_test_number, ttd_overall_actual_test_number, ttd_ratio)
# 
# rm(ttd_2019, ttd_test, rf, site_names, ttd_threshold_test, ttd_threshold_test_number, ttd_actual_test_number, 
#    ttd_overall_actual_test_number, ttd_ratio)
# 
# 
# # Oath testing data set
# 
# 
# # sites oath is not spending on in newer data set
# oath_2019 <- overall[which(overall$oath_spend_flag == 1),]
# oath_2019 <- oath_2019$site_name
# oath_2019 <- as.list(oath_2019)
# 
# # testing data set
# oath_test <- older_data
# 
# # load oath RF for testing
# rf <- readRDS(file = "~/r_files/spo_machine_learning_models/rf_rds_files/rf_oath.RDS")
# 
# oath_test$site_name <- NULL
# oath_test$tot_oath_spend_usd <- NULL
# 
# # run model on old data
# oath_test$spo_probability <- round(predict(rf, newdata = oath_test), 2)
# 
# # put site name back in
# site_names <- older_data
# site_names <- site_names[,c(1)]
# oath_test <- cbind(oath_test, site_names)
# 
# # threshold test
# oath_threshold_test <- oath_test
# oath_threshold_test <- oath_threshold_test[which(oath_threshold_test$spo_probability > .80), ]
# oath_threshold_test$count <- 1
# 
# # add in column to see if sites in old data wound up going away in new data
# oath_threshold_test$exists_flag <- ifelse(oath_threshold_test$site_name %in% oath_2019, 0, 1)
# 
# # get percentage of sites that model predicted correctly at threshold level
# oath_threshold_test_number <- (sum(oath_threshold_test$oath_spend_flag) / sum(oath_threshold_test$count))
# 
# # get percentage of sites that model predicted correctly based on dsp specific actual data
# oath_actual_test_number <- round((sum(oath_threshold_test$exists_flag) / sum(oath_threshold_test$count)), 3)
# 
# # get percentage of sites that model predicted correctly based on entire actual data (random probability)
# oath_overall_actual_test_number <- (sum(oath_threshold_test$exists_flag) / length(older_data$site_name))
# 
# # ratio
# oath_ratio <- oath_actual_test_number / oath_overall_actual_test_number
# 
# # combine fields
# oath_data <- c("oath", oath_threshold_test_number, oath_actual_test_number, oath_overall_actual_test_number, oath_ratio)
# 
# rm(oath_2019, oath_test, rf, site_names, oath_threshold_test, oath_threshold_test_number, oath_actual_test_number, 
#    oath_overall_actual_test_number, oath_ratio)
# 
# 
# # Amobee testing data set
# 
# 
# # sites amobee is not spending on in newer data set
# amobee_2019 <- overall[which(overall$amobee_spend_flag == 1),]
# amobee_2019 <- amobee_2019$site_name
# amobee_2019 <- as.list(amobee_2019)
# 
# # testing data set
# amobee_test <- older_data
# 
# # load amobee RF for testing
# rf <- readRDS(file = "~/r_files/spo_machine_learning_models/rf_rds_files/rf_amobee.RDS")
# 
# amobee_test$site_name <- NULL
# amobee_test$tot_amobee_spend_usd <- NULL
# 
# # run model on old data
# amobee_test$spo_probability <- round(predict(rf, newdata = amobee_test), 2)
# 
# # put site name back in
# site_names <- older_data
# site_names <- site_names[,c(1)]
# amobee_test <- cbind(amobee_test, site_names)
# 
# # threshold test
# amobee_threshold_test <- amobee_test
# amobee_threshold_test <- amobee_threshold_test[which(amobee_threshold_test$spo_probability > .80), ]
# amobee_threshold_test$count <- 1
# 
# # add in column to see if sites in old data wound up going away in new data
# amobee_threshold_test$exists_flag <- ifelse(amobee_threshold_test$site_name %in% amobee_2019, 0, 1)
# 
# # get percentage of sites that model predicted correctly at threshold level
# amobee_threshold_test_number <- (sum(amobee_threshold_test$amobee_spend_flag) / sum(amobee_threshold_test$count))
# 
# # get percentage of sites that model predicted correctly based on dsp specific actual data
# amobee_actual_test_number <- round((sum(amobee_threshold_test$exists_flag) / sum(amobee_threshold_test$count)), 3)
# 
# # get percentage of sites that model predicted correctly based on entire actual data (random probability)
# amobee_overall_actual_test_number <- (sum(amobee_threshold_test$exists_flag) / length(older_data$site_name))
# 
# # ratio
# amobee_ratio <- amobee_actual_test_number / amobee_overall_actual_test_number
# 
# # combine fields
# amobee_data <- c("amobee", amobee_threshold_test_number, amobee_actual_test_number, amobee_overall_actual_test_number, amobee_ratio)
# 
# rm(amobee_2019, amobee_test, rf, site_names, amobee_threshold_test, amobee_threshold_test_number, amobee_actual_test_number, 
#    amobee_overall_actual_test_number, amobee_ratio)
# 
# 
# # MM testing data set
# 
# 
# # sites mm is not spending on in newer data set
# mm_2019 <- overall[which(overall$mm_spend_flag == 1),]
# mm_2019 <- mm_2019$site_name
# mm_2019 <- as.list(mm_2019)
# 
# # testing data set
# mm_test <- older_data
# 
# # load mm RF for testing
# rf <- readRDS(file = "~/r_files/spo_machine_learning_models/rf_rds_files/rf_mm.RDS")
# 
# mm_test$site_name <- NULL
# mm_test$tot_mm_spend_usd <- NULL
# 
# # run model on old data
# mm_test$spo_probability <- round(predict(rf, newdata = mm_test), 2)
# 
# # put site name back in
# site_names <- older_data
# site_names <- site_names[,c(1)]
# mm_test <- cbind(mm_test, site_names)
# 
# # threshold test
# mm_threshold_test <- mm_test
# mm_threshold_test <- mm_threshold_test[which(mm_threshold_test$spo_probability > .80), ]
# mm_threshold_test$count <- 1
# 
# # add in column to see if sites in old data wound up going away in new data
# mm_threshold_test$exists_flag <- ifelse(mm_threshold_test$site_name %in% mm_2019, 0, 1)
# 
# # get percentage of sites that model predicted correctly at threshold level
# mm_threshold_test_number <- (sum(mm_threshold_test$mm_spend_flag) / sum(mm_threshold_test$count))
# 
# # get percentage of sites that model predicted correctly based on dsp specific actual data
# mm_actual_test_number <- round((sum(mm_threshold_test$exists_flag) / sum(mm_threshold_test$count)), 3)
# 
# # get percentage of sites that model predicted correctly based on entire actual data (random probability)
# mm_overall_actual_test_number <- (sum(mm_threshold_test$exists_flag) / length(older_data$site_name))
# 
# # ratio
# mm_ratio <- mm_actual_test_number / mm_overall_actual_test_number
# 
# # combine fields
# mm_data <- c("mm", mm_threshold_test_number, mm_actual_test_number, mm_overall_actual_test_number, mm_ratio)
# 
# rm(mm_2019, mm_test, rf, site_names, mm_threshold_test, mm_threshold_test_number, mm_actual_test_number, 
#    mm_overall_actual_test_number, mm_ratio)
# 
# 
# 
# # A9 testing data set
#
# 
# # sites a9 is not spending on in newer data set
# a9_2019 <- overall[which(overall$a9_spend_flag == 1),]
# a9_2019 <- a9_2019$site_name
# a9_2019 <- as.list(a9_2019)
# 
# # testing data set
# a9_test <- older_data
# 
# # load a9 RF for testing
# rf <- readRDS(file = "~/r_files/spo_machine_learning_models/rf_rds_files/rf_a9.RDS")
# 
# a9_test$site_name <- NULL
# a9_test$tot_a9_spend_usd <- NULL
# 
# # run model on old data
# a9_test$spo_probability <- round(predict(rf, newdata = a9_test), 2)
# 
# # put site name back in
# site_names <- older_data
# site_names <- site_names[,c(1)]
# a9_test <- cbind(a9_test, site_names)
# 
# # threshold test
# a9_threshold_test <- a9_test
# a9_threshold_test <- a9_threshold_test[which(a9_threshold_test$spo_probability > .80), ]
# a9_threshold_test$count <- 1
# 
# # add in column to see if sites in old data wound up going away in new data
# a9_threshold_test$exists_flag <- ifelse(a9_threshold_test$site_name %in% a9_2019, 0, 1)
# 
# # get percentage of sites that model predicted correctly at threshold level
# a9_threshold_test_number <- (sum(a9_threshold_test$a9_spend_flag) / sum(a9_threshold_test$count))
# 
# # get percentage of sites that model predicted correctly based on dsp specific actual data
# a9_actual_test_number <- round((sum(a9_threshold_test$exists_flag) / sum(a9_threshold_test$count)), 3)
# 
# # get percentage of sites that model predicted correctly based on entire actual data (random probability)
# a9_overall_actual_test_number <- (sum(a9_threshold_test$exists_flag) / length(older_data$site_name))
# 
# # ratio
# a9_ratio <- a9_actual_test_number / a9_overall_actual_test_number
# 
# # combine fields
# a9_data <- c("a9", a9_threshold_test_number, a9_actual_test_number, a9_overall_actual_test_number, a9_ratio)
# 
# rm(a9_2019, a9_test, rf, site_names, a9_threshold_test, a9_threshold_test_number, a9_actual_test_number, 
#    a9_overall_actual_test_number, a9_ratio)
# 
#
# # Criteo testing data set
# 
# 
# # sites criteo is not spending on in newer data set
# criteo_2019 <- overall[which(overall$criteo_spend_flag == 1),]
# criteo_2019 <- criteo_2019$site_name
# criteo_2019 <- as.list(criteo_2019)
# 
# # testing data set
# criteo_test <- older_data
# 
# # load criteo RF for testing
# rf <- readRDS(file = "~/r_files/spo_machine_learning_models/rf_rds_files/rf_criteo.RDS")
# 
# criteo_test$site_name <- NULL
# criteo_test$tot_criteo_spend_usd <- NULL
# 
# # run model on old data
# criteo_test$spo_probability <- round(predict(rf, newdata = criteo_test), 2)
# 
# # put site name back in
# site_names <- older_data
# site_names <- site_names[,c(1)]
# criteo_test <- cbind(criteo_test, site_names)
# 
# # threshold test
# criteo_threshold_test <- criteo_test
# criteo_threshold_test <- criteo_threshold_test[which(criteo_threshold_test$spo_probability > .80), ]
# criteo_threshold_test$count <- 1
# 
# # add in column to see if sites in old data wound up going away in new data
# criteo_threshold_test$exists_flag <- ifelse(criteo_threshold_test$site_name %in% criteo_2019, 0, 1)
# 
# # get percentage of sites that model predicted correctly at threshold level
# criteo_threshold_test_number <- (sum(criteo_threshold_test$criteo_spend_flag) / sum(criteo_threshold_test$count))
# 
# # get percentage of sites that model predicted correctly based on dsp specific actual data
# criteo_actual_test_number <- round((sum(criteo_threshold_test$exists_flag) / sum(criteo_threshold_test$count)), 3)
# 
# # get percentage of sites that model predicted correctly based on entire actual data (random probability)
# criteo_overall_actual_test_number <- (sum(criteo_threshold_test$exists_flag) / length(older_data$site_name))
# 
# # ratio
# criteo_ratio <- criteo_actual_test_number / criteo_overall_actual_test_number
# 
# # combine fields
# criteo_data <- c("criteo", criteo_threshold_test_number, criteo_actual_test_number, criteo_overall_actual_test_number, criteo_ratio)
# 
# rm(criteo_2019, criteo_test, rf, site_names, criteo_threshold_test, criteo_threshold_test_number, criteo_actual_test_number, 
#    criteo_overall_actual_test_number, criteo_ratio)
# 
# 
# # SimpliFi testing data set
# 
# 
# # sites smplfi is not spending on in newer data set
# smplfi_2019 <- overall[which(overall$smplfi_spend_flag == 1),]
# smplfi_2019 <- smplfi_2019$site_name
# smplfi_2019 <- as.list(smplfi_2019)
# 
# # testing data set
# smplfi_test <- older_data
# 
# # load smplfi RF for testing
# rf <- readRDS(file = "~/r_files/spo_machine_learning_models/rf_rds_files/rf_smplfi.RDS")
# 
# smplfi_test$site_name <- NULL
# smplfi_test$tot_smplfi_spend_usd <- NULL
# 
# # run model on old data
# smplfi_test$spo_probability <- round(predict(rf, newdata = smplfi_test), 2)
# 
# # put site name back in
# site_names <- older_data
# site_names <- site_names[,c(1)]
# smplfi_test <- cbind(smplfi_test, site_names)
# 
# # threshold test
# smplfi_threshold_test <- smplfi_test
# smplfi_threshold_test <- smplfi_threshold_test[which(smplfi_threshold_test$spo_probability > .80), ]
# smplfi_threshold_test$count <- 1
# 
# # add in column to see if sites in old data wound up going away in new data
# smplfi_threshold_test$exists_flag <- ifelse(smplfi_threshold_test$site_name %in% smplfi_2019, 0, 1)
# 
# # get percentage of sites that model predicted correctly at threshold level
# smplfi_threshold_test_number <- (sum(smplfi_threshold_test$smplfi_spend_flag) / sum(smplfi_threshold_test$count))
# 
# # get percentage of sites that model predicted correctly based on dsp specific actual data
# smplfi_actual_test_number <- round((sum(smplfi_threshold_test$exists_flag) / sum(smplfi_threshold_test$count)), 3)
# 
# # get percentage of sites that model predicted correctly based on entire actual data (random probability)
# smplfi_overall_actual_test_number <- (sum(smplfi_threshold_test$exists_flag) / length(older_data$site_name))
# 
# # ratio
# smplfi_ratio <- smplfi_actual_test_number / smplfi_overall_actual_test_number
# 
# # combine fields
# smplfi_data <- c("smplfi", smplfi_threshold_test_number, smplfi_actual_test_number, smplfi_overall_actual_test_number, smplfi_ratio)
# 
# rm(smplfi_2019, smplfi_test, rf, site_names, smplfi_threshold_test, smplfi_threshold_test_number, smplfi_actual_test_number, 
#    smplfi_overall_actual_test_number, smplfi_ratio)
# 
# 
# # Basis testing data set
# 
# 
# # sites basis is not spending on in newer data set
# basis_2019 <- overall[which(overall$basis_spend_flag == 1),]
# basis_2019 <- basis_2019$site_name
# basis_2019 <- as.list(basis_2019)
# 
# # testing data set
# basis_test <- older_data
# 
# # load basis RF for testing
# rf <- readRDS(file = "~/r_files/spo_machine_learning_models/rf_rds_files/rf_basis.RDS")
# 
# basis_test$site_name <- NULL
# basis_test$tot_basis_spend_usd <- NULL
# 
# # run model on old data
# basis_test$spo_probability <- round(predict(rf, newdata = basis_test), 2)
# 
# # put site name back in
# site_names <- older_data
# site_names <- site_names[,c(1)]
# basis_test <- cbind(basis_test, site_names)
# 
# # threshold test
# basis_threshold_test <- basis_test
# basis_threshold_test <- basis_threshold_test[which(basis_threshold_test$spo_probability > .80), ]
# basis_threshold_test$count <- 1
# 
# # add in column to see if sites in old data wound up going away in new data
# basis_threshold_test$exists_flag <- ifelse(basis_threshold_test$site_name %in% basis_2019, 0, 1)
# 
# # get percentage of sites that model predicted correctly at threshold level
# basis_threshold_test_number <- (sum(basis_threshold_test$basis_spend_flag) / sum(basis_threshold_test$count))
# 
# # get percentage of sites that model predicted correctly based on dsp specific actual data
# basis_actual_test_number <- round((sum(basis_threshold_test$exists_flag) / sum(basis_threshold_test$count)), 3)
# 
# # get percentage of sites that model predicted correctly based on entire actual data (random probability)
# basis_overall_actual_test_number <- (sum(basis_threshold_test$exists_flag) / length(older_data$site_name))
# 
# # ratio
# basis_ratio <- basis_actual_test_number / basis_overall_actual_test_number
# 
# # combine fields
# basis_data <- c("basis", basis_threshold_test_number, basis_actual_test_number, basis_overall_actual_test_number, basis_ratio)
# 
# rm(basis_2019, basis_test, rf, site_names, basis_threshold_test, basis_threshold_test_number, basis_actual_test_number, 
#    basis_overall_actual_test_number, basis_ratio)
# 
# 
# # Adroll testing data set
# 
# 
# # sites adroll is not spending on in newer data set
# adroll_2019 <- overall[which(overall$adroll_spend_flag == 1),]
# adroll_2019 <- adroll_2019$site_name
# adroll_2019 <- as.list(adroll_2019)
# 
# # testing data set
# adroll_test <- older_data
# 
# # load adroll RF for testing
# rf <- readRDS(file = "~/r_files/spo_machine_learning_models/rf_rds_files/rf_adroll.RDS")
# 
# adroll_test$site_name <- NULL
# adroll_test$tot_adroll_spend_usd <- NULL
# 
# # run model on old data
# adroll_test$spo_probability <- round(predict(rf, newdata = adroll_test), 2)
# 
# # put site name back in
# site_names <- older_data
# site_names <- site_names[,c(1)]
# adroll_test <- cbind(adroll_test, site_names)
# 
# # threshold test
# adroll_threshold_test <- adroll_test
# adroll_threshold_test <- adroll_threshold_test[which(adroll_threshold_test$spo_probability > .80), ]
# adroll_threshold_test$count <- 1
# 
# # add in column to see if sites in old data wound up going away in new data
# adroll_threshold_test$exists_flag <- ifelse(adroll_threshold_test$site_name %in% adroll_2019, 0, 1)
# 
# # get percentage of sites that model predicted correctly at threshold level
# adroll_threshold_test_number <- (sum(adroll_threshold_test$adroll_spend_flag) / sum(adroll_threshold_test$count))
# 
# # get percentage of sites that model predicted correctly based on dsp specific actual data
# adroll_actual_test_number <- round((sum(adroll_threshold_test$exists_flag) / sum(adroll_threshold_test$count)), 3)
# 
# # get percentage of sites that model predicted correctly based on entire actual data (random probability)
# adroll_overall_actual_test_number <- (sum(adroll_threshold_test$exists_flag) / length(older_data$site_name))
# 
# # ratio
# adroll_ratio <- adroll_actual_test_number / adroll_overall_actual_test_number
# 
# # combine fields
# adroll_data <- c("adroll", adroll_threshold_test_number, adroll_actual_test_number, adroll_overall_actual_test_number, adroll_ratio)
# 
# rm(adroll_2019, adroll_test, rf, site_names, adroll_threshold_test, adroll_threshold_test_number, adroll_actual_test_number, 
#    adroll_overall_actual_test_number, adroll_ratio)
# 
# 
# # Beeswax testing data set
# 
# 
# # sites beeswax is not spending on in newer data set
# beeswax_2019 <- overall[which(overall$beeswax_spend_flag == 1),]
# beeswax_2019 <- beeswax_2019$site_name
# beeswax_2019 <- as.list(beeswax_2019)
# 
# # testing data set
# beeswax_test <- older_data
# 
# # load beeswax RF for testing
# rf <- readRDS(file = "~/r_files/spo_machine_learning_models/rf_rds_files/rf_beeswax.RDS")
# 
# beeswax_test$site_name <- NULL
# beeswax_test$tot_beeswax_spend_usd <- NULL
# 
# # run model on old data
# beeswax_test$spo_probability <- round(predict(rf, newdata = beeswax_test), 2)
# 
# # put site name back in
# site_names <- older_data
# site_names <- site_names[,c(1)]
# beeswax_test <- cbind(beeswax_test, site_names)
# 
# # threshold test
# beeswax_threshold_test <- beeswax_test
# beeswax_threshold_test <- beeswax_threshold_test[which(beeswax_threshold_test$spo_probability > .80), ]
# beeswax_threshold_test$count <- 1
# 
# # add in column to see if sites in old data wound up going away in new data
# beeswax_threshold_test$exists_flag <- ifelse(beeswax_threshold_test$site_name %in% beeswax_2019, 0, 1)
# 
# # get percentage of sites that model predicted correctly at threshold level
# beeswax_threshold_test_number <- (sum(beeswax_threshold_test$beeswax_spend_flag) / sum(beeswax_threshold_test$count))
# 
# # get percentage of sites that model predicted correctly based on dsp specific actual data
# beeswax_actual_test_number <- round((sum(beeswax_threshold_test$exists_flag) / sum(beeswax_threshold_test$count)), 3)
# 
# # get percentage of sites that model predicted correctly based on entire actual data (random probability)
# beeswax_overall_actual_test_number <- (sum(beeswax_threshold_test$exists_flag) / length(older_data$site_name))
# 
# # ratio
# beeswax_ratio <- beeswax_actual_test_number / beeswax_overall_actual_test_number
# 
# # combine fields
# beeswax_data <- c("beeswax", beeswax_threshold_test_number, beeswax_actual_test_number, beeswax_overall_actual_test_number, beeswax_ratio)
# 
# rm(beeswax_2019, beeswax_test, rf, site_names, beeswax_threshold_test, beeswax_threshold_test_number, beeswax_actual_test_number, 
#    beeswax_overall_actual_test_number, beeswax_ratio)
# 
# 
# # Quantcast testing data set
# 
# 
# # sites quantcast is not spending on in newer data set
# quantcast_2019 <- overall[which(overall$quantcast_spend_flag == 1),]
# quantcast_2019 <- quantcast_2019$site_name
# quantcast_2019 <- as.list(quantcast_2019)
# 
# # testing data set
# quantcast_test <- older_data
# 
# # load quantcast RF for testing
# rf <- readRDS(file = "~/r_files/spo_machine_learning_models/rf_rds_files/rf_quantcast.RDS")
# 
# quantcast_test$site_name <- NULL
# quantcast_test$tot_quantcast_spend_usd <- NULL
# 
# # run model on old data
# quantcast_test$spo_probability <- round(predict(rf, newdata = quantcast_test), 2)
# 
# # put site name back in
# site_names <- older_data
# site_names <- site_names[,c(1)]
# quantcast_test <- cbind(quantcast_test, site_names)
# 
# # threshold test
# quantcast_threshold_test <- quantcast_test
# quantcast_threshold_test <- quantcast_threshold_test[which(quantcast_threshold_test$spo_probability > .80), ]
# quantcast_threshold_test$count <- 1
# 
# # add in column to see if sites in old data wound up going away in new data
# quantcast_threshold_test$exists_flag <- ifelse(quantcast_threshold_test$site_name %in% quantcast_2019, 0, 1)
# 
# # get percentage of sites that model predicted correctly at threshold level
# quantcast_threshold_test_number <- (sum(quantcast_threshold_test$quantcast_spend_flag) / sum(quantcast_threshold_test$count))
# 
# # get percentage of sites that model predicted correctly based on dsp specific actual data
# quantcast_actual_test_number <- round((sum(quantcast_threshold_test$exists_flag) / sum(quantcast_threshold_test$count)), 3)
# 
# # get percentage of sites that model predicted correctly based on entire actual data (random probability)
# quantcast_overall_actual_test_number <- (sum(quantcast_threshold_test$exists_flag) / length(older_data$site_name))
# 
# # ratio
# quantcast_ratio <- quantcast_actual_test_number / quantcast_overall_actual_test_number
# 
# # combine fields
# quantcast_data <- c("quantcast", quantcast_threshold_test_number, quantcast_actual_test_number, quantcast_overall_actual_test_number, quantcast_ratio)
# 
# rm(quantcast_2019, quantcast_test, rf, site_names, quantcast_threshold_test, quantcast_threshold_test_number, quantcast_actual_test_number, 
#    quantcast_overall_actual_test_number, quantcast_ratio)
# 
# 
# # Dataxu testing data set
#
# 
# # sites dataxu is not spending on in newer data set
# dataxu_2019 <- overall[which(overall$dataxu_spend_flag == 1),]
# dataxu_2019 <- dataxu_2019$site_name
# dataxu_2019 <- as.list(dataxu_2019)
# 
# # testing data set
# dataxu_test <- older_data
# 
# # load dataxu RF for testing
# rf <- readRDS(file = "~/r_files/spo_machine_learning_models/rf_rds_files/rf_dataxu.RDS")
# 
# dataxu_test$site_name <- NULL
# dataxu_test$tot_dataxu_spend_usd <- NULL
# 
# # run model on old data
# dataxu_test$spo_probability <- round(predict(rf, newdata = dataxu_test), 2)
# 
# # put site name back in
# site_names <- older_data
# site_names <- site_names[,c(1)]
# dataxu_test <- cbind(dataxu_test, site_names)
# 
# # threshold test
# dataxu_threshold_test <- dataxu_test
# dataxu_threshold_test <- dataxu_threshold_test[which(dataxu_threshold_test$spo_probability > .80), ]
# dataxu_threshold_test$count <- 1
# 
# # add in column to see if sites in old data wound up going away in new data
# dataxu_threshold_test$exists_flag <- ifelse(dataxu_threshold_test$site_name %in% dataxu_2019, 0, 1)
# 
# # get percentage of sites that model predicted correctly at threshold level
# dataxu_threshold_test_number <- (sum(dataxu_threshold_test$dataxu_spend_flag) / sum(dataxu_threshold_test$count))
# 
# # get percentage of sites that model predicted correctly based on dsp specific actual data
# dataxu_actual_test_number <- round((sum(dataxu_threshold_test$exists_flag) / sum(dataxu_threshold_test$count)), 3)
# 
# # get percentage of sites that model predicted correctly based on entire actual data (random probability)
# dataxu_overall_actual_test_number <- (sum(dataxu_threshold_test$exists_flag) / length(older_data$site_name))
# 
# # ratio
# dataxu_ratio <- dataxu_actual_test_number / dataxu_overall_actual_test_number
# 
# # combine fields
# dataxu_data <- c("dataxu", dataxu_threshold_test_number, dataxu_actual_test_number, dataxu_overall_actual_test_number, dataxu_ratio)
# 
# rm(dataxu_2019, dataxu_test, rf, site_names, dataxu_threshold_test, dataxu_threshold_test_number, dataxu_actual_test_number, 
#    dataxu_overall_actual_test_number, dataxu_ratio)
# 
# 
# # Adform testing data set
# 
# 
# # sites adform is not spending on in newer data set
# adform_2019 <- overall[which(overall$adform_spend_flag == 1),]
# adform_2019 <- adform_2019$site_name
# adform_2019 <- as.list(adform_2019)
# 
# # testing data set
# adform_test <- older_data
# 
# # load adform RF for testing
# rf <- readRDS(file = "~/r_files/spo_machine_learning_models/rf_rds_files/rf_adform.RDS")
# 
# adform_test$site_name <- NULL
# adform_test$tot_adform_spend_usd <- NULL
# 
# # run model on old data
# adform_test$spo_probability <- round(predict(rf, newdata = adform_test), 2)
# 
# # put site name back in
# site_names <- older_data
# site_names <- site_names[,c(1)]
# adform_test <- cbind(adform_test, site_names)
# 
# # threshold test
# adform_threshold_test <- adform_test
# adform_threshold_test <- adform_threshold_test[which(adform_threshold_test$spo_probability > .80), ]
# adform_threshold_test$count <- 1
# 
# # add in column to see if sites in old data wound up going away in new data
# adform_threshold_test$exists_flag <- ifelse(adform_threshold_test$site_name %in% adform_2019, 0, 1)
# 
# # get percentage of sites that model predicted correctly at threshold level
# adform_threshold_test_number <- (sum(adform_threshold_test$adform_spend_flag) / sum(adform_threshold_test$count))
# 
# # get percentage of sites that model predicted correctly based on dsp specific actual data
# adform_actual_test_number <- round((sum(adform_threshold_test$exists_flag) / sum(adform_threshold_test$count)), 3)
# 
# # get percentage of sites that model predicted correctly based on entire actual data (random probability)
# adform_overall_actual_test_number <- (sum(adform_threshold_test$exists_flag) / length(older_data$site_name))
# 
# # ratio
# adform_ratio <- adform_actual_test_number / adform_overall_actual_test_number
# 
# # combine fields
# adform_data <- c("adform", adform_threshold_test_number, adform_actual_test_number, adform_overall_actual_test_number, adform_ratio)
# 
# rm(adform_2019, adform_test, rf, site_names, adform_threshold_test, adform_threshold_test_number, adform_actual_test_number, 
#    adform_overall_actual_test_number, adform_ratio)
# 
# 
# # Adtheorent testing data set
# 
# 
# # sites adtheorent is not spending on in newer data set
# adtheorent_2019 <- overall[which(overall$adtheorent_spend_flag == 1),]
# adtheorent_2019 <- adtheorent_2019$site_name
# adtheorent_2019 <- as.list(adtheorent_2019)
# 
# # testing data set
# adtheorent_test <- older_data
# 
# # load adtheorent RF for testing
# rf <- readRDS(file = "~/r_files/spo_machine_learning_models/rf_rds_files/rf_adtheorent.RDS")
# 
# adtheorent_test$site_name <- NULL
# adtheorent_test$tot_adtheorent_spend_usd <- NULL
# 
# # run model on old data
# adtheorent_test$spo_probability <- round(predict(rf, newdata = adtheorent_test), 2)
# 
# # put site name back in
# site_names <- older_data
# site_names <- site_names[,c(1)]
# adtheorent_test <- cbind(adtheorent_test, site_names)
# 
# # threshold test
# adtheorent_threshold_test <- adtheorent_test
# adtheorent_threshold_test <- adtheorent_threshold_test[which(adtheorent_threshold_test$spo_probability > .80), ]
# adtheorent_threshold_test$count <- 1
# 
# # add in column to see if sites in old data wound up going away in new data
# adtheorent_threshold_test$exists_flag <- ifelse(adtheorent_threshold_test$site_name %in% adtheorent_2019, 0, 1)
# 
# # get percentage of sites that model predicted correctly at threshold level
# adtheorent_threshold_test_number <- (sum(adtheorent_threshold_test$adtheorent_spend_flag) / sum(adtheorent_threshold_test$count))
# 
# # get percentage of sites that model predicted correctly based on dsp specific actual data
# adtheorent_actual_test_number <- round((sum(adtheorent_threshold_test$exists_flag) / sum(adtheorent_threshold_test$count)), 3)
# 
# # get percentage of sites that model predicted correctly based on entire actual data (random probability)
# adtheorent_overall_actual_test_number <- (sum(adtheorent_threshold_test$exists_flag) / length(older_data$site_name))
# 
# # ratio
# adtheorent_ratio <- adtheorent_actual_test_number / adtheorent_overall_actual_test_number
# 
# # combine fields
# adtheorent_data <- c("adtheorent", adtheorent_threshold_test_number, adtheorent_actual_test_number, adtheorent_overall_actual_test_number, adtheorent_ratio)
# 
# rm(adtheorent_2019, adtheorent_test, rf, site_names, adtheorent_threshold_test, adtheorent_threshold_test_number, adtheorent_actual_test_number, 
#    adtheorent_overall_actual_test_number, adtheorent_ratio)
# 
# 
# # Bidswitch testing data set
# 
# 
# # sites bidswitch is not spending on in newer data set
# bidswitch_2019 <- overall[which(overall$bidswitch_spend_flag == 1),]
# bidswitch_2019 <- bidswitch_2019$site_name
# bidswitch_2019 <- as.list(bidswitch_2019)
# 
# # testing data set
# bidswitch_test <- older_data
# 
# # load bidswitch RF for testing
# rf <- readRDS(file = "~/r_files/spo_machine_learning_models/rf_rds_files/rf_bidswitch.RDS")
# 
# bidswitch_test$site_name <- NULL
# bidswitch_test$tot_bidswitch_spend_usd <- NULL
# 
# # run model on old data
# bidswitch_test$spo_probability <- round(predict(rf, newdata = bidswitch_test), 2)
# 
# # put site name back in
# site_names <- older_data
# site_names <- site_names[,c(1)]
# bidswitch_test <- cbind(bidswitch_test, site_names)
# 
# # threshold test
# bidswitch_threshold_test <- bidswitch_test
# bidswitch_threshold_test <- bidswitch_threshold_test[which(bidswitch_threshold_test$spo_probability > .80), ]
# bidswitch_threshold_test$count <- 1
# 
# # add in column to see if sites in old data wound up going away in new data
# bidswitch_threshold_test$exists_flag <- ifelse(bidswitch_threshold_test$site_name %in% bidswitch_2019, 0, 1)
# 
# # get percentage of sites that model predicted correctly at threshold level
# bidswitch_threshold_test_number <- (sum(bidswitch_threshold_test$bidswitch_spend_flag) / sum(bidswitch_threshold_test$count))
# 
# # get percentage of sites that model predicted correctly based on dsp specific actual data
# bidswitch_actual_test_number <- round((sum(bidswitch_threshold_test$exists_flag) / sum(bidswitch_threshold_test$count)), 3)
# 
# # get percentage of sites that model predicted correctly based on entire actual data (random probability)
# bidswitch_overall_actual_test_number <- (sum(bidswitch_threshold_test$exists_flag) / length(older_data$site_name))
# 
# # ratio
# bidswitch_ratio <- bidswitch_actual_test_number / bidswitch_overall_actual_test_number
# 
# # combine fields
# bidswitch_data <- c("bidswitch", bidswitch_threshold_test_number, bidswitch_actual_test_number, bidswitch_overall_actual_test_number, bidswitch_ratio)
# 
# rm(bidswitch_2019, bidswitch_test, rf, site_names, bidswitch_threshold_test, bidswitch_threshold_test_number, bidswitch_actual_test_number, 
#    bidswitch_overall_actual_test_number, bidswitch_ratio)
# 
# 
# # Conversant testing data set
# 
# 
# # sites conversant is not spending on in newer data set
# conversant_2019 <- overall[which(overall$conversant_spend_flag == 1),]
# conversant_2019 <- conversant_2019$site_name
# conversant_2019 <- as.list(conversant_2019)
# 
# # testing data set
# conversant_test <- older_data
# 
# # load conversant RF for testing
# rf <- readRDS(file = "~/r_files/spo_machine_learning_models/rf_rds_files/rf_conversant.RDS")
# 
# conversant_test$site_name <- NULL
# conversant_test$tot_conversant_spend_usd <- NULL
# 
# # run model on old data
# conversant_test$spo_probability <- round(predict(rf, newdata = conversant_test), 2)
# 
# # put site name back in
# site_names <- older_data
# site_names <- site_names[,c(1)]
# conversant_test <- cbind(conversant_test, site_names)
# 
# # threshold test
# conversant_threshold_test <- conversant_test
# conversant_threshold_test <- conversant_threshold_test[which(conversant_threshold_test$spo_probability > .80), ]
# conversant_threshold_test$count <- 1
# 
# # add in column to see if sites in old data wound up going away in new data
# conversant_threshold_test$exists_flag <- ifelse(conversant_threshold_test$site_name %in% conversant_2019, 0, 1)
# 
# # get percentage of sites that model predicted correctly at threshold level
# conversant_threshold_test_number <- (sum(conversant_threshold_test$conversant_spend_flag) / sum(conversant_threshold_test$count))
# 
# # get percentage of sites that model predicted correctly based on dsp specific actual data
# conversant_actual_test_number <- round((sum(conversant_threshold_test$exists_flag) / sum(conversant_threshold_test$count)), 3)
# 
# # get percentage of sites that model predicted correctly based on entire actual data (random probability)
# conversant_overall_actual_test_number <- (sum(conversant_threshold_test$exists_flag) / length(older_data$site_name))
# 
# # ratio
# conversant_ratio <- conversant_actual_test_number / conversant_overall_actual_test_number
# 
# # combine fields
# conversant_data <- c("conversant", conversant_threshold_test_number, conversant_actual_test_number, conversant_overall_actual_test_number, conversant_ratio)
# 
# rm(conversant_2019, conversant_test, rf, site_names, conversant_threshold_test, conversant_threshold_test_number, conversant_actual_test_number, 
#    conversant_overall_actual_test_number, conversant_ratio)
# 
# 
# # Liftoff testing data set
# 
# 
# # sites liftoff is not spending on in newer data set
# liftoff_2019 <- overall[which(overall$liftoff_spend_flag == 1),]
# liftoff_2019 <- liftoff_2019$site_name
# liftoff_2019 <- as.list(liftoff_2019)
# 
# # testing data set
# liftoff_test <- older_data
# 
# # load liftoff RF for testing
# rf <- readRDS(file = "~/r_files/spo_machine_learning_models/rf_rds_files/rf_liftoff.RDS")
# 
# liftoff_test$site_name <- NULL
# liftoff_test$tot_liftoff_spend_usd <- NULL
# 
# # run model on old data
# liftoff_test$spo_probability <- round(predict(rf, newdata = liftoff_test), 2)
# 
# # put site name back in
# site_names <- older_data
# site_names <- site_names[,c(1)]
# liftoff_test <- cbind(liftoff_test, site_names)
# 
# # threshold test
# liftoff_threshold_test <- liftoff_test
# liftoff_threshold_test <- liftoff_threshold_test[which(liftoff_threshold_test$spo_probability > .80), ]
# liftoff_threshold_test$count <- 1
# 
# # add in column to see if sites in old data wound up going away in new data
# liftoff_threshold_test$exists_flag <- ifelse(liftoff_threshold_test$site_name %in% liftoff_2019, 0, 1)
# 
# # get percentage of sites that model predicted correctly at threshold level
# liftoff_threshold_test_number <- (sum(liftoff_threshold_test$liftoff_spend_flag) / sum(liftoff_threshold_test$count))
# 
# # get percentage of sites that model predicted correctly based on dsp specific actual data
# liftoff_actual_test_number <- round((sum(liftoff_threshold_test$exists_flag) / sum(liftoff_threshold_test$count)), 3)
# 
# # get percentage of sites that model predicted correctly based on entire actual data (random probability)
# liftoff_overall_actual_test_number <- (sum(liftoff_threshold_test$exists_flag) / length(older_data$site_name))
# 
# # ratio
# liftoff_ratio <- liftoff_actual_test_number / liftoff_overall_actual_test_number
# 
# # combine fields
# liftoff_data <- c("liftoff", liftoff_threshold_test_number, liftoff_actual_test_number, liftoff_overall_actual_test_number, liftoff_ratio)
# 
# rm(liftoff_2019, liftoff_test, rf, site_names, liftoff_threshold_test, liftoff_threshold_test_number, liftoff_actual_test_number, 
#    liftoff_overall_actual_test_number, liftoff_ratio)
# 
# # combine all data
# results <- data.frame(matrix(ncol = 5, nrow = 0))
# 
# results <- rbind(a9_data, adform_data, adobe_data, adroll_data, adtheorent_data, amobee_data, basis_data, beeswax_data,
#                  bidswitch_data, conversant_data, criteo_data, dataxu_data, dbm_data, liftoff_data, mm_data,
#                  oath_data, quantcast_data, smplfi_data, ttd_data)
# 
# colnames(results) <- c("dsp", "dsp_threshold_test_number", "dsp_actual_test_number", "dsp_overall_actual_test_number", "dsp_ratio")
# 
# rownames(results) <- NULL
# 
# write.csv(results, "~/r_files/spo_machine_learning_models/older_spo_results.csv", row.names = F)
# #########################################################################