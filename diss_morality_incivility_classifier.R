###### author details: Taegyoon Kim, taegyoon@psu.edu
###### environment: R 4.1.0, macOS BigSur
###### purpose: This script fits the incivility classifier developed by Theocharis et al, 2020 to replies to legislators' tweets
###### last edit: 24 Jun 2021


# packages -------------------------

library(data.table)
library(quanteda)
library(glmnet)


# load classifier -------------------------

paths_classifier <- '/Users/taegyoon/Google Drive/diss_morality/script/'
source(paste0(paths_classifier, 'incivility_functions.R')) # download classifier materials from https://github.com/pablobarbera/incivility-sage-open
load(paste0('lasso-classifier.rdata'))
load(paste0('dfm-file.rdata'))


# fit classifier and produce CSV files for predicted probabilities -------------------------

path_replies <- '/Users/taegyoon/Google Drive/diss_morality/data/reply_labels_incivility/' # the reply files will be shared individually later
for (i in 0:7){
  file_name <- paste0(path_replies, 'reply_text_', i , '.csv') # define file name
  reply_text <- fread(file_name) # read each batch
  colnames(reply_text) <- c('tweet_no', 'reply_full_text')
  reply_text <- reply_text[-c(1)]
  reply_text_label_incivil <- predict_incivility(
    reply_text$reply_full_text, # compute predicted probabilities
    old_dfm = dfm,
    classifier = lasso
    )
  reply_text_labeled <- cbind(reply_text, reply_text_label_incivil)
  path_incivility_labels <- '/Users/taegyoon/Google Drive/diss_morality/data/reply_labels_incivility/'
  new_file_name <- paste0(path_incivility_labels, 'reply_label_incivility_', i , '.csv')
  write.csv(reply_text_labeled, new_file_name) # write as CSV each batch 
  }


