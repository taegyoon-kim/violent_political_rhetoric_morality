###### author details: Taegyoon Kim, taegyoon@psu.edu
###### purpose: This script fits the incivility classifier developed by Theocharis et al, 2020 to replies to legislators' tweets
###### last edit: 24 Jun 2021



##### packages

library(data.table)
library(quanteda)
library(glmnet)



##### load classifier

source('/Users/taegyoon/Google Drive/diss_morality/script/incivility_functions.R')
load('/Users/taegyoon/Google Drive/diss_morality/data/lasso-classifier.rdata')
load('/Users/taegyoon/Google Drive/diss_morality/data/dfm-file.rdata')



##### fit classifier and produce CSV files for predicted probabilities

for (i in 0:7){
  print(i)
  file_name <- paste0('/Users/taegyoon/Google Drive/diss_morality/reply_text_', i , '.csv')
  reply_text <- fread(file_name)
  nrow(reply_text)
  colnames(reply_text) <- c('tweet_no', 'reply_full_text')
  reply_text <- reply_text[-c(1)]
  reply_text_label_incivil <- predict_incivility(reply_text$reply_full_text, 
                                                 old_dfm = dfm,
                                                 classifier = lasso)
  reply_text_labeled <- cbind(reply_text, reply_text_label_incivil)
  path_incivility_labels <- '/Users/taegyoon/Google Drive/diss_morality/data/reply_labels_incivility/'
  new_file_name <- paste0(path_incivility_labels, 'reply_label_incivility_', i , '.csv')
  write.csv(reply_text_labeled, new_file_name) 
}


