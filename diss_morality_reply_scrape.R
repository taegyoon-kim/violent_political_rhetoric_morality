###### author details: Taegyoon Kim, taegyoon@psu.edu
###### purpose: This scripts scrapes replies to legislators and writes relevant CSV and RData files
###### last edit: 24 Jun 2021



##### packages

lapply(c("academictwitteR", "stringr", "lubridate", "tidyverse"), require, character.only = TRUE)



##### developer credentials (academic track)

api_key <- ''
api_secret <- ''
access_key <- ''
access_secret <- ''
bearer_token <- ''



##### read and divide handle data 

path_handle <- '/Users/taegyoon/Google Drive/diss_morality/data/handles/' 
handles_117th <- read.csv(paste0(path_handle, 'handles_117th.csv'))
handles_117th_rep <- handles_117th[which(handles_117th$chamber=='H'),]
handles_117th_sen <- handles_117th[which(handles_117th$chamber=='S'),]



##### scrape replies and write as csv

nulltona <- function(x) {
  x[sapply(x, is.null)] <- NA
  return(x)}

get_to_tweets_export <- function(x, s_time, e_time){
  replies_all <- get_to_tweets(x,
                              start_tweets = s_time,
                              end_tweets = e_time,
                              bearer_token,
                              bind_tweets = TRUE,
                              verbose = TRUE)
 replies_all$referenced_tweets_na <- nulltona(replies_all$referenced_tweets)
 replies_all$referenced <- is.na(replies_all$referenced_tweets_na)
 replies <- replies_all[which(replies_all$referenced==FALSE),]
 replies$referenced_tweets_spread <- lapply(replies$referenced_tweets, function(x) spread(x, type, id))
 df_reference <- do.call("rbind.fill", replies$referenced_tweets_spread)
 df <- cbind(created_at = replies$created_at, 
             id = replies$id,
             text = replies$text, 
             author_id = replies$author_id, 
             in_reply_to_user_id = replies$in_reply_to_user_id, 
             df_reference, 
             replies$public_metrics)
 path_reply <- ''
 write.csv(df, paste0(s_time, '-', e_time, '-', x,"-Replies.csv"))
 save(replies_all, file = paste0(s_time, '-', e_time, '-', x,"-Replies.RData"))
 return(print('Exported!'))
 }

for (i in 1:length(handles_117th_rep$handle_1)){
  print(paste0(handles_117th_rep$handle_1[i], '(', i,'/', length(handles_117th_rep$handle_1),')'))
  tryCatch(get_to_tweets_export(handles_117th_rep$handle_1[i], "2021-01-03T05:00:00Z", "2021-05-15T05:00:00Z"),
           error = function(e) print(e))
  }



