ADD JAR /home/hduser/hive-0.9.0/lib/hive-serdes-1.0-SNAPSHOT.jar;


CREATE EXTERNAL TABLE IF NOT EXISTS tweet1 (
   id BIGINT,
   created_at STRING,
   source STRING,
   favorited BOOLEAN,
   retweet_count INT,
   retweeted_status STRUCT<
      text:STRING,
      user:STRUCT<screen_name:STRING,name:STRING>>,
   entities STRUCT<
      urls:ARRAY<STRUCT<expanded_url:STRING>>,
      user_mentions:ARRAY<STRUCT<screen_name:STRING,name:STRING>>,
      hashtags:ARRAY<STRUCT<text:STRING>>>,
   text STRING,
   user STRUCT<
      screen_name:STRING,
      name:STRING,
      friends_count:INT,
      followers_count:INT,
      statuses_count:INT,
      verified:BOOLEAN,
      utc_offset:INT,
      time_zone:STRING>,
   in_reply_to_screen_name STRING
) 
ROW FORMAT SERDE 'com.cloudera.hive.serde.JSONSerDe'
LOCATION '/user/flume/tweet1';

-- All the queries below store the data in the locations mentioned before the query. 
-- The syntax insert overwrite local directory 'directory path' performs this operation.

-- The below query gets the total number of tweets from the table tweet1 for that particular batch.
insert overwrite local directory '/home/hduser/eclipse/jee-mars/eclipse/Outputs/Tweet1/count' select count(id) AS Number_of_Tweets from tweet1;

--The below query fetches the number of favourites for that particular key word given in the Flume config file.
insert overwrite local directory '/home/hduser/eclipse/jee-mars/eclipse/Outputs/Tweet1/favourites' select count(favorited) AS Favorited_Tweet_Count from tweet1 GROUP BY favorited;

-- The following query fetches the sources from where tweets are posted.
insert overwrite local directory '/home/hduser/eclipse/jee-mars/eclipse/Outputs/Tweet1/source' select source, count(id) from tweet1 GROUP BY source;

-- The below query gives the user with maximum number of followers in the current batch of data.
insert overwrite local directory '/home/hduser/eclipse/jee-mars/eclipse/Outputs/Tweet1/MaxFollowedUser' select user.screen_name, user.followers_count from tweet1 where user.statuses_count > 50000 limit 1;

--The below query fetches the verified user who has more than 10 thousand followers.
insert overwrite local directory '/home/hduser/eclipse/jee-mars/eclipse/Outputs/Tweet1/Verified' select user.screen_name, user.followers_count from tweet1 where user.verified = true AND user.followers_count > 10000 limit 1;

--The below query gives the tweet with maximum number of retweets.
insert overwrite local directory '/home/hduser/eclipse/jee-mars/eclipse/Outputs/Tweet1/MaxRetweet' select user.screen_name, retweeted_status.text from tweet1 where user.followers_count > 50000 limit 1;

-- The below query fetches the location wise tweets from different time zones.
insert overwrite local directory '/home/hduser/eclipse/jee-mars/eclipse/Outputs/Tweet1/timezone' select user.time_zone, count(user.time_zone) from tweet1 where user.time_zone != ' ' GROUP BY user.time_zone;


ADD JAR /home/hduser/hive-0.9.0/lib/hive-serdes-1.0-SNAPSHOT.jar;


CREATE EXTERNAL TABLE IF NOT EXISTS tweet2 (
   id BIGINT,
   created_at STRING,
   source STRING,
   favorited BOOLEAN,
   retweet_count INT,
   retweeted_status STRUCT<
      text:STRING,
      user:STRUCT<screen_name:STRING,name:STRING>>,
   entities STRUCT<
      urls:ARRAY<STRUCT<expanded_url:STRING>>,
      user_mentions:ARRAY<STRUCT<screen_name:STRING,name:STRING>>,
      hashtags:ARRAY<STRUCT<text:STRING>>>,
   text STRING,
   user STRUCT<
      screen_name:STRING,
      name:STRING,
      friends_count:INT,
      followers_count:INT,
      statuses_count:INT,
      verified:BOOLEAN,
      utc_offset:INT,
      time_zone:STRING>,
   in_reply_to_screen_name STRING
) 
ROW FORMAT SERDE 'com.cloudera.hive.serde.JSONSerDe'
LOCATION '/user/flume/tweet2';

-- All the queries below store the data in the locations mentioned before the query. 
-- The syntax insert overwrite local directory 'directory path' performs this operation.

-- The below query gets the total number of tweets from the table tweet2 for that particular batch.
insert overwrite local directory '/home/hduser/eclipse/jee-mars/eclipse/Outputs/Tweet2/count' select count(id) AS Number_of_Tweets from tweet2;

--The below query fetches the number of favourites for that particular key word given in the Flume config file.
insert overwrite local directory '/home/hduser/eclipse/jee-mars/eclipse/Outputs/Tweet2/favourites' select count(favorited) AS Favorited_Tweet_Count from tweet2 GROUP BY favorited;

-- The following query fetches the sources from where tweets are posted.
insert overwrite local directory '/home/hduser/eclipse/jee-mars/eclipse/Outputs/Tweet2/source' select source, count(id) from tweet2 GROUP BY source;

-- The below query gives the user with maximum number of followers in the current batch of data.
insert overwrite local directory '/home/hduser/eclipse/jee-mars/eclipse/Outputs/Tweet2/MaxFollowedUser' select user.screen_name, user.followers_count from tweet2 where user.statuses_count > 50000 limit 1;

--The below query fetches the verified user who has more than 10 thousand followers.
insert overwrite local directory '/home/hduser/eclipse/jee-mars/eclipse/Outputs/Tweet2/Verified' select user.screen_name, user.followers_count from tweet2 where user.verified = true AND user.followers_count > 10000 limit 1;

--The below query gives the tweet with maximum number of retweets.
insert overwrite local directory '/home/hduser/eclipse/jee-mars/eclipse/Outputs/Tweet2/MaxRetweet' select user.screen_name, retweeted_status.text from tweet2 where user.followers_count > 50000 limit 1;

-- The below query fetches the location wise tweets from different time zones.
insert overwrite local directory '/home/hduser/eclipse/jee-mars/eclipse/Outputs/Tweet2/timezone' select user.time_zone, count(user.time_zone) from tweet2 where user.time_zone != ' ' GROUP BY user.time_zone;


ADD JAR /home/hduser/hive-0.9.0/lib/hive-serdes-1.0-SNAPSHOT.jar;


CREATE EXTERNAL TABLE IF NOT EXISTS tweet3 (
   id BIGINT,
   created_at STRING,
   source STRING,
   favorited BOOLEAN,
   retweet_count INT,
   retweeted_status STRUCT<
      text:STRING,
      user:STRUCT<screen_name:STRING,name:STRING>>,
   entities STRUCT<
      urls:ARRAY<STRUCT<expanded_url:STRING>>,
      user_mentions:ARRAY<STRUCT<screen_name:STRING,name:STRING>>,
      hashtags:ARRAY<STRUCT<text:STRING>>>,
   text STRING,
   user STRUCT<
      screen_name:STRING,
      name:STRING,
      friends_count:INT,
      followers_count:INT,
      statuses_count:INT,
      verified:BOOLEAN,
      utc_offset:INT,
      time_zone:STRING>,
   in_reply_to_screen_name STRING
) 
ROW FORMAT SERDE 'com.cloudera.hive.serde.JSONSerDe'
LOCATION '/user/flume/tweet3';

describe tweet3;

select * from tweet3;

--Next, we will split the text into words using the split() UDF available in Hive. If we use the split() function to split the text as words, it will return an array of values. So, we will create another Hive table and store the -- tweet_id and the array of words.

create table split_words as select id as id,split(text,' ') as words from tweet3;

select * from split_words;

create table tweet_word as select id as id,word from split_words LATERAL VIEW explode(words) w as word;

lateralView: LATERAL VIEW udtf(expression) tableAlias AS columnAlias (',' columnAlias)*fromClause: FROM baseTable (lateralView)

	
select * from tweet_word;

create table dictionary(word string,rating int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

	
LOAD DATA INPATH '/AFINN.txt' into TABLE dictionary;

select * from dictionary;

create table word_join as select tweet_word.id,tweet_word.word,dictionary.rating from tweet_word LEFT OUTER JOIN dictionary ON(tweet_word.word =dictionary.word);



select * from word_join;


select id,AVG(rating) as rating from word_join GROUP BY word_join.id order by rating DESC


-- All the queries below store the data in the locations mentioned before the query. 
-- The syntax insert overwrite local directory 'directory path' performs this operation.

-- The below query gets the total number of tweets from the table tweet3 for that particular batch.
insert overwrite local directory '/home/hduser/eclipse/jee-mars/eclipse/Outputs/Tweet3/count' select count(id) AS Number_of_Tweets from tweet3;

--The below query fetches the number of favourites for that particular key word given in the Flume config file.
insert overwrite local directory '/home/hduser/eclipse/jee-mars/eclipse/Outputs/Tweet3/favourites' select count(favorited) AS Favorited_Tweet_Count from tweet3 GROUP BY favorited;

-- The following query fetches the sources from where tweets are posted.
insert overwrite local directory '/home/hduser/eclipse/jee-mars/eclipse/Outputs/Tweet3/source' select source, count(id) from tweet3 GROUP BY source;

-- The below query gives the user with maximum number of followers in the current batch of data.
insert overwrite local directory '/home/hduser/eclipse/jee-mars/eclipse/Outputs/Tweet3/MaxFollowedUser' select user.screen_name, user.followers_count from tweet3 where user.statuses_count > 50000 limit 1;

--The below query fetches the verified user who has more than 10 thousand followers.
insert overwrite local directory '/home/hduser/eclipse/jee-mars/eclipse/Outputs/Tweet3/Verified' select user.screen_name, user.followers_count from tweet3 where user.verified = true AND user.followers_count > 10000 limit 1;

--The below query gives the tweet with maximum number of retweets.
insert overwrite local directory '/home/hduser/eclipse/jee-mars/eclipse/Outputs/Tweet3/MaxRetweet' select user.screen_name, retweeted_status.text from tweet3 where user.followers_count > 50000 limit 1;

-- The below query fetches the location wise tweets from different time zones.
insert overwrite local directory '/home/hduser/eclipse/jee-mars/eclipse/Outputs/Tweet3/timezone' select user.time_zone, count(user.time_zone) from tweet3 where user.time_zone != ' ' GROUP BY user.time_zone;


