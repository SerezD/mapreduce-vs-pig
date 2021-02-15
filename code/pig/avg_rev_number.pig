-- Load review table and get desired columns and rows
review = LOAD 'project/inputs/review_small.json' USING JsonLoader('review_id:chararray, user_id:chararray, business_id:chararray, stars:int, useful:int, funny:int, cool:int, text:chararray, date:chararray');
review = FILTER review BY business_id == 'd4qwVw4PcN-_2mK2o1Ro1g';
users_rev = FOREACH review GENERATE user_id;
users_rev = DISTINCT users_rev;

-- Load user table
user = LOAD 'project/inputs/user_small.json' USING JsonLoader('user_id:chararray, name:chararray, review_count:int, yelping_since:chararray, useful:int, funny:int, cool:int, elite:chararray, friends:chararray, fans:int, average_stars:float, compliment_hot:int, compliment_more:int, compliment_profile:int, compliment_cute:int, compliment_list:int, compliment_note:int, compliment_plain:int, compliment_cool:int, compliment_funny:int, compliment_writer:int, compliment_photos:int');
user = FOREACH user GENERATE user_id, review_count;

-- Join on Users
join_users = JOIN users_rev BY user_id, user BY user_id;

-- get the review counts of the users
counts = FOREACH join_users GENERATE user::review_count;

-- compute average
grouped = GROUP counts ALL;
result = FOREACH grouped GENERATE AVG(counts.user::review_count);

STORE result INTO 'project/outputs/avg_rev_number_pig';


