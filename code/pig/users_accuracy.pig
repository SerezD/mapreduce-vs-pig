review = LOAD 'project/inputs/review_small.json' USING JsonLoader('review_id:chararray, user_id:chararray, business_id:chararray, stars:int, useful:int, funny:int, cool:int, text:chararray, date:chararray');
business = LOAD 'project/inputs/business_small.json' USING JsonLoader('business_id:chararray, name:chararray, address:chararray, city:chararray, state:chararray, postal_code:chararray, latitude:float, longitude:float, stars:float, review_count:int, is_open:int, attributes_BusinessAcceptsCreditCards:chararray, attributes_BikeParking:chararray, attributes_GoodForKids:chararray, attributes_BusinessParking:chararray, attributes_ByAppointmentOnly:chararray, attributes_RestaurantsPriceRange2:chararray, categories:chararray, hours_Monday:chararray, hours_Tuesday:chararray, hours_Wednesday:chararray, hours_Thursday:chararray, hours_Friday:chararray, hours_Saturday:chararray, hours_Sunday:chararray');

--projections on desired columns
review = FOREACH review GENERATE user_id, business_id, stars;
business = FOREACH business GENERATE business_id, stars;

--join the two tables on business_id: "user, user review for business, business, business average review"
rev_bus = JOIN review BY business_id, business BY business_id;

-- projection and difference between user review and average review
total_diff = FOREACH rev_bus GENERATE review::user_id AS user_id, (business::stars - review::stars) AS difference;

-- group by user and average of the differences for this user = "accuracy"
user_grouped = GROUP total_diff BY user_id;
result = FOREACH user_grouped GENERATE group AS user_id, AVG(total_diff.difference) AS average_difference;

STORE result INTO 'project/outputs/users_accuracy_pig';


