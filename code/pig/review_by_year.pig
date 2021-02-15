review = LOAD 'project/inputs/review_small.json' USING JsonLoader('review_id:chararray, user_id:chararray, business_id:chararray, stars:int, useful:int, funny:int, cool:int, text:chararray, date:chararray');
business = LOAD 'project/inputs/business_small.json' USING JsonLoader('business_id:chararray, name:chararray, address:chararray, city:chararray, state:chararray, postal_code:chararray, latitude:float, longitude:float, stars:float, review_count:int, is_open:int, attributes_BusinessAcceptsCreditCards:chararray, attributes_BikeParking:chararray, attributes_GoodForKids:chararray, attributes_BusinessParking:chararray, attributes_ByAppointmentOnly:chararray, attributes_RestaurantsPriceRange2:chararray, categories:chararray, hours_Monday:chararray, hours_Tuesday:chararray, hours_Wednesday:chararray, hours_Thursday:chararray, hours_Friday:chararray, hours_Saturday:chararray, hours_Sunday:chararray');

--projections on desired columns
business = FILTER business BY city == 'Las Vegas';
business = FOREACH business GENERATE business_id;
review = FOREACH review GENERATE business_id, stars, SUBSTRING(date,0,4) AS year;

--join and project the two tables on business_id: "business_id, stars, year"
joined = JOIN review BY business_id, business BY business_id;
joined = FOREACH joined GENERATE business::business_id AS business_id, review::year AS year, review::stars AS stars;

-- group by year and business
grouped = GROUP joined BY (year,business_id);

-- compute average
result = FOREACH grouped GENERATE group, AVG(joined.stars);

STORE result INTO 'project/outputs/review_by_year_pig';


