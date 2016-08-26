/********************************************************************************
BEGIN UTILITY FUNCTIONS
********************************************************************************/
-- A handy function...
CREATE OR REPLACE FUNCTION isnumeric(text) RETURNS BOOLEAN AS $$
DECLARE x NUMERIC;
BEGIN
    x = $1::NUMERIC;
    RETURN TRUE;
EXCEPTION WHEN others THEN
    RETURN FALSE;
END;
$$
STRICT
LANGUAGE plpgsql IMMUTABLE;

-- To play with that fun "data"
create extension hstore;

/*--------------------------------------------------------------------------------
END   UTILITY FUNCTIONS
--------------------------------------------------------------------------------*/
/********************************************************************************
BEGIN DATA IMPORT
********************************************************************************/
/*
How many rows?
>wc -l shard_clickstream_data_20160801.csv
2078894 shard_clickstream_data_20160801.csv
*/
-- DROP TABLE shard_clickstream_data_20160801;
CREATE TABLE shard_clickstream_data_20160801(
    id integer NOT NULL,
    hash_id bigint,
    segment character varying(255) COLLATE "default".pg_catalog,
    site_id character varying(255) COLLATE "default".pg_catalog,
    type_id integer,
    visitor_id character varying(255) COLLATE "default".pg_catalog,
    "timestamp" timestamp without time zone,
    data "public.hstore",
    CONSTRAINT shard_clickstream_data_20160801_pkey PRIMARY KEY (id),
    CONSTRAINT shard_clickstream_data_20160801_hash_id_segment_key UNIQUE (hash_id, segment)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

-- Bring it on in...
copy shard_clickstream_data_20160801 FROM 'C:\Temp\shard_clickstream_data_20160801.csv' WITH DELIMITER ',' QUOTE '"' ESCAPE '"' CSV HEADER;
-- Did we get em all?
select count(*) from shard_clickstream_data_20160801;
-- 2078893
select count(distinct id) FROM shard_clickstream_data_20160801;
-- 2078893
select top 10 * from shard_clickstream_data_20160801;

-- Get a sample to play with so I don't always have to type LIMIT x, yes, I know I"m lazy ;)
select *
into click_sample
from shard_clickstream_data_20160801
limit 1000;
select count(*) from click_sample;

/*--------------------------------------------------------------------------------
END   DATA IMPORT
--------------------------------------------------------------------------------*/

/********************************************************************************
BEGIN INVESTIGATION
I actually was not familiar with the "hstore" datatype in PostgreSQL, though I can definitely see how it would be useful! 
Holy cow, you can even put indexes on there...

Let's poke around a bit with the columns you said were important...

"http_x_forwarded_for"=>"72.192.3.141" -- ip address of the device that generated this event
"http_user_agent"=>"Mozilla/5.0 (Windows NT 6.0; rv:47.0) Gecko/20100101 Firefox/47.0" -- the user agent of the device that generate this event
"http_referer"=>"https://www.johnstonmurphy.com/melton-cap-toe/2388.html" -- url the user visited
"query_string" --This is a special key, it contains all the data that is captured and sent by the tracker
********************************************************************************/
-- Are you sneaking any keys by me?
select distinct k
into data_hstore_keys
from (
    select skeys(s.data) as k
    from shard_clickstream_data_20160801 s
) as dt;
/*
status
request_body
time_local
http_x_forwarded_for
remote_user
query_string
request_method
http_cookie
uri
remote_addr
http_referer
http_user_agent
*/

select
	'union select ''' || k || ''' , max(char_length(data->''' || k || ''')) from shard_clickstream_data_20160801'
    	as dynsql
from data_hstore_keys;

select 'status' , max(char_length(data->'status')) from shard_clickstream_data_20160801
union select 'request_body' , max(char_length(data->'request_body')) from shard_clickstream_data_20160801
union select 'time_local' , max(char_length(data->'time_local')) from shard_clickstream_data_20160801
union select 'http_x_forwarded_for' , max(char_length(data->'http_x_forwarded_for')) from shard_clickstream_data_20160801
union select 'remote_user' , max(char_length(data->'remote_user')) from shard_clickstream_data_20160801
union select 'query_string' , max(char_length(data->'query_string')) from shard_clickstream_data_20160801
union select 'request_method' , max(char_length(data->'request_method')) from shard_clickstream_data_20160801
union select 'http_cookie' , max(char_length(data->'http_cookie')) from shard_clickstream_data_20160801
union select 'uri' , max(char_length(data->'uri')) from shard_clickstream_data_20160801
union select 'remote_addr' , max(char_length(data->'remote_addr')) from shard_clickstream_data_20160801
union select 'http_referer' , max(char_length(data->'http_referer')) from shard_clickstream_data_20160801
union select 'http_user_agent' , max(char_length(data->'http_user_agent')) from shard_clickstream_data_20160801;
/*
?column?               max
time_local             26
status                 3
http_user_agent        584
http_cookie            1349
remote_addr            13
request_body           1
remote_user            1
query_string           6522
http_x_forwarded_for   71
http_referer           1497
uri                    6
request_method         4
*/
-- Why does the ip address field have values that long?
select id, data->'http_x_forwarded_for' FROM shard_clickstream_data_20160801 
where char_length(data->'http_x_forwarded_for')>16 limit 10;
select id, data->'http_x_forwarded_for', DATA 
from shard_clickstream_data_20160801 where id=1383528665;
-- Aha! Can be multiple ips... tricky ;)
/*--------------------------------------------------------------------------------
END   INVESTIGATION
--------------------------------------------------------------------------------*/

/********************************************************************************
BEGIN SPLIT HSTORE DATA

Oh fun, query_string is another nested data set :)
Let's break it into a temp table to make sure I don't miss something...
For instance, do all the query_strings have the same keys?

(After some various investigations...)
Not even close! 

It will probably make our lives easier to build a big 'ole denormalized table
that has all that "embedded" stuff extracted into simple fields (while ensuring 
that we maintain uniqueness on id). Let's first split the hstore stuff off into
columns, then we can worry about what to do with query_string.
********************************************************************************/

CREATE TABLE shard_clickstream_data_split (
	id INTEGER NOT NULL,
	hash_id BIGINT,
	segment VARCHAR(255),
	site_id VARCHAR(255),
	type_id INTEGER,
	visitor_id VARCHAR(255),
	"timestamp" TIMESTAMP WITHOUT TIME ZONE,
	
	uri VARCHAR(255),
	status VARCHAR(255),
	time_local VARCHAR(255),
	http_cookie VARCHAR(2048),
	remote_addr VARCHAR(255),
	remote_user VARCHAR(255),
	request_body VARCHAR(255),
	request_method VARCHAR(255),
	
	http_x_forwarded_for VARCHAR(255),
	http_user_agent VARCHAR(1023),
	http_referer VARCHAR(2047),
	query_string VARCHAR(8191),
	CONSTRAINT shard_clickstream_data_split_hash_id_segment_key UNIQUE(hash_id, segment),
	CONSTRAINT shard_clickstream_data_split_pkey PRIMARY KEY(id)
) 
;

INSERT INTO shard_clickstream_data_split (
  id,
  hash_id,
  segment,
  site_id,
  type_id,
  visitor_id,
  "timestamp",
  uri,
  status,
  time_local,
  http_cookie,
  remote_addr,
  remote_user,
  request_body,
  request_method,
  http_x_forwarded_for,
  http_user_agent,
  http_referer,
  query_string
)
SELECT id,
  hash_id,
  segment,
  site_id,
  type_id,
  visitor_id,
  "timestamp",
  data->'uri',
  data->'status',
  data->'time_local',
  data->'http_cookie',
  data->'remote_addr',
  data->'remote_user',
  data->'request_body',
  data->'request_method',
  data->'http_x_forwarded_for',
  data->'http_user_agent',
  data->'http_referer',
  data->'query_string'  
from shard_clickstream_data_20160801;


/*--------------------------------------------------------------------------------
END   SPLIT HSTORE DATA
--------------------------------------------------------------------------------*/
/********************************************************************************
BEGIN SPLIT QUERY_STRING

Split the query_string into key-value pairs on ampersand.
********************************************************************************/

drop table if exists click_query_string;
select id
	,split_part(nv, '=', 1) as name
    ,split_part(nv, '=', 2) as value
into click_query_string
from (
  select id
	,unnest(string_to_array(query_string, '&'))
  FROM shard_clickstream_data_split
 )as t(id, nv);
select count(*) from click_query_string;
-- 48698117
CREATE INDEX idx_click_query_string_id_name
ON click_query_string(id, name);
CREATE INDEX idx_click_query_string_name
ON click_query_string(name);
--CREATE INDEX idx_click_query_string_id
--ON click_query_string(id);

select count(*) from click_query_string;
-- 48698117

-- Let's take a look at the keys(name)
drop table if exists click_query_name_counts;
select name, count(*) as cnt
into click_query_name_counts
from click_query_string
group by name;

select * from click_query_name_counts order by cnt desc;

/*
It looks like there are several cases where idsite got a '%20' stuck in front
of it instead of an ampersand. We're doing a ton aggregates later on site_id, but
since we have this already as an explicit column, it's probably not worth a ton
of effort to fix. However, it would certainly be interesting to know what caused
the mangling AND if there are any cases where site_id != idsite...
*/
drop table if exists click_query_string_bad_idsite;
select s.id
	,s.data->'query_string' as query_string
    ,s.data
into click_query_string_bad_idsite
from shard_clickstream_data_20160801 s
join (
  select distinct id
  from click_query_string
  where name='%20idsite'
 )t
 on s.id=t.id;
select * from click_query_string_bad_idsite;

select query_string,count(*)
from click_query_string_bad_idsite
group by query_string;
/*
query_string              count
%20idsite={SITE_ID}       56
%20idsite=%7BSITE_ID%7D   234
%20idsite=1000035         292
*/

--Let's go ahead and fix it, it will be fun :)
update click_query_string
set name='idsite'
where name='%20idsite';

/*
Another example of potential corruption: this query_string appears to have 
embedded ampersands in the action_name value.

action_name=Guide%20Outdoors:%20Hunting,%20Outdoor%20&%20Fishing%20Tips,%20Articles%20&%20Gear%20Reviews%20%7C%20Sportsman's%20Guide&idsite=999974...
                                                     ^                                ^
If I had to guess, it looks like someone forgot to encode the ampersand as '%26'.
However, it's only affecting a few rows.
*/
select id, query_string
from shard_clickstream_data_split
where query_string like '%!%20&!%%' escape '!';

-- Any blanks?
select count(*) from click_query_string where coalesce(name,'')='';
select count(*) from click_query_string where coalesce(value,'')='';
-- Yup, 1651 blank values, what names do they correspond to?

select name,count(*)
from click_query_string
where coalesce(value,'')=''
group by name
order by count(*) desc;
/*
name                                                                    count
action_name                                                             1155
Shoes                                                                   347
Shoes:Streetwear,Jeans,Sneakers,Tees,Polos,Jackets,Boots,UrbanApparel   33
_viewts                                                                 31
idsite                                                                  27
Bags|Filson                                                             15
Manuals|PartsTown                                                       9
wd                                                                      8
*/

/*
Obviously there's still a ton of work we could do here sanitizing, but since the
fields you've specifically mentioned as important in query_string don't seem to
be completely jacked up, I think we need to leave this be for now and get on to
the aggregates.

Data is never completely clean... :(
*/

/*--------------------------------------------------------------------------------
END   SPLIT QUERY_STRING
--------------------------------------------------------------------------------*/
/********************************************************************************
BEGIN SITE_MAP

Now, we need to start moving toward our "MEGA-Denormalized table", and to do
that the main hurdle is pivoting those name-value pairs from click_query_string
back in line with our ids in shard_clickstream_data_split.

BUT... it would be nice if we could tie alot of what appear to be mangled 
site_ids to their actual values. Let's make a map to re-associate them with the 
proper site_id so that our attribution is cleaner.

I'm going to just guess at proper sanitation here, obviously in the real world
I would need confirmation that these assumptions are correct. But I basically just
pruned the %XX characters and the curly braces on {999934}. (Those are reasonable
assumptions, no?)
********************************************************************************/

--drop table if exists site_map;
SELECT distinct
  site_id as raw_site_id
  ,site_id
into site_map
from shard_clickstream_data_20160801
where site_id is not null;
ALTER TABLE site_map ALTER COLUMN site_id SET NOT NULL;
ALTER TABLE site_map
ADD CONSTRAINT pk_site_map PRIMARY KEY (raw_site_id);
CREATE INDEX ix_site_map
ON site_map(site_id);

--Manual editing magic... here's what I changed:
select *
from site_map
where raw_site_id!=site_id;
/*
raw_site_id                   site_id
%7B999934%7D                  999934
%20%20{999934}                999934
%20%20%20%20%20%20999900      999900
%20%20%20%20%20%20%20999900   999900
%0A%20%20%20%20%20%20999900   999900
{999934}                      999934
*/

/*--------------------------------------------------------------------------------
END   SITE_MAP
--------------------------------------------------------------------------------*/
/********************************************************************************
BEGIN click_query_pivot

Let's pivot those query_string columns back around for the fields we're interested in...
********************************************************************************/

drop table if exists click_query_pivot;
select c.id
	,c.site_id
	,cast(cqs_rev.value as varchar(255)) as revenue_raw
    ,case when isnumeric(cqs_rev.value) then 1 else 0 end as revenue_valid
    ,cqs_conv.value as idgoal_raw
	,case when cqs_conv.value is not null then 1 else 0 end as conversion_flag
    ,cqs_refdom.value as _ref_raw
	,cqs_ec_id.value as ec_id_raw
	-- Don't think I'll need these but why not?
	,cqs_action.value as action_name
	,cqs_url.value as url
	,cqs_urlref.value as urlref
    ,cqs_idsite.value as idsite_qs
into click_query_pivot
from shard_clickstream_data_split c
left join click_query_string cqs_rev
	on c.id=cqs_rev.id
  	and cqs_rev.name='revenue'
left join click_query_string cqs_conv
	on c.id=cqs_conv.id
  	and cqs_conv.name='idgoal'
left join click_query_string cqs_refdom
	on c.id=cqs_refdom.id
    and cqs_refdom.name='_ref'
left join click_query_string cqs_ec_id
	on c.id=cqs_ec_id.id
	and cqs_ec_id.name='ec_id'
left join click_query_string cqs_action
	on c.id=cqs_action.id
	and cqs_action.name='action_name'
left join click_query_string cqs_url
	on c.id=cqs_url.id
	and cqs_url.name='url'
left join click_query_string cqs_urlref
	on c.id=cqs_urlref.id
	and cqs_urlref.name='urlref'
left join click_query_string cqs_idsite
	on c.id=cqs_idsite.id
	and cqs_idsite.name='idsite'
where (
	cqs_rev.id is not null
    or cqs_conv.id is not null
    or cqs_refdom.id is not null
	or cqs_ec_id.id is not null
	or cqs_action.id is not null
	or cqs_url.id is not null
	or cqs_urlref.id is not null
    or cqs_idsite.id is not null
);
ALTER TABLE click_query_pivot
ADD CONSTRAINT pk_click_query_pivot PRIMARY KEY (id);

select count(*),count(distinct id) from click_query_pivot;
--1270569
/*--------------------------------------------------------------------------------
END   click_query_pivot
--------------------------------------------------------------------------------*/

/********************************************************************************
BEGIN refdomain_map
I do love extra credit, so let's parse those domains...

Hmm, it just occurred to me, you didn't specify how deeply you want to track the
domain heirarchy... Are you interested in each of these sepparately:
adclick.g.doubleclick.net
ad.doubleclick.net
adclick.g.doubleclick.net

Or should they all get attributed to doubleclick.net? Hmmm.... I can see both 
being very useful in different circumstances...

TODO: Replace other special characters! %25
********************************************************************************/


drop table if exists refdomain_map;
;with special_chars_filtered as (
  select distinct
	 _ref_raw
	-- Replace all those crazy %XX with colons and slashes :)
	,replace(
		replace(_ref_raw,'%2F','/')
		,'%3A',':') as sub_ref
  from click_query_pivot
where _ref_raw is not null
)
,domain_parsed as (
  select distinct _ref_raw
      --,sub_ref
      --Parse out the first thing that's NOT a forward slash after two of em (and a colon etc...)
      ,substring(sub_ref from '.*://([^/%]*)') as domain
  from special_chars_filtered
)
, lvl2 as (
  select _ref_raw
	,domain
    ,reverse(array_to_string(
    	(string_to_array(reverse(domain),'.'))[1:2]
        	,'.')) as lvl2_domain
  from domain_parsed
)
select
   _ref_raw
  ,"domain"
  --HACK: Only use lvl2_domain if:
  ,case when "domain" ~ '[[:alpha:]]' --It contains a letter
  	and lvl2_domain !~ '^com\..*' --Not one of those funky com.uk or co.tk or whaterver...
  	and lvl2_domain !~ '^co\..*'
    then lvl2_domain else "domain" end
     	as lvl2_domain
into refdomain_map
from lvl2
;
ALTER TABLE refdomain_map
ADD CONSTRAINT pk_refdomain_map PRIMARY KEY (_ref_raw);
select distinct lvl2_domain from refdomain_map order by lvl2_domain;

select * from refdomain_map limit 1000;
select count(*) from refdomain_map;
select * from refdomain_map where raw_ref like '%android%' limit 1000;

/*--------------------------------------------------------------------------------
END   refdomain_map
--------------------------------------------------------------------------------*/
/********************************************************************************
BEGIN shard_clickstream_data_denorm

Now we can finally build our "MEGA-Denormalized" table!
(Shhh... don't tell Ronald Fagin...)
********************************************************************************/

--drop table if exists shard_clickstream_data_denorm;
select
	s.id,
    s.hash_id,
    s.segment,
    sm.raw_site_id,
    sm.site_id,
    s.type_id,
    s.visitor_id,
    s."timestamp",
    s.uri,
    s.status,
    s.time_local,
	s.http_cookie,
  	s.remote_addr,
  	s.remote_user,
  	s.request_body,
  	s.request_method,
  	s.http_x_forwarded_for,
  	s.http_user_agent,
  	s.http_referer
	--,s.query_string
  	,c.revenue_raw
	,c.revenue_valid
	,c.idgoal_raw
	,c.conversion_flag
	,c._ref_raw
	,c.ec_id_raw
    ,c.action_name
    ,c.url
    ,c.urlref
	,c.idsite_qs
	
	,dm.domain
	,dm.lvl2_domain
into shard_clickstream_data_denorm
from shard_clickstream_data_split s
left join click_query_pivot c
	on s.id=c.id
left join site_map sm
	on s.site_id=sm.raw_site_id
left join refdomain_map dm
	on c._ref_raw=dm._ref_raw;
ALTER TABLE shard_clickstream_data_denorm
ADD CONSTRAINT pk_shard_clickstream_data_denorm PRIMARY KEY (id);
/*
TODO: Some of these indices are unnecessary, and we could maybe
use some others on site_id, timestamp and site_id, visitor_id, timestamp
for the first/last and average time queries below?
*/
CREATE INDEX ix_shard_clickstream_data_denorm_site_id
ON shard_clickstream_data_denorm(site_id);
CREATE INDEX ix_shard_clickstream_data_denorm_timestamp
ON shard_clickstream_data_denorm("timestamp");
CREATE INDEX ix_shard_clickstream_data_denorm_visitor_id
ON shard_clickstream_data_denorm(visitor_id);
CREATE INDEX ix_shard_clickstream_data_denorm_ec_id_raw
ON shard_clickstream_data_denorm(ec_id_raw);
CREATE INDEX ix_shard_clickstream_data_denorm_site_id_visitor_id
ON shard_clickstream_data_denorm(site_id,visitor_id);


select count(*),count(distinct id) from shard_clickstream_data_denorm;
--2078893
select * from shard_clickstream_data_denorm limit 1000;

--are there any ec_id without conversions? no.
select *
from shard_clickstream_data_denorm
where conversion_flag=0
	and ec_id_raw is not null
 limit 1;

select count(*), count(distinct ec_id_raw)
from shard_clickstream_data_denorm
where ec_id_raw is not null;


-- Is there an identical ec_id_raw that has multiple revenues?
-- I don't think there are any concerning dupes...
;with ec_id_inv as (
  select
      ec_id_raw
      ,visitor_id
      ,revenue_raw
      ,id
      --*
  from shard_clickstream_data_denorm
  where ec_id_raw in (
    select ec_id_raw--,count(*)
    from shard_clickstream_data_denorm
    where ec_id_raw is not null
    group by ec_id_raw
    having count(*)>1
  )
  order by ec_id_raw
)
,dupe_ec_id as (
  select ec_id_raw, count(distinct revenue_raw)
  from ec_id_inv
  group by ec_id_raw
  having count(distinct revenue_raw)>1
  )
 select d.ec_id_raw, d.revenue_raw, d.id
 from dupe_ec_id e
join shard_clickstream_data_denorm d
on e.ec_id_raw=d.ec_id_raw
order by d.ec_id_raw, d.revenue_raw;

-- TODO: I hope it's correct to assume that ANY non-NULL ec_id value 
-- indicates a conversion, and not ec_id=1...
select idgoal_raw,count(*)
from shard_clickstream_data_denorm
group by idgoal_raw;
/*
"idgoal_raw" "count"
"0"      "8071"
"1"      "52"
       "2070770"
*/

-- To fix lvl2_domain when refdomain_map get's updated...
-- THIS is why we normalize our data ;)
select s._ref_raw, s.lvl2_domain, r.lvl2_domain
from shard_clickstream_data_denorm s
join refdomain_map r
	on s._ref_raw=r._ref_raw
where s.lvl2_domain!=r.lvl2_domain;

update shard_clickstream_data_denorm as s
set lvl2_domain=r.lvl2_domain
	,"domain"=r."domain"
from refdomain_map as r
where s._ref_raw=r._ref_raw
	and (
		s.lvl2_domain!=r.lvl2_domain
		or s."domain"!=r."domain"
	)
;

/*--------------------------------------------------------------------------------
END   
--------------------------------------------------------------------------------*/

/********************************************************************************
BEGIN ec_conversion

Build a list of unique "conversion" events associated with revenue.

I may be way off the mark here, but my assumption is that each ec_id represents
a distinct conversion associated with revenue. This is based on a comment you
made during Ex 2:
	"total conversions (distinct ec_id)"
and also that fact that ec_ids always seem to be associated with the same revenue
value/visitor_id/site_id etc... This is what I was trying to investigate with the
ec_id_inv CTE above.

Therefore, this would be the classic "make sure you don't count revenue twice"
scenario, so it would would be nice to have a table of each conversion event
we DO want to count.
********************************************************************************/
drop table if exists ec_conversion;
select distinct
      ec_id_raw
      ,visitor_id
      ,revenue_raw
	  ,domain
	  ,lvl2_domain
      ,case when isnumeric(revenue_raw) then 1
      	else 0 end as revenue_valid
      ,case when isnumeric(revenue_raw) then
      	cast(revenue_raw as decimal)
        else 0.0 end
        	as revenue
      ,site_id
into ec_conversion
from shard_clickstream_data_denorm
where ec_id_raw is not null;
ALTER TABLE ec_conversion ADD CONSTRAINT pk_ec_conversion
PRIMARY KEY (ec_id_raw);
CREATE INDEX ix_ec_conversion
ON ec_conversion(site_id);



select count(*),count(distinct ec_id_raw) from ec_conversion;
--7961 7961
select * from ec_conversion;

-- Any null/blank values?
select *
from ec_conversion e
where coalesce(e.visitor_id,'')=''
	or coalesce(e.revenue_raw,'')=''
    or coalesce(e.site_id,'')=''
    or coalesce(e.domain,'')='';
/*
There are >3k NULL domains, but that's fine, because as far as I can tell,
the _ref_raw was legitimately blank as well.
*/
select id,domain,_ref_raw
from shard_clickstream_data_denorm
where coalesce(domain,'')=''
	and domain!=_ref_raw;

/*--------------------------------------------------------------------------------
END   ec_conversion
--------------------------------------------------------------------------------*/

/********************************************************************************
BEGIN site_revenue_report

Ex 1.A and Ex 1.C
Show revenue sums by site, even for ones that didn't have entries.
Some people would have wrapped coalesce(revenue_sum, 0) etc... around the
final select columns, which is obviously fine. I personally like to see the NULLs
because it presents extra data. 
********************************************************************************/

drop table if exists site_revenue_report;
;with sites_with_data as (
  select site_id
      ,sum(revenue) as revenue_sum
      ,count(distinct ec_id_raw) as total_conversions
      ,sum(revenue_valid) as valid_revenue_conversions
  from ec_conversion
  group by site_id
)
select coalesce(s.site_id, swd.site_id) as site_id
	,swd.revenue_sum
    ,swd.total_conversions
    ,swd.valid_revenue_conversions
into site_revenue_report
from (select distinct site_id from site_map) s
full join sites_with_data swd
	on s.site_id=swd.site_id
;
ALTER TABLE site_revenue_report
ADD CONSTRAINT pk_site_revenue_report PRIMARY KEY (site_id);

select *
from site_revenue_report
order by total_conversions;

/*
site_id         revenue_sum   total_conversions   valid_revenue_conversions
%7BSITE_ID%7D   2             1                   1
1000030         12829.35      30                  29
1000035         84356.43      357                 357
1000024         72291.36      393                 393
999931          59234.0958    629                 629
1000027         304394.03     1102                1102
999900          125705.35     1166                1166
999974          654797.17     4283                4283
999934                                                
999929                                                
99996                                                 
999944                                                
99954                                                 
999936                                                
99955                                                 
999903                                                
999933                                                
99997                                                 
undefined                                             
1000034                                               
999937                                                
999914                                                
999932                                                
999943                                                
999997                                                

*/


/*--------------------------------------------------------------------------------
END   site_revenue_report
--------------------------------------------------------------------------------*/
/********************************************************************************
BEGIN domain_revenue_report

Ex 1.B
Show revenue by the first referral domain, then again using only the first two 
sub-domains of the referral domain (clear as mud?).
Ex:
google.com 		--> 	google.com
www.google.com 	--> 	google.com
********************************************************************************/

drop table if exists domain_revenue_report;
;with domains_with_data as (
  select domain
      ,sum(revenue) as revenue_sum
      ,count(distinct ec_id_raw) as total_conversions
      ,sum(revenue_valid) as valid_revenue_conversions
  from ec_conversion
  group by domain
)
select coalesce(d.domain, dwd.domain,'UNKNOWN') as domain
	,dwd.revenue_sum
    ,dwd.total_conversions
    ,dwd.valid_revenue_conversions
into domain_revenue_report
from (select distinct domain from refdomain_map) d
full join domains_with_data dwd
	on d.domain=dwd.domain
;
ALTER TABLE domain_revenue_report
ADD CONSTRAINT pk_domain_revenue_report PRIMARY KEY (domain);

select *
from domain_revenue_report
where revenue_sum is not null
order by revenue_sum desc;

-- Rinse and repeat for lvl2_domain...
drop table if exists lvl2_domain_revenue_report;
;with domains_with_data as (
  select lvl2_domain
      ,sum(revenue) as revenue_sum
      ,count(distinct ec_id_raw) as total_conversions
      ,sum(revenue_valid) as valid_revenue_conversions
  from ec_conversion
  group by lvl2_domain
)
select coalesce(d.lvl2_domain, dwd.lvl2_domain,'UNKNOWN') as lvl2_domain
	,dwd.revenue_sum
    ,dwd.total_conversions
    ,dwd.valid_revenue_conversions
into lvl2_domain_revenue_report
from (select distinct lvl2_domain from refdomain_map) d
full join domains_with_data dwd
	on d.lvl2_domain=dwd.lvl2_domain
;
ALTER TABLE lvl2_domain_revenue_report
ADD CONSTRAINT pk_lvl2_domain_revenue_report PRIMARY KEY (lvl2_domain);

select *
from lvl2_domain_revenue_report
where revenue_sum is not null
order by revenue_sum desc;


/*
Unfortunately it looks like alot of this money is going unattributed due to the
'_ref' key being completely missing from the query string :( Several of them DO
have other keys like urlref, but then that's not what was asked for... though it
wouldn't be difficult to run urlref through similar queries.
*/
-- Idiot check, did we miss any ref (or similar) values that went to the 'UNKNOWN' domain?
select *
-- from shard_clickstream_data_20160801
--from clicks_sanitized_query
from click_query_string
where id in (
  select distinct c.id
  from click_query_pivot c
  left join refdomain_map r
      on r.id=c.id
	where r.id is null
)
	and name like '%ref%'
;

/*--------------------------------------------------------------------------------
END   domain_revenue_report
--------------------------------------------------------------------------------*/
/********************************************************************************
BEGIN FIRST/LAST VISITOR INFO

Get first/last visitor by site and calculate the timespan
********************************************************************************/

drop table if exists site_visitor_timespan;
;with site_first_visitor as (
  select distinct on (site_id)
      site_id
      ,visitor_id as first_visitor
      ,"timestamp" as min_ts
  from shard_clickstream_data_denorm
  where site_id is not null
	and visitor_id is not null
  order by site_id, "timestamp" asc
)
, site_last_visitor as (
  select distinct on (site_id)
      site_id
      ,visitor_id as last_visitor
      ,"timestamp" as max_ts
  from shard_clickstream_data_denorm
  where site_id is not null
	and visitor_id is not null
  order by site_id, "timestamp" desc
)
, span as (
  select f.site_id
	--Let's get a fairly accurate span of hours between the first and last event
	,(DATE_PART('day', max_ts - min_ts)*(24*60*60))
      +(DATE_PART('hour', max_ts - min_ts)*(60*60))
      +(DATE_PART('minute', max_ts - min_ts)*(60))
      +(DATE_PART('second', max_ts - min_ts)*(1)) as seconds_span
	,f.first_visitor
	,f.min_ts
	,l.last_visitor
	,l.max_ts
  from site_first_visitor f
  join site_last_visitor l
      on f.site_id=l.site_id
)
select *
	,seconds_span / (60*60.0) as hours_span
into site_visitor_timespan
from span
;

select * from site_visitor_timespan;

/*
site_id         seconds_span   first_visitor      min_ts                 last_visitor       max_ts
%7BSITE_ID%7D   91             afc14a47612942f6   8/1/2016 5:45:22 AM    afc14a47612942f6   8/1/2016 5:46:53 AM
1000024         86398          c5ace240e45784a5   8/1/2016 12:00:01 AM   3b3902dab145dcf7   8/1/2016 11:59:59 PM
1000027         86394          6dc53424f29469c4   8/1/2016 12:00:05 AM   cd6676e10c81866a   8/1/2016 11:59:59 PM
1000030         86384          079cdb117f496572   8/1/2016 12:00:04 AM   52bdd33e79782b8c   8/1/2016 11:59:48 PM
1000034         20947          c77687ad4d8fa177   8/1/2016 6:08:31 PM    c8aa0d1132f9d6dd   8/1/2016 11:57:38 PM
1000035         86399          421958e4900bcb8f   8/1/2016               8808228a174d0eee   8/1/2016 11:59:59 PM
99954           86017          abaaf13f6e7cb8e8   8/1/2016 12:04:24 AM   e92aa5547bf24180   8/1/2016 11:58:01 PM
99955           80763          c840312871c902bf   8/1/2016 1:20:11 AM    c840312871c902bf   8/1/2016 11:46:14 PM
999900          86399          4e123a71340a1136   8/1/2016               b0e9f05313e8b782   8/1/2016 11:59:59 PM
999903          85649          07bea77bdf6c2e43   8/1/2016 12:04:05 AM   4f14bc4829484fc0   8/1/2016 11:51:34 PM
999914          50920          861822cb501dd49d   8/1/2016 5:49:24 AM    c300ca4bdee4739e   8/1/2016 7:58:04 PM
999929          27449          ce1b6425bf388904   8/1/2016 3:08:12 AM    b66a7bcf39218c60   8/1/2016 10:45:41 AM
999931          86328          29b05c10f930f781   8/1/2016 12:00:59 AM   fc878781dba8afc4   8/1/2016 11:59:47 PM
999932          86258          9f343a8e8934adc0   8/1/2016 12:01:03 AM   255dc4a01e09d500   8/1/2016 11:58:41 PM
999933          86387          c1c709b37e9db327   8/1/2016 12:00:10 AM   dc57feae73a7f492   8/1/2016 11:59:57 PM
999934          86191          7c3e4062a1e4d7e7   8/1/2016 12:00:12 AM   6bcbed23af67dbe0   8/1/2016 11:56:43 PM
999936          0              32f8d42fee54f088   8/1/2016 11:22:00 AM   32f8d42fee54f088   8/1/2016 11:22:00 AM
999937          86395          2a6557070f7f1254   8/1/2016 12:00:03 AM   da4406c73c71282b   8/1/2016 11:59:58 PM
999943          86330          dafff352ddde97f9   8/1/2016 12:00:10 AM   83eb42266ae51e37   8/1/2016 11:59:00 PM
999944          0              d4c98305d78a775c   8/1/2016 1:24:45 PM    d4c98305d78a775c   8/1/2016 1:24:45 PM
99996           86370          b15aef1d5b114c28   8/1/2016 12:00:19 AM   a6dee82c7dc331c8   8/1/2016 11:59:49 PM
99997           86378          31596ea5acddbd5e   8/1/2016 12:00:12 AM   0a2cac6d27c12a50   8/1/2016 11:59:50 PM
999974          86399          e44b12852c497ab7   8/1/2016               b1aaf33b6a1a030a   8/1/2016 11:59:59 PM
999997          86203          587c938fe3cf47dc   8/1/2016 12:00:28 AM   890c45d43c5cf9da   8/1/2016 11:57:11 PM
undefined       80763          c840312871c902bf   8/1/2016 1:20:11 AM    c840312871c902bf   8/1/2016 11:46:14 PM
*/

/*--------------------------------------------------------------------------------
END   FIRST/LAST VISITOR INFO
--------------------------------------------------------------------------------*/
/********************************************************************************
BEGIN FIRST/LAST CONVERSION VISITOR INFO

Similar to above, but only get info where conversions occurred.
TODO: Is idgoal_raw necessary, or does ec_id_raw alone indicate a conversion?
********************************************************************************/
drop table if exists site_converted_visitor;
;with site_first_visitor as (
  select distinct on (site_id)
      site_id
      ,visitor_id as first_visitor
      ,"timestamp" as min_ts
  from shard_clickstream_data_denorm
  where site_id is not null
	and visitor_id is not null
    and ec_id_raw is not null
  	and idgoal_raw is not null --TODO: Redundant?
  order by site_id, "timestamp" asc
)
, site_last_visitor as (
  select distinct on (site_id)
      site_id
      ,visitor_id as last_visitor
      ,"timestamp" as max_ts
  from shard_clickstream_data_denorm
  where site_id is not null
	and visitor_id is not null
    and ec_id_raw is not null
  	and idgoal_raw is not null --TODO: Redundant?
  order by site_id, "timestamp" desc
)
select f.site_id
	,f.first_visitor
    ,l.last_visitor
	--TODO: Not needed for report, nice for QC...
	,(DATE_PART('day', max_ts - min_ts)*(24*60*60))
      +(DATE_PART('hour', max_ts - min_ts)*(60*60))
      +(DATE_PART('minute', max_ts - min_ts)*(60))
      +(DATE_PART('second', max_ts - min_ts)*(1)) as seconds_span
    ,f.min_ts
    ,l.max_ts
into site_converted_visitor
from site_first_visitor f
join site_last_visitor l
	on f.site_id=l.site_id
;
select * from site_converted_visitor;

/*--------------------------------------------------------------------------------
END   FIRST/LAST CONVERSION VISITOR INFO
--------------------------------------------------------------------------------*/
/********************************************************************************
BEGIN SITE AGGREGATES
********************************************************************************/

-- Get some basic counts by site_id...
drop table if exists site_visitors;
select
	 COALESCE(site_id, 'UNKNOWN') as site_id
    ,count(*) as raw_count
    ,count(distinct visitor_id) as distinct_visitor_id
	,sum(case when visitor_id is not null then 1 else 0 end) as total_views_by_non_null_visitor_ids
into site_visitors
from shard_clickstream_data_denorm
group by COALESCE(site_id, 'UNKNOWN');
ALTER TABLE site_visitors
ADD CONSTRAINT pk_site_visitors PRIMARY KEY (site_id);
select count(*) from site_visitors limit 1000;

-- What's up with the rows with no site_id?
select * from site_visitors where site_id='UNKNOWN' limit 1000;
-- Ah, no visitor_id either :)


/*
Calculate visitors/hour
If we haven't actually sampled longer than 1 hour, it seems unfair to
extrapolate on the fractional span?

Since timestamp for this dataset appears to be over the course of 1 day,
2 people coming over the course of 10 minutes does not seem to imply 12 
visitors/hour (at least to me).
*/
drop table if exists site_visitors_per_hour;
SELECT sv.site_id
	,distinct_visitor_id / CASE
		WHEN svt.hours_span > 1
			THEN svt.hours_span
		ELSE 1
		END AS avg_visitors_per_hour
INTO site_visitors_per_hour
FROM site_visitors sv
JOIN site_visitor_timespan svt
ON sv.site_id = svt.site_id;
select * from site_visitors_per_hour;

/*
Calculate average page views per visitor
*/
drop table if exists site_repeat_visits;
select site_id
	,avg(num_visits_to_site) as avg_page_views_per_visitor
    --,sum(num_visits_to_site) as total_visits --Handled elsewhere, doesn't count null visitor_id...
into site_repeat_visits
from (
  select site_id
	  ,visitor_id
      ,count(*) as num_visits_to_site
  from shard_clickstream_data_denorm
  where site_id is not null
	and visitor_id is not null
  group by visitor_id, site_id
)s
group by site_id;
select count(*) from site_repeat_visits;
select * from site_repeat_visits;

/*
To calculate average time on site, first we need to find the time on
site for each visitor.

TODO: This assumes that they are continuously on the site between
first/last event :( Perhaps I need to look at ip address/user agent or
other cookie info for more accurate results?
*/
drop table if exists site_visits_timespan;
;with site_visits_min as (
  select distinct on (site_id, visitor_id)
  	site_id
    ,visitor_id
    ,"timestamp" as min_ts
 from shard_clickstream_data_denorm s
 where site_id is not null
 	and visitor_id is not null
 order by site_id, visitor_id, "timestamp" asc
)
, site_visits_max as (
  select distinct on (site_id, visitor_id)
  	site_id
    ,visitor_id
    ,"timestamp" as max_ts
 from shard_clickstream_data_denorm s
 where site_id is not null
 	and visitor_id is not null
 order by site_id, visitor_id, "timestamp" desc
)
, span as (
  select f.site_id
	,f.visitor_id
	--Let's get a fairly accurate span of hours between the first and last event
	,(DATE_PART('day', max_ts - min_ts)*(24*60*60))
      +(DATE_PART('hour', max_ts - min_ts)*(60*60))
      +(DATE_PART('minute', max_ts - min_ts)*(60))
      +(DATE_PART('second', max_ts - min_ts)*(1)) as seconds_span
	,f.min_ts
	,l.max_ts
  from site_visits_min f
  join site_visits_max l
      on f.site_id=l.site_id
      and f.visitor_id=l.visitor_id
)
select *
	,seconds_span / (60*60.0) as hours_span
into site_visits_timespan
from span
;
select count(*) from site_visits_timespan;
--321881
select * from site_visits_timespan limit 1000;

drop table if exists avg_time_on_site;
;with agg as (
  select site_id
    ,avg(seconds_span) as avg_seconds_on_site
    ,sum(seconds_span) as total_seconds_on_site
  from site_visits_timespan
  group by site_id
)
select *
  ,avg_seconds_on_site / (60*60.0) as avg_hours_on_site
  ,total_seconds_on_site / (60*60.0) as total_hours_on_site
into avg_time_on_site
from agg
;
select * from avg_time_on_site;

select a.site_id
	,a.avg_hours_on_site
    ,r.avg_hours_on_site
    ,a.total_hours_on_site
    ,r.total_time_on_site
from avg_time_on_site a
join ex_2_site_report r
on a.site_id=r.site_id;
 
  
/*--------------------------------------------------------------------------------
END   SITE AGGREGATES
--------------------------------------------------------------------------------*/
/********************************************************************************
BEGIN Ex 2: SITE AGGREGATES REPORT


 A. Columns
    - site_id
    - average visitors per hour !(timestamp)
    - total visitor_ids	!(visitor_id)
    - first converted visitor id
    - last converted visitor id
    - total page views
    - avg page view per visitor id
    - total revenue
    - total conversions (distinct ec_id)
    - average time on site
    - total time on site
********************************************************************************/

drop table if exists ex_2_site_report_v2; 
select sv.site_id
	,svph.avg_visitors_per_hour
    ,sv.distinct_visitor_id as total_visitor_ids
    ,scv.first_visitor as first_converted_visitor_id
    ,scv.last_visitor as last_converted_visitor_id
    ,sv.raw_count as total_page_views
    ,srv.avg_page_views_per_visitor
    ,srr.revenue_sum
    ,srr.total_conversions
    ,srr.valid_revenue_conversions --NOTE: Note in requirements!
    ,atos.avg_hours_on_site
	,atos.total_hours_on_site
into ex_2_site_report_v2
from site_visitors sv
left join site_visitors_per_hour svph
	on sv.site_id=svph.site_id
left join site_converted_visitor scv
	on sv.site_id=scv.site_id
left join site_repeat_visits srv --TODO: Rename?
	on sv.site_id=srv.site_id
left join site_revenue_report srr
	on sv.site_id=srr.site_id
left join avg_time_on_site atos
	on sv.site_id=atos.site_id
;

select *
from ex_2_site_report_v2
order by site_id;


/*--------------------------------------------------------------------------------
END   Ex 2: SITE AGGREGATES REPORT
--------------------------------------------------------------------------------*/
/********************************************************************************
BEGIN Ex 3: FIND INTERESTING SH!*

"
	Tell me something cool about the data!
	This is completely subjective, feel free to show off your skills and ideas.
"
Parsing the _ref domain two levels deep was me attempting to show off my skills
with regular expressions and trying to come up with ways to make reporting more
clear (and therefore more useful). Now, I realize that I only took the report
from 2.3k rows to 2k, which is still WAY more than anyone is actually going to 
read (let's be honest). However, if we started filtering out things like localhost,
or further aggregating (Ex. put all Google domains under one label), I think we
could pretty easily get to the point where you could distill alot of info from
a single glance. (Which is, IMO, the Holy Grail of reporting.)

Plus, I just like playing with Regex ;)

Here's some more random stuff...
********************************************************************************/
select *
from shard_clickstream_data_denorm s
where s.raw_site_id!=s.idsite_qs;
--Nope! (That's a relief)

--Let's check out the rows where I "sanitized" site_id...
select site_id, idsite_qs,raw_site_id
from shard_clickstream_data_denorm s
where s.site_id!=s.idsite_qs;

--What is the deal with those (still) corrupted site_ids?
select *
from shard_clickstream_data_denorm s
where s.site_id='%7BSITE_ID%7D';
--No idea ;)

-- Who's droppin' that mad cash?
select *
from ec_conversion e
order by revenue desc
limit 1;
/*
"ec_id_raw"   "visitor_id"         "revenue_raw"   "domain"   "lvl2_domain"   "revenue_valid"   "revenue"    "site_id"
"00989294"    "5763ea94c4748c3e"   "11227.34"                                 "1"               "11227.34"   "1000027"

Not bad Mr. (or Mrs.) 00989294, 11 Grand...
However, we don't know the referring domain :(
*/


select *
from ec_conversion e
where revenue!=0
order by revenue asc
limit 2;
/*
"ec_id_raw"    "visitor_id"         "revenue_raw"   "domain"   "lvl2_domain"   "revenue_valid"   "revenue"   "site_id"
"1200520931"   "f54f7fab081c5762"   "-5.1316"                                  "1"               "-5.1316"   "999931"
Looks like f54f7fab081c5762 got a refund? Or should I have checked for negative revenue and filtered? :(

"1200520581"   "df552f453239f3fe"   "0.9894"                                   "1"               "0.9894"    "999931"
Congratulations df552f453239f3fe, you spent less than a dollar!
*/

-- "Session grouping" investigation
select 
	s."timestamp"
    ,s.http_user_agent
    ,s.http_x_forwarded_for
    ,s.http_cookie
from shard_clickstream_data_denorm s
where site_id='999974'
and visitor_id='e23d82a7173317ed'
order by "timestamp";
/*
timestamp              http_user_agent                                                                                                  http_x_forwarded_for   http_cookie
8/1/2016               Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36   74.171.62.15           -
8/1/2016 12:00:08 AM   Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36   74.171.62.15           _acx_data=empty
8/1/2016 12:00:26 AM   Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36   74.171.62.15           _acx_data=empty
8/1/2016 12:00:29 AM   Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36   74.171.62.15           _acx_data=empty
8/1/2016 12:02:04 AM   Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36   74.171.62.15           _acx_data=empty
8/1/2016 11:56:47 PM   Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36   74.171.62.15           _acx_data=empty
8/1/2016 11:58:49 PM   Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36   74.171.62.15           _acx_data=empty
8/1/2016 11:59:02 PM   Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36   74.171.62.15           _acx_data=empty
8/1/2016 11:59:14 PM   Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36   74.171.62.15           _acx_data=empty
8/1/2016 11:59:39 PM   Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36   74.171.62.15           _acx_data=empty
8/1/2016 11:59:50 PM   Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36   74.171.62.15           _acx_data=empty

This was very likely 2 discreete sessions, not one contiguous :(

The question is, how do we define session boundaries? We could define some 
"idle time" such that if there is no click for that span then we split up the
session... But that's sloppy and arbitrary, is there another field that could
indicate it?

Hmmmm, perhaps segment! You DID say to ignore it though...
*/
select 
	s.visitor_id
	,s."timestamp"
    ,s.segment
    ,dense_rank() over (partition by visitor_id order by s.segment asc)
    ,s.http_x_forwarded_for
    --,s.http_cookie
    --,s.http_user_agent
    --,*
from shard_clickstream_data_denorm s
where site_id='999974'
and visitor_id in ('e23d82a7173317ed','c0b9e739f0d3ccf1', 'e001fc4578e1af85')
order by visitor_id, "timestamp";
/*
visitor_id         timestamp              segment            dense_rank   http_x_forwarded_for
c0b9e739f0d3ccf1   8/1/2016 12:00:02 AM   1470009661-10961   1            108.238.185.76
c0b9e739f0d3ccf1   8/1/2016 12:00:40 AM   1470009661-10961   1            108.238.185.76
c0b9e739f0d3ccf1   8/1/2016 12:00:52 AM   1470009661-10961   1            108.238.185.76
c0b9e739f0d3ccf1   8/1/2016 12:00:55 AM   1470009661-18647   2            108.238.185.76
c0b9e739f0d3ccf1   8/1/2016 12:01:00 AM   1470009661-18647   2            108.238.185.76
c0b9e739f0d3ccf1   8/1/2016 12:01:40 AM   1470009721-14664   3            108.238.185.76
c0b9e739f0d3ccf1   8/1/2016 12:02:02 AM   1470009781-12114   4            108.238.185.76
c0b9e739f0d3ccf1   8/1/2016 11:53:35 PM   1470095641-24932   5            108.238.185.76
c0b9e739f0d3ccf1   8/1/2016 11:56:58 PM   1470095821-2279    6            108.238.185.76
c0b9e739f0d3ccf1   8/1/2016 11:57:28 PM   1470095881-28246   7            108.238.185.76
c0b9e739f0d3ccf1   8/1/2016 11:57:54 PM   1470095881-28246   7            108.238.185.76
c0b9e739f0d3ccf1   8/1/2016 11:58:15 PM   1470095941-10331   8            108.238.185.76
c0b9e739f0d3ccf1   8/1/2016 11:58:38 PM   1470095941-10331   8            108.238.185.76
c0b9e739f0d3ccf1   8/1/2016 11:59:36 PM   1470096001-4375    9            108.238.185.76
c0b9e739f0d3ccf1   8/1/2016 11:59:45 PM   1470096001-4375    9            108.238.185.76
e001fc4578e1af85   8/1/2016               1470009601-21267   1            68.169.155.25
e001fc4578e1af85   8/1/2016 12:00:47 AM   1470009661-10961   2            68.169.155.25
e001fc4578e1af85   8/1/2016 12:02:12 AM   1470009781-12114   3            68.169.155.25
e001fc4578e1af85   8/1/2016 12:09:37 AM   1470010201-29558   4            68.169.155.25
e001fc4578e1af85   8/1/2016 12:10:28 AM   1470010261-32600   6            68.169.155.25
e001fc4578e1af85   8/1/2016 12:10:37 AM   1470010261-32600   6            68.169.155.25
e001fc4578e1af85   8/1/2016 12:10:54 AM   1470010261-10371   5            68.169.155.25
e001fc4578e1af85   8/1/2016 12:10:59 AM   1470010261-32600   6            68.169.155.25
e001fc4578e1af85   8/1/2016 12:11:56 AM   1470010321-10026   7            68.169.155.25
e001fc4578e1af85   8/1/2016 12:12:38 AM   1470010381-29325   8            68.169.155.25
e001fc4578e1af85   8/1/2016 12:13:28 AM   1470010441-8442    9            68.169.155.25
e001fc4578e1af85   8/1/2016 12:14:53 AM   1470010501-25384   10           68.169.155.25
e001fc4578e1af85   8/1/2016 12:17:29 AM   1470010681-16890   11           68.169.155.25
e001fc4578e1af85   8/1/2016 12:20:24 AM   1470010861-32079   12           68.169.155.25
e001fc4578e1af85   8/1/2016 12:22:44 AM   1470010981-22750   13           68.169.155.25
e001fc4578e1af85   8/1/2016 12:23:18 AM   1470011041-18496   14           68.169.155.25
e001fc4578e1af85   8/1/2016 12:25:22 AM   1470011161-2400    15           68.169.155.25
e001fc4578e1af85   8/1/2016 12:25:35 AM   1470011161-2400    15           68.169.155.25
e001fc4578e1af85   8/1/2016 12:36:16 AM   1470011821-30882   16           68.169.155.25
e001fc4578e1af85   8/1/2016 12:37:54 AM   1470011881-20668   17           68.169.155.25
e001fc4578e1af85   8/1/2016 12:38:00 AM   1470011881-23793   18           68.169.155.25
e001fc4578e1af85   8/1/2016 12:38:28 AM   1470011941-20604   19           68.169.155.25
e001fc4578e1af85   8/1/2016 9:19:22 AM    1470043201-16136   20           68.169.155.25
e001fc4578e1af85   8/1/2016 9:20:17 AM    1470043261-6542    21           68.169.155.25
e001fc4578e1af85   8/1/2016 9:21:01 AM    1470043321-13099   22           68.169.155.25
e001fc4578e1af85   8/1/2016 9:21:02 AM    1470043321-29400   23           68.169.155.25
e001fc4578e1af85   8/1/2016 9:21:13 AM    1470043321-13099   22           68.169.155.25
e001fc4578e1af85   8/1/2016 9:21:46 AM    1470043321-13099   22           68.169.155.25
e001fc4578e1af85   8/1/2016 2:22:47 PM    1470061381-4525    24           68.169.176.247
e001fc4578e1af85   8/1/2016 2:22:53 PM    1470061381-4525    24           68.169.176.247
e001fc4578e1af85   8/1/2016 2:24:05 PM    1470061501-21433   25           68.169.176.247
e001fc4578e1af85   8/1/2016 2:24:55 PM    1470061501-25937   26           68.169.176.247
e001fc4578e1af85   8/1/2016 2:25:49 PM    1470061561-15467   27           68.169.176.247
e001fc4578e1af85   8/1/2016 2:25:54 PM    1470061561-15467   27           68.169.176.247
e001fc4578e1af85   8/1/2016 2:31:23 PM    1470061921-3889    28           68.169.176.247
e001fc4578e1af85   8/1/2016 2:31:42 PM    1470061921-3889    28           68.169.176.247
e001fc4578e1af85   8/1/2016 2:31:48 PM    1470061921-3889    28           68.169.176.247
e001fc4578e1af85   8/1/2016 5:09:13 PM    1470071401-4770    29           68.169.176.247
e001fc4578e1af85   8/1/2016 5:10:44 PM    1470071461-8748    31           68.169.176.247
e001fc4578e1af85   8/1/2016 5:10:50 PM    1470071461-6903    30           68.169.176.247
e001fc4578e1af85   8/1/2016 5:11:19 PM    1470071521-31408   32           68.169.176.247
e001fc4578e1af85   8/1/2016 5:11:46 PM    1470071521-31408   32           68.169.176.247
e001fc4578e1af85   8/1/2016 11:33:35 PM   1470094441-30365   33           68.169.155.25
e001fc4578e1af85   8/1/2016 11:38:16 PM   1470094741-30456   35           68.169.155.25
e001fc4578e1af85   8/1/2016 11:39:01 PM   1470094741-25788   34           68.169.155.25
e001fc4578e1af85   8/1/2016 11:39:10 PM   1470094801-13806   36           68.169.155.25
e001fc4578e1af85   8/1/2016 11:39:56 PM   1470094801-13806   36           68.169.155.25
e001fc4578e1af85   8/1/2016 11:40:08 PM   1470094861-7352    38           68.169.155.25
e001fc4578e1af85   8/1/2016 11:40:31 PM   1470094861-19826   37           68.169.155.25
e001fc4578e1af85   8/1/2016 11:40:37 PM   1470094861-19826   37           68.169.155.25
e001fc4578e1af85   8/1/2016 11:41:10 PM   1470094921-15567   39           68.169.155.25
e001fc4578e1af85   8/1/2016 11:42:05 PM   1470094981-28709   40           68.169.155.25
e001fc4578e1af85   8/1/2016 11:42:15 PM   1470094981-28709   40           68.169.155.25
e001fc4578e1af85   8/1/2016 11:42:30 PM   1470094981-28709   40           68.169.155.25
e001fc4578e1af85   8/1/2016 11:55:36 PM   1470095761-15325   41           68.169.155.25
e001fc4578e1af85   8/1/2016 11:57:27 PM   1470095881-28246   42           68.169.155.25
e001fc4578e1af85   8/1/2016 11:59:42 PM   1470096001-25928   43           68.169.155.25
e23d82a7173317ed   8/1/2016               1470009601-3017    1            74.171.62.15
e23d82a7173317ed   8/1/2016 12:00:08 AM   1470009661-10961   2            74.171.62.15
e23d82a7173317ed   8/1/2016 12:00:26 AM   1470009661-10961   2            74.171.62.15
e23d82a7173317ed   8/1/2016 12:00:29 AM   1470009661-18647   3            74.171.62.15
e23d82a7173317ed   8/1/2016 12:02:04 AM   1470009781-18011   4            74.171.62.15
e23d82a7173317ed   8/1/2016 11:56:47 PM   1470095821-2279    5            74.171.62.15
e23d82a7173317ed   8/1/2016 11:58:49 PM   1470095941-9587    6            74.171.62.15
e23d82a7173317ed   8/1/2016 11:59:02 PM   1470096001-4375    7            74.171.62.15
e23d82a7173317ed   8/1/2016 11:59:14 PM   1470096001-4375    7            74.171.62.15
e23d82a7173317ed   8/1/2016 11:59:39 PM   1470096001-4375    7            74.171.62.15
e23d82a7173317ed   8/1/2016 11:59:50 PM   1470096001-4375    7            74.171.62.15
*/


/*--------------------------------------------------------------------------------
END   Ex 3: FIND INTERESTING SH!*
--------------------------------------------------------------------------------*/