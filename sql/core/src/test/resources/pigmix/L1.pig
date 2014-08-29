
A = load ':INPATH:/page_views' using PigStorage('\u0001')
as (user, action, timespent, query_term, ip_addr, timestamp,
estimated_revenue, page_info, page_links);
B = foreach A generate user, (int)action as action, (map[])page_info as page_info,
flatten((bag{tuple(map[])})page_links) as page_links;
C = foreach B generate user,
(action == 1 ? page_info#'a' : page_links#'b') as header;
D = group C by user parallel 1;
E = foreach D generate group, COUNT(C) as cnt;
store E into ':OUTPATH:/L1out';


