
A = load ':INPATH:/page_views' using PigStorage()
as (user, action, timespent:int, query_term, ip_addr, timestamp,
estimated_revenue:double, page_info, page_links);
B = order A by query_term, estimated_revenue desc, timespent parallel 1;
store B into ':OUTPATH:/L10out';
