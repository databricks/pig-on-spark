
A = load ':INPATH:/page_views' using PigStorage('\u0001')
as (user, action, timespent, query_term, ip_addr, timestamp,
estimated_revenue, page_info, page_links);
B = order A by query_term parallel 1;
store B into ':OUTPATH:/L9out';
