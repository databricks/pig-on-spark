
A = load ':INPATH:/page_views' using PigStorage('\u0001')
as (user, action, timespent, query_term, ip_addr, timestamp,
estimated_revenue, page_info, page_links);
B = foreach A generate user, (int)timespent as timespent, (double)estimated_revenue as estimated_revenue;
C = group B all;
D = foreach C generate SUM(B.timespent), AVG(B.estimated_revenue);
store D into ':OUTPATH:/L8out';
