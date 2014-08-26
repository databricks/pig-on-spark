
A = load ':INPATH:/page_views' using PigStorage()
as (user, action, timespent, query_term, ip_addr, timestamp,
estimated_revenue, page_info, page_links);
B = foreach A generate user, action;
C = group B by user parallel 1;
D = foreach C {
aleph = B.action;
beth = distinct aleph;
generate group, COUNT(beth);
}
store D into ':OUTPATH:/L4out';
