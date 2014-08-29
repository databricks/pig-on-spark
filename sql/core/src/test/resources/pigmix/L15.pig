A = load ':INPATH:/page_views' using PigStorage('\u0001')
as (user, action, timespent, query_term, ip_addr, timestamp, estimated_revenue, page_info, page_links);
B = foreach A generate user, action, estimated_revenue, timespent;
C = group B by user parallel 1;
D = foreach C {
beth = distinct B.action;
rev = distinct B.estimated_revenue;
ts = distinct B.timespent;
generate group, COUNT(beth), SUM(rev), (int)AVG(ts);
}
store D into ':OUTPATH:/L15out';

