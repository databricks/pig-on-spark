A = load ':INPATH:/page_views' using PigStorage('\u0001')
as (user, action, timespent, query_term, ip_addr, timestamp, estimated_revenue, page_info, page_links);
B = foreach A generate user, estimated_revenue;
C = group B by user parallel 1;
D = foreach C {
E = order B by estimated_revenue;
F = E.estimated_revenue;
generate group, SUM(F);
}

store D into ':OUTPATH:/L16out';

