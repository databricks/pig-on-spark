
A = load ':INPATH:/page_views' using PigStorage() as (user, action, timespent, query_term,
ip_addr, timestamp, estimated_revenue, page_info, page_links);
B = foreach A generate user, timestamp;
C = group B by user parallel 1;
D = foreach C {
morning = filter B by timestamp < 43200;
afternoon = filter B by timestamp >= 43200;
generate group, COUNT(morning), COUNT(afternoon);
}
store D into ':OUTPATH:/L7out';
