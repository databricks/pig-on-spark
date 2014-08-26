


A = load ':INPATH:/page_views' using PigStorage()
as (user, action, timespent, query_term, ip_addr, timestamp,
estimated_revenue, page_info, page_links);
B = foreach A generate user, (double)estimated_revenue;
alpha = load ':INPATH:/users' using PigStorage('\u0001') as (name, phone, address,
city, state, zip);
beta = foreach alpha generate name;
C = join beta by name, B by user parallel 1;
D = group C by :0: parallel 1;
E = foreach D generate group, SUM(C.estimated_revenue);
store E into ':OUTPATH:/L3out';

