

A = load ':INPATH:/page_views' using PigStorage()
as (user, action, timespent, query_term, ip_addr, timestamp,
estimated_revenue, page_info, page_links);
B = foreach A generate user;
alpha = load ':INPATH:/users' using PigStorage('\u0001') as (name, phone, address,
city, state, zip);
beta = foreach alpha generate name;
C = cogroup beta by name, B by user parallel 1;
D = filter C by COUNT(beta) == 0;
E = foreach D generate group;
store E into ':OUTPATH:/L5out';
