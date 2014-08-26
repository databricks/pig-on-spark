
A = load ':INPATH:/page_views' using PigStorage()
as (user, action, timespent, query_term, ip_addr, timestamp,
estimated_revenue, page_info, page_links);
B = foreach A generate user, estimated_revenue;
alpha = load ':INPATH:/power_users' using PigStorage('\u0001') as (name, phone,
address, city, state, zip);
beta = foreach alpha generate name;
C = join B by user, beta by name using 'replicated' parallel 1;
store C into ':OUTPATH:/L2out';

