
A = load ':INPATH:/page_views' using PigStorage('\u0001')
as (user, action, timespent, query_term, ip_addr, timestamp,
estimated_revenue, page_info, page_links);
B = foreach A generate user, action, (int)timespent as timespent, query_term,
(double)estimated_revenue as estimated_revenue;
split B into C if user is not null, alpha if user is null;
split C into D if query_term is not null, aleph if query_term is null;
E = group D by user parallel 1;
F = foreach E generate group, MAX(D.estimated_revenue);
store F into ':OUTPATH:/highest_value_page_per_user';
beta = group alpha by query_term parallel 1;
gamma = foreach beta generate group, SUM(alpha.timespent);
store gamma into ':OUTPATH:/total_timespent_per_term';
beth = group aleph by action parallel 1;
gimel = foreach beth generate group, COUNT(aleph);
store gimel into ':OUTPATH:/queries_per_action';
