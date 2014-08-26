A = load ':INPATH:/widegroupbydata' using PigStorage()
as (user, action, timespent, query_term, ip_addr, timestamp,
estimated_revenue, page_info, page_links, user_1, action_1, timespent_1, query_term_1, ip_addr_1, timestamp_1,
estimated_revenue_1, page_info_1, page_links_1, user_2, action_2, timespent_2, query_term_2, ip_addr_2, timestamp_2,
estimated_revenue_2, page_info_2, page_links_2);
B = group A by (user, action, timespent, query_term, ip_addr, timestamp,
estimated_revenue, user_1, action_1, timespent_1, query_term_1, ip_addr_1, timestamp_1,
estimated_revenue_1, user_2, action_2, timespent_2, query_term_2, ip_addr_2, timestamp_2,
estimated_revenue_2) parallel 1;
C = foreach B generate SUM(A.timespent), SUM(A.timespent_1), SUM(A.timespent_2), AVG(A.estimated_revenue), AVG(A.estimated_revenue_1), AVG(A.estimated_revenue_2);
store C into ':OUTPATH:/L17out';

