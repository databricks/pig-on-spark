
A = load ':INPATH:/page_views' using PigStorage('\u0001')
as (user, action, timespent, query_term, ip_addr, timestamp,
estimated_revenue, page_info, page_links);
B = foreach A generate user;
C = distinct B parallel 1;
alpha = load ':INPATH:/widerow' using PigStorage('\u0001');
beta = foreach alpha generate :0: as name;
gamma = distinct beta parallel 1;
D = union C, gamma;
E = distinct D parallel 1;
store E into ':OUTPATH:/L11out';
