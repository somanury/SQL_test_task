with cte_previous
as
(select user_id, happened_at, page, 
lag(happened_at, 1) over (partition by user_id order by happened_at asc) as previous_time
from pages
where page in ('page1', 'page2', 'page3')
),

cte_new_session
as
(select user_id, happened_at, page,
case when datediff(minute, previous_time, happened_at) > 60 or previous_time is null then 1 else 0 end as new_session
from 
cte_previous
),

cte_session_id
as
(select user_id, happened_at, page, 
sum(new_session) over(partition by user_id order by happened_at rows between unbounded preceding and current row) as session_id
from
cte_new_session
),

-- optional: allows to clean path from page refreshing
cte_previous_page
as
(select user_id, session_id, page, happened_at,
lag(page, 1) over (partition by user_id, session_id order by happened_at asc) as previous_page,
case when previous_page is not null then previous_page + ', ' + page else page end as minipath
from
cte_session_id
)

select user_id, session_start_time, session_end_time
from 
(select user_id, listagg(minipath, ', ') as path, 
min(happened_at) as session_start_time,
max(happened_at) as session_end_time
from
		-- optional: allows to clean path from page refreshing
    (select session_id, page, user_id, happened_at, previous_page, minipath
    from cte_previous_page
    where minipath not in ('page1, page1', 'page2, page2', 'page3, page3'))
group by user_id, session_id
having path like ('%page1%page2%page3%'))
