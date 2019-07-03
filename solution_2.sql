with cte_previous
as
(select user_id, happened_at, page, 
	lag(happened_at, 1) over (partition by user_id order by happened_at asc) as previous_time
from pages
where page in ('page1', 'page2', 'page3')),

cte_new_session
as
(select user_id, happened_at, page,
	case 
	when datediff(minute, previous_time, happened_at) > 60 or previous_time is null then 1
	else 0 
	end as new_session
from cte_previous),

cte_session_id
as
(select user_id, happened_at, page, 
	sum(new_session) over (partition by user_id order by happened_at rows between unbounded preceding and current row) as session_id
from cte_new_session)

select c.user_id, c.session_id
from 
	(select a.user_id, a.happened_at, a.session_id, b.page
	from cte_session_id as a
		inner join cte_session_id as b 
		on a.user_id = b.user_id 
			and a.session_id = b.session_id
		where a.page = 'page1' and b.page = 'page2'
		and datediff(min, a.happend_at, b.happened_at) > 0) as c
inner join cte_session_id as e
on c.user_id = e.user_id 
	and c.session_id = e.session_id
where e.page = 'page3' 
and datediff(min, c.happened_at, e.happened_at) > 0
