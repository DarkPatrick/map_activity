select
    toUnixTimestamp(datetime) as datetime,
    source,
    platform,
    ip_address,
    country,
    is_bot,
    uniq(user_id) as user_cnt
from
    buffer.ug_session_events_buffer as e
where
    date = today()
and
    event = 'Session Start'
and
    e.datetime between now() - interval 1 minute and now()
group by
    datetime,
    source,
    platform,
    ip_address,
    country,
    is_bot
order by
    datetime
