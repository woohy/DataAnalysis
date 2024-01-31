-- sess_url_cnt : url별 첫 페이지 세션수, bounce : url별 pv=1만 발생(바로 이탈)시킨 세션id수
select dt, url, count(url) as sess_url_cnt, count(case when pv = 1 then 1 end) as bounce, round(count(case when pv = 1 then 1 end)/count(url)*100, 2) as bounce_rate
from (
    -- 첫 방문 visit = 1인 조건만 추출
    select *
    from (
        -- visit : 세션id별 방문 시간을 오름차순으로하여 방문 경로 나열, pv : session_id별 발생 pv수(이탈률 계산시 pv=1만 발생(유입 후 pv=1만 발생시키고 바로 이탈)시킨 session_id만 추출하기 위해 계산)
        select *, row_number() over (partition by dt, session_id order by ymdHMS asc) as visit,
            count(1) over (partition by dt, session_id order by ymdHMS ROWS between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) as pv
        from (
            -- 30분 기준 세션id 부여
            select *, CONCAT(zuid,CONCAT('_', SUM(seg) OVER (PARTITION BY dt, zuid, device ORDER BY ymdHMS))) AS session_id
            from (
                -- 30분 기준 세션 적용
                select *, unix_timestamp(ymdHMS) - unix_timestamp(ymdHMS_prev) as diff_ss,
                case when (unix_timestamp(ymdHMS) - unix_timestamp(ymdHMS_prev))/60 > 30
                    or datediff(to_date(ymdHMS), to_date(ymdHMS_prev)) >= 1 then 1
                else 0 end as seg
                from (
                    select *, NVL(LAG(ymdHMS) OVER(PARTITION BY zuid, dt, device ORDER BY ymdHMS),ymdHMS) AS ymdHMS_prev
                    from (
                        select zuid,
                        dt,
                        case when parse_url(prop_url, 'HOST') in ('url_host1', 'url_host2') then 'url_host1'
                            when parse_url(prop_url, 'HOST') = 'url_host3' then 'url_host3'
                            when parse_url(prop_url, 'HOST') = 'url_host4' then 'url_host4'
                            when parse_url(prop_url, 'HOST') = 'url_host5' then 'url_host5'
                            when (parse_url(prop_url, 'HOST') = 'url_host6' and split(parse_url(prop_url, 'PATH'), '/')[1] = 'm') then 'url_host6'
                            else 'others'
                        end as url,
                        user_agent_extractor(access_useragent, 'device') as device,
                        concat(concat(dt,' '),(substring(substring_index(substring_index(access_time,'/',-1),']',1),6,8))) as ymdHMS,
                        case when get_json_object(raw_json, '$.properties.property_name') = 'true' then 'true'
                            else 'false' end as is_Refresh
                        from zum.m_pvuv
                        where dt = '2023-06-30'
                        and zuid is not null
                        and access_host != ''
                        and not (parse_url(prop_url, 'HOST') like 'dev-%' or parse_url(prop_url, 'HOST') like 'pip-%' or parse_url(prop_url, 'HOST') like '%preview%')
                        ) T
                    where is_Refresh = 'false'
                    ) T
                ) T
            ) T
        ) T
    where visit = 1
    ) T
group by dt, url
order by dt, url