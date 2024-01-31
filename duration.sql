-- utm 캠페인별 체류시간 계산(hiveQL)
select
    dt,
    user_seg,
    utm_source,
    utm_medium,
    utm_campaign,
    count(distinct zuid) as uv,
    sum(diff_min) as total_duration,
    sum(diff_min) / count(distinct zuid) as avg_duration
from (
select dt,
       zuid,
       user_seg,
       utm_source,
       utm_medium,
       utm_campaign,
       session_id,
       ROUND(SUM(diff_ss)/60, 0) as diff_min
from (
   select *
          ,CONCAT(zuid,CONCAT('_', SUM(seg) OVER (PARTITION BY zuid, dt, utm_source, utm_medium, utm_campaign ORDER BY ymdHMS))) AS session_id
from (
    select *,
            unix_timestamp(ymdHMS) -unix_timestamp(ymdHMS_prev) AS diff_ss
            ,CASE
            WHEN (UNIX_TIMESTAMP(ymdHMS) - UNIX_TIMESTAMP(ymdHMS_prev))/60 > 30
                OR datediff(to_date(ymdHMS), to_date(ymdHMS_prev))>=1 THEN 1
            ELSE 0
          END AS seg
from (
     SELECT *,
            NVL(LAG(ymdHMS) OVER(PARTITION BY zuid, dt, utm_source, utm_medium, utm_campaign ORDER BY ymdHMS),ymdHMS) AS ymdHMS_prev
     from (
        select
             zuid
            ,user_seg
            ,dt
            ,utm_source
            ,utm_medium
            ,utm_campaign
            ,utm_time
            ,pv_time as ymdHMS
        from (
            select
                 zuid
                ,user_seg
                ,dt
                ,utm_source
                ,utm_medium
                ,utm_campaign
                ,utm_time
                ,pv_time
                ,row_number() over(partition by dt, zuid, utm_medium, pv_time order by utm_time desc) as utm_time_num
            from (
                select
                     UU.zuid as zuid
                    ,UU.user_seg as user_seg
                    ,UU.dt as dt
                    ,UU.url_host as url_host
                    ,UU.utm_source as utm_source
                    ,UU.utm_medium as utm_medium
                    ,UU.utm_campaign as utm_campaign
                    ,UU.acc_time as utm_time
                    ,IP.acc_time as pv_time
                from (
                    select
                         T.dt as dt
                        ,T.url_host as url_host
                        ,T.utm_source as utm_source
                        ,T.utm_medium as utm_medium
                        ,T.utm_campaign as utm_campaign
                        ,T.zuid as zuid
                        ,T.acc_time as acc_time
                        ,case when Z.last_visit is null then 'new'
                            when Z.last_visit < date_add(date_format(current_date, 'yyyy-MM-dd'), -31) then 'return'
                            else 'active'
                         end as user_seg
                    from (
                        select
                             dt
                            ,parse_url(prop_url, 'HOST') as url_host
                            ,parse_url(prop_url, 'QUERY', 'utm_source') as utm_source
                            ,parse_url(prop_url, 'QUERY', 'utm_medium') as utm_medium
                            ,parse_url(prop_url, 'QUERY', 'utm_campaign') as utm_campaign
                            ,zuid
                            ,concat(concat(dt,' '),(substring(substring_index(substring_index(access_time,'/',-1),']',1),6,8))) as acc_time
                        from zum.m_pvuv
                        where dt = date_add(date_format(current_date, 'yyyy-MM-dd'), -1)
                        and zuid is not null
                        and parse_url(prop_url, 'HOST') in ('url_host1', 'url_host2')
                        and parse_url(prop_url, 'QUERY', 'utm_source') is not null
                        and parse_url(prop_url, 'QUERY', 'utm_source') != ''
                        and parse_url(prop_url, 'QUERY', 'utm_campaign') is not null
                        and parse_url(prop_url, 'QUERY', 'utm_campaign') != ''
                        and parse_url(prop_url, 'QUERY', 'utm_medium') is not null
                        and parse_url(prop_url, 'QUERY', 'utm_medium') != ''
                        ) T
                    left join analysis.invest_zuid_6month Z on T.zuid = Z.zuid
                ) UU
                left join (
                    select
                         zuid
                        ,dt
                        ,parse_url(prop_url, 'HOST') as url_host
                        ,concat(concat(dt,' '),(substring(substring_index(substring_index(access_time,'/',-1),']',1),6,8))) as acc_time
                    from zum.m_pvuv
                    where dt = date_add(date_format(current_date, 'yyyy-MM-dd'), -1)
                    and zuid is not null
                    and parse_url(prop_url, 'HOST') in ('url_host1', 'url_host2')
                    ) IP on UU.zuid = IP.zuid and UU.url_host = IP.url_host
                where unix_timestamp(UU.acc_time) <= unix_timestamp(IP.acc_time)
            ) T
        ) T
        where utm_time_num = 1
     ) A
    ) A
   ) A
   WHERE A.diff_ss > 0 AND  A.diff_ss <= 30*60
   ) A
group by dt, zuid, user_seg, utm_source, utm_medium, utm_campaign, session_id
) A
group by dt, user_seg, utm_source, utm_medium, utm_campaign
order by dt, utm_source, utm_medium