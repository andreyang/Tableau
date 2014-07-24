use tableau;

DROP TABLE IF EXISTS ${table};

CREATE TABLE ${table} as
  SELECT
    pb.event_date,
    pb.hosting_domain,
    pb.player_id,
    pb.video_id,
    pb.video_index_id,
    coalesce(c.id, 0) as content_id,
    pb.event_type as beacon_type,
    sum(pb.count) as beacon_count,
    coalesce(p.domain_id, 0) as domain_id,
    coalesce(d.tier, '0') as tier
  FROM 
  (
    select
      concat('${year_yesterday}', '-', '${month_yesterday}', '-', '${date_yesterday}')  as event_date,
      coalesce(parse_url(reflect('java.net.URLDecoder','decode', reflect('java.net.URLDecoder','decode', params['client.referrer_url'])), 'HOST'), 'NA') as hosting_domain,
      coalesce(params['client.player_id'], 'NA') as player_id,
      coalesce(params['detail.media.video_id'], 'NA') as video_id,
      coalesce(params['detail.media.index'], '0') as video_index_id,
      case when upper(params['_event']) = 'WIDGET_CREATED' then 'player_load'
      when upper(params['_event']) = 'LOAD' then 'video_load'
      when upper(params['_event']) = 'AD_PLAY' AND upper(params['detail.ads.slot']) = 'PREROLL' then 'pre_roll_ad_begin'
      when upper(params['_event']) = 'AD_CLICK' AND upper(params['detail.ads.slot']) = 'PREROLL' then 'pre_roll_ad_click'
      when upper(params['_event']) = 'AD_ENDED' AND upper(params['detail.ads.slot']) = 'PREROLL' then 'pre_roll_ad_complete'
      when upper(params['_event']) = 'AD_PLAY' AND upper(params['detail.ads.slot']) = 'MIDROLL' then 'mid_roll_ad_begin'
      when upper(params['_event']) = 'AD_CLICK' AND upper(params['detail.ads.slot']) = 'MIDROLL' then 'mid_roll_ad_click'
      when upper(params['_event']) = 'AD_ENDED' AND upper(params['detail.ads.slot']) = 'MIDROLL' then 'mid_roll_ad_complete'
      when upper(params['_event']) = 'AD_PLAY' AND upper(params['detail.ads.slot']) = 'POSTROLL' then 'post_roll_ad_begin'
      when upper(params['_event']) = 'AD_CLICK' AND upper(params['detail.ads.slot']) = 'POSTROLL' then 'post_roll_ad_click'
      when upper(params['_event']) = 'AD_ENDED' AND upper(params['detail.ads.slot']) = 'POSTROLL' then 'post_roll_ad_complete'
      when upper(params['_event']) = 'AD_PLAY' AND upper(params['detail.ads.slot']) = 'OVERLAY' then 'overlay_ad_begin'
      when upper(params['_event']) = 'AD_CLICK' AND upper(params['detail.ads.slot']) = 'OVERLAY' then 'overlay_ad_click'
      when upper(params['_event']) = 'AD_ENDED' AND upper(params['detail.ads.slot']) = 'OVERLAY' then 'overlay_ad_complete'
      when upper(params['_event']) = 'DECILE_UPDATE' AND params['detail.media.decile.percentage'] = '0' then 'at0'
      when upper(params['_event']) = 'DECILE_UPDATE' AND params['detail.media.decile.percentage'] = '10' then 'at10'
      when upper(params['_event']) = 'DECILE_UPDATE' AND params['detail.media.decile.percentage'] = '20' then 'at20'
      when upper(params['_event']) = 'DECILE_UPDATE' AND params['detail.media.decile.percentage'] = '30' then 'at30'
      when upper(params['_event']) = 'DECILE_UPDATE' AND params['detail.media.decile.percentage'] = '40' then 'at40'
      when upper(params['_event']) = 'DECILE_UPDATE' AND params['detail.media.decile.percentage'] = '50' then 'at50'
      when upper(params['_event']) = 'DECILE_UPDATE' AND params['detail.media.decile.percentage'] = '60' then 'at60'
      when upper(params['_event']) = 'DECILE_UPDATE' AND params['detail.media.decile.percentage'] = '70' then 'at70'
      when upper(params['_event']) = 'DECILE_UPDATE' AND params['detail.media.decile.percentage'] = '80' then 'at80'
      when upper(params['_event']) = 'DECILE_UPDATE' AND params['detail.media.decile.percentage'] = '90' then 'at90'
      when upper(params['_event']) = 'DECILE_UPDATE' AND params['detail.media.decile.percentage'] = '100'then 'at100'
      else lower(params['_event']) end as event_type,
      count(*) as count
      FROM butterfly.data_primitive
      where ((y = '${year_yesterday}' and m = '${month_yesterday}' and d= '${date_yesterday}' and h >= '${time_shift}') 
      or (y = '${year_today}' and m = '${month_today}' and d= '${date_today}' and h < '${time_shift}'))
      GROUP BY 
      concat('${year_yesterday}', '-', '${month_yesterday}', '-', '${date_yesterday}'),
      coalesce(parse_url(reflect('java.net.URLDecoder','decode', reflect('java.net.URLDecoder','decode', params['client.referrer_url'])), 'HOST'), 'NA'),
      coalesce(params['client.player_id'], 'NA'),
      coalesce(params['detail.media.video_id'], 'NA'),
      coalesce(params['detail.media.index'], '0'),
      case when upper(params['_event']) = 'WIDGET_CREATED' then 'player_load'
      when upper(params['_event']) = 'LOAD' then 'video_load'
      when upper(params['_event']) = 'AD_PLAY' AND upper(params['detail.ads.slot']) = 'PREROLL' then 'pre_roll_ad_begin'
      when upper(params['_event']) = 'AD_CLICK' AND upper(params['detail.ads.slot']) = 'PREROLL' then 'pre_roll_ad_click'
      when upper(params['_event']) = 'AD_ENDED' AND upper(params['detail.ads.slot']) = 'PREROLL' then 'pre_roll_ad_complete'
      when upper(params['_event']) = 'AD_PLAY' AND upper(params['detail.ads.slot']) = 'MIDROLL' then 'mid_roll_ad_begin'
      when upper(params['_event']) = 'AD_CLICK' AND upper(params['detail.ads.slot']) = 'MIDROLL' then 'mid_roll_ad_click'
      when upper(params['_event']) = 'AD_ENDED' AND upper(params['detail.ads.slot']) = 'MIDROLL' then 'mid_roll_ad_complete'
      when upper(params['_event']) = 'AD_PLAY' AND upper(params['detail.ads.slot']) = 'POSTROLL' then 'post_roll_ad_begin'
      when upper(params['_event']) = 'AD_CLICK' AND upper(params['detail.ads.slot']) = 'POSTROLL' then 'post_roll_ad_click'
      when upper(params['_event']) = 'AD_ENDED' AND upper(params['detail.ads.slot']) = 'POSTROLL' then 'post_roll_ad_complete'
      when upper(params['_event']) = 'AD_PLAY' AND upper(params['detail.ads.slot']) = 'OVERLAY' then 'overlay_ad_begin'
      when upper(params['_event']) = 'AD_CLICK' AND upper(params['detail.ads.slot']) = 'OVERLAY' then 'overlay_ad_click'
      when upper(params['_event']) = 'AD_ENDED' AND upper(params['detail.ads.slot']) = 'OVERLAY' then 'overlay_ad_complete'
      when upper(params['_event']) = 'DECILE_UPDATE' AND params['detail.media.decile.percentage'] = '0' then 'at0'
      when upper(params['_event']) = 'DECILE_UPDATE' AND params['detail.media.decile.percentage'] = '10' then 'at10'
      when upper(params['_event']) = 'DECILE_UPDATE' AND params['detail.media.decile.percentage'] = '20' then 'at20'
      when upper(params['_event']) = 'DECILE_UPDATE' AND params['detail.media.decile.percentage'] = '30' then 'at30'
      when upper(params['_event']) = 'DECILE_UPDATE' AND params['detail.media.decile.percentage'] = '40' then 'at40'
      when upper(params['_event']) = 'DECILE_UPDATE' AND params['detail.media.decile.percentage'] = '50' then 'at50'
      when upper(params['_event']) = 'DECILE_UPDATE' AND params['detail.media.decile.percentage'] = '60' then 'at60'
      when upper(params['_event']) = 'DECILE_UPDATE' AND params['detail.media.decile.percentage'] = '70' then 'at70'
      when upper(params['_event']) = 'DECILE_UPDATE' AND params['detail.media.decile.percentage'] = '80' then 'at80'
      when upper(params['_event']) = 'DECILE_UPDATE' AND params['detail.media.decile.percentage'] = '90' then 'at90'
      when upper(params['_event']) = 'DECILE_UPDATE' AND params['detail.media.decile.percentage'] = '100'then 'at100'
      else lower(params['_event']) end
  )  pb
  left outer join client_portal.players p on (pb.player_id = p.uuid)
  left outer join client_portal.contents c on c.uuid = pb.video_id
  left outer join ayang.domainlist_tableau_clean d on p.domain_id = d.domain_uuid
  where pb.event_type is not NULL
  and pb.video_id not in ('889e6b80-0621-012e-2ba9-12313b079c51','68664b27-3510-48f4-a1be-d0d0b64d3115')
  group by pb.event_date,
    pb.hosting_domain,
    pb.player_id,
    pb.video_id,
    pb.video_index_id,
    coalesce(c.id, 0),
    pb.event_type,
    coalesce(p.domain_id, 0),
    coalesce(d.tier, '0')
  ;