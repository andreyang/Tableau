use tableau;

INSERT INTO TABLE player_beacons PARTITION (y='${YEAR}', m, d, h) 
  select
    q.event_date,
    q.ip_address,
    q.session_id,
    q.instance_id,
    q.hosting_page,
    q.hosting_domain,
    q.player_id,
    q.publisher_id,
    q.video_catalog_id,
    q.video_index_id,
    q.video_id,
    q.domain_id,
    q.lable,
    q.playlist_id,
    q.player_type,
    q.document_hidden,
    case 
      when q.event_type = 'rgpluginloads' then 'plugin_load'
      when q.event_type = 'adnotenabled' then 'ad_not_enabled'
      when q.event_type = 'adpresent' then
        case
          when q.action like '%before%' then 'pre_roll_delivery'
          when q.action like '%during%' then 'mid_roll_delivery'
          when q.action like '%after%' then 'post_roll_delivery'
          when q.action like '%branding%' then 'branding_delivery'
          else 'delivery_other'
        end
      when q.event_type = 'adabsent' then
        case
          when q.action like '%before%' then 'pre_roll_absent'
          when q.action like '%during%' then 'mid_roll_absent'
          when q.action like '%after%' then 'post_roll_absent'
          when q.action like '%branding%' then 'branding_absent'
          else 'adabsent_other'
        end
      when q.event_type = ('adrequested') then
        case
          when q.action like '%before%' then 'pre_roll_request'
          when q.action like '%during%' then 'mid_roll_request'
          when q.action like '%after%' then 'post_roll_request'
          when q.action like '%branding%' then 'branding_request'
          else 'adrequest_other'
        end
      when q.event_type = ('adplaying') then
        case
          when q.action like '%before%' then 'pre_roll_play'
          when q.action like '%during%' then 'mid_roll_play'
          when q.action like '%after%' then 'post_roll_play'
          when q.action like '%branding%' then 'branding_play'
          else 'adplay_other'
        end
      when q.event_type = ('adresponse') then
        case
          when q.action like '%before%' then 'pre_roll_response'
          when q.action like '%during%' then 'mid_roll_response'
          when q.action like '%after%' then 'post_roll_response'
          when q.action like '%branding%' then 'branding_response'
          else 'adresponse_other'
        end
      when q.event_type = ('adclicked2site') then
        case
          when q.action like '%before%' then 'pre_roll_click2site'
          when q.action like '%during%' then 'mid_roll_click2site'
          when q.action like '%after%' then 'post_roll_click2site'
          when q.action like '%branding%' then 'branding_click2site'
          else 'click2site_other'
        end
      when q.event_type = ('adskipped') then
        case
          when q.action like '%before%' then 'pre_roll_skip'
          when q.action like '%during%' then 'mid_roll_skip'
          when q.action like '%after%' then 'post_roll_skip'
          when q.action like '%branding%' then 'branding_skip'
          else 'ad_skip_other'
        end
      when q.event_type = ('adcompleted') then
        case
          when q.action like '%before%' then 'pre_roll_complete'
          when q.action like '%during%' then 'mid_roll_complete'
          when q.action like '%after%' then 'post_roll_complete'
          when q.action like '%branding%' then 'branding_complete'
          else 'ad_complete_other'
        end
      when q.event_type = ('adsstopped') then
        case
          when q.action like '%geoblocking%' then 'ad_stop_geoblock'
          when q.action like '%adprovider%' then 'ad_stop_nullprovider'
          else 'ad_stop_other'
        end
      when q.event_type in ('jwplayerready', 'playerready') then 'player_load'
      when q.event_type in ('jwplayerplaylistitem', 'playerplaylistitem') then 'media_load'
      when q.event_type in ('jwplayerplaylistloaded', 'playerplaylistloaded') then 'playlist_load'
      when q.event_type in ('jwplayermediatime', 'playermediatime') then
        case 
          when q.counter = 0 then 'video_content_start'
          when q.counter between 1 and 10 then 'at10' 
          when q.counter between 11 and 20 then 'at20'
          when q.counter between 21 and 30 then 'at30' 
          when q.counter between 31 and 40 then 'at40'
          when q.counter between 41 and 50 then 'at50' 
          when q.counter between 51 and 60 then 'at60' 
          when q.counter between 61 and 70 then 'at70' 
          when q.counter between 71 and 80 then 'at80' 
          when q.counter between 81 and 90 then 'at90' 
          when q.counter between 91 and 100 then 'at100' 
          else 'at_other'
        end
      when q.event_type in ('jwplayerplayerstate', 'playerplayerstate') then
        case
          when q.action like '%paused' then 'video_pause'
          when q.action like '%playing' then 'video_unpause'
          else 'video_action_other'
        end
      when q.event_type in ('jwplayermediaerror', 'mediaerror') then 'media_error'
      when q.event_type in ('jwplayermediamute', 'playermediamute') then 'media_mute'
      when q.event_type in ('jwplayermediavolume', 'playermediavolume') then 'volume_change'
      else q.event_type
    end as event_type,
    q.beacon_idx,
    q.server_useragent,
    q.server_referer,
    q.player_useragent,
    q.player_referer,
    q.action,
    q.counter,
    q.player_size,
    q.coordinates,
    q.player_location,
    regexp_replace(q.settings, '.* ', '') as autostart,                       
    printf('%02d', MONTH(q.event_date)) as m,
    printf('%02d', DAY(q.event_date)) as d,
    printf('%02d', HOUR(q.event_date)) as h
  from (          
  SELECT 
    from_utc_timestamp(cast(time_local as bigint), "EST") as event_date,
    ip as ip_address,
    params['rg_session'] as session_id,
    params['rg_instance'] as instance_id,
    regexp_replace(reflect('java.net.URLDecoder','decode',reflect('java.net.URLDecoder','decode',params['rg_page_host_url'])), '[#?].*$', '') as hosting_page,
    parse_url(reflect('java.net.URLDecoder','decode',reflect('java.net.URLDecoder','decode',params['rg_page_host_url'])), 'HOST') as hosting_domain,
    params['rg_player_uuid'] as player_id,
    params['rg_publisher_id'] as publisher_id,
    params['rg_video_catalog_id'] as video_catalog_id,
    params['rg_video_index_id'] as video_index_id,
    params['rg_guid'] as video_id,
    params['rg_domain_id'] as domain_id,
    params['rg_lable'] as lable,
    params['rg_playlist_id'] as playlist_id,
    params['rg_player_type'] as player_type,
    params['rg_document_hidden'] as document_hidden,
    lower(params['rg_event']) as event_type,
    useragent as server_useragent,
    referer as server_referer,
    reflect('java.net.URLDecoder','decode', params['rg_user_agent']) as player_useragent,
    reflect('java.net.URLDecoder','decode', params['rg_referrer']) as player_referer,
    lower(params['rg_action']) as action,
    regexp_replace(regexp_replace(params['rg_counter'], '.*%20', ''), '.*\\+', '') as counter,
    params['rg_size'] as player_size,
    reflect('java.net.URLDecoder','decode',params['rg_coordinates']) as coordinates,
    reflect('java.net.URLDecoder','decode',params['rg_position']) as player_location,
    regexp_replace(regexp_replace(params['rg_settings'], '.*%20', ''), '.*\\+', '') as settings,
    params['rg_beacon_idx'] as beacon_idx
  FROM cocoon.data_primitives
  WHERE
    y='${YEAR}' and m in (${MONTH}) and d in (${DAY}) and h in (${HOUR})
  ) q WHERE q.event_type is not null
  ;