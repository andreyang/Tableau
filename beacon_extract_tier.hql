use tableau;

DROP TABLE IF EXISTS ${TABLE};

CREATE TABLE IF NOT EXISTS ${TABLE} (
  event_date DATE,
  hosting_domain STRING,
  player_id STRING,
  video_id STRING,
  video_index_id STRING,
  content_id INT,
  beacon_type STRING,
  beacon_count INT,
  domain_id INT,
  tier STRING
);

INSERT OVERWRITE TABLE ${TABLE}
  SELECT
    pb.event_date,
    pb.hosting_domain,
    pb.player_id,
    pb.video_id,
    pb.video_index_id,
    coalesce(c.id, 0) as content_id,
    pb.event_type,
    sum(pb.count) as count,
    coalesce(d.domain_id, 0) as domain_id,
    coalesce(d.tier, '0') as tier
  FROM 
  (
    select
      DATE(event_date) as event_date,
      coalesce(domain_id, 'NA') as domain_id,
      coalesce(hosting_domain, 'NA') as hosting_domain,
      coalesce(player_id, 'NA') as player_id,
      coalesce(video_id, 'NA') as video_id,
      event_type,
      coalesce(regexp_replace(video_index_id, 'undefined', '0'), '0') as video_index_id,
      count(*) as count
    FROM player_beacons pb
    WHERE y='${YEAR}' and m=${MONTH} and d in (${DAY})
    GROUP BY 
      DATE(event_date),
      coalesce(domain_id, 'NA'),
      coalesce(hosting_domain, 'NA'),
      coalesce(player_id, 'NA'),
      coalesce(video_id, 'NA'),
      event_type,
      coalesce(regexp_replace(video_index_id, 'undefined', '0'), '0')
  )  pb
  left outer join client_portal.contents c on c.uuid = pb.video_id
  left outer join ayang.domainlist_tableau_clean d on pb.domain_id = d.domain_uuid
  where pb.event_type is not NULL
  and pb.video_id not in ('889e6b80-0621-012e-2ba9-12313b079c51','68664b27-3510-48f4-a1be-d0d0b64d3115')
  group by pb.event_date,
    pb.hosting_domain,
    pb.player_id,
    pb.video_id,
    pb.video_index_id,
    coalesce(c.id, 0),
    pb.event_type,
    coalesce(d.domain_id, 0),
    coalesce(d.tier, '0')
  ;