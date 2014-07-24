use tableau;

DROP TABLE IF EXISTS ${TABLE};

CREATE TABLE IF NOT EXISTS ${TABLE} (
  event_date DATE,
  publisher_id INT,
  hosting_domain STRING,
  player_id STRING,
  video_id STRING,
  video_index_id STRING,
  content_id INT,
  beacon_type STRING,
  beacon_count INT
);

INSERT OVERWRITE TABLE ${TABLE}
  SELECT
    pb.event_date,
    pb.publisher_id,
    pb.hosting_domain,
    pb.player_id,
    pb.video_id,
    pb.video_index_id,
    coalesce(c.id, 0) as id,
    pb.event_type,
    pb.count
  FROM 
  (
    select
      DATE(event_date) as event_date,
      coalesce(publisher_id,0) as publisher_id,
      coalesce(hosting_domain, 'Unknown') as hosting_domain,
      coalesce(player_id, 'NA') as player_id,
      coalesce(video_id, 'NA') as video_id,
      event_type,
      min(video_index_id) as video_index_id,
      count(*) as count
    FROM player_beacons pb
    WHERE video_id not in ('889e6b80-0621-012e-2ba9-12313b079c51','68664b27-3510-48f4-a1be-d0d0b64d3115')
      and y='${YEAR}' and m=${MONTH} and d in (${DAY})
    GROUP BY 
      DATE(event_date),
      coalesce(publisher_id, 0),
      coalesce(hosting_domain, 'Unknown'),
      coalesce(player_id, 'NA'),
      coalesce(video_id, 'NA'),
      event_type
  )  pb
  left outer join client_portal.contents c on c.uuid = pb.video_id
  where event_type is not NULL
;