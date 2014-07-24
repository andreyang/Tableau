use tableau;

CREATE TABLE IF NOT EXISTS player_beacons(
	 event_date timestamp,
	 ip_address string,
	 session_id string,
	 instance_id string,
	 hosting_page string,
	 hosting_domain string,
	 player_id string,
	 publisher_id int,
	 video_catalog_id string,
	 video_index_id string,
	 video_id string,
	 domain_id string,
	 lable string,
	 playlist_id string,
	 player_type string,
	 document_hidden string,
	 event_type string,
         beacon_idx INT,
	 server_useragent string,
	 server_referer string,
	 player_useragent string,
	 player_referer string,
	 action string,
	 counter string,
	 player_size string,
	 coordinates string,
	 page_location string,
	 autostart string)
PARTITIONED BY (y STRING, m STRING, d STRING, h STRING)
STORED AS orc
;