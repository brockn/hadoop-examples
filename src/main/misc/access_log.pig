 /*
  * Register piggbank (contrib code)
  */
REGISTER /usr/lib/pig/contrib/piggybank/java/piggybank.jar;


 /*
  * Load file from HDFS giving it an alias of `logs'
  */
logs = LOAD 'access_log/input-small' USING
org.apache.pig.piggybank.storage.apachelog.CombinedLogLoader()
AS
(
  remoteAddr,
  remoteLogname,
  user,
  time,
  method,
  uri,
  proto,
  status,
  bytes,
  referer,
  userAgent
);

 /*
  * Find the record which hacked this site
  */

bad = FILTER logs BY uri MATCHES '.*=http.*';

/*
 * Generate Counts by Date
 */

/* extract the day part of dates */
days = FOREACH logs GENERATE
  org.apache.pig.piggybank.evaluation.string.RegexExtract
  (time, '^(\\d+/\\w+)/', 1);

/* Group them */
days_grped = GROUP days BY $0;

/* Count how many hits in each day */
day_counts = FOREACH days_grped GENERATE 
  group, COUNT($1) AS cnt;

/* ORDER by the count in DESCENDING order */
day_ordered = ORDER day_counts BY cnt DESC;

/* Trim off the top 10 records */
day_result = LIMIT day_ordered 10;


/* Filter out junk */
logs_filtered = FOREACH logs 
  GENERATE remoteAddr, 
  SUBSTRING(userAgent, 0, 5) as userAgent,
  uri, status, bytes;


/*
 * Sessionize by ip address and user agent
 */
/* Group by ip address and user agent (our fake session token) */
session = GROUP logs_filtered BY (remoteAddr, userAgent);

/* Calculate metrics */
session_counted = FOREACH session GENERATE 
  group, COUNT(logs_filtered) AS cnt, AVG(logs.bytes) AS avg, 
    MAX(logs_filtered.bytes) as max;


/* Calculate metrics */
session_counted = FOREACH session GENERATE 
  group, COUNT(logs) AS cnt, AVG(logs.bytes) AS avg, 
    MAX(logs.bytes) as max;

/* Order by the count */
session_ordered = ORDER session_counted BY cnt DESC;


/* Trim the top 2 records */
session_result = LIMIT session_ordered 2;

STORE session_result INTO 'access_log/output-session-result';

STORE bad INTO 'access_log/output-bad';

STORE day_result INTO 'access_log/output-day-result';


