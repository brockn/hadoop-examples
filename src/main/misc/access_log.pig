REGISTER /usr/lib/pig/contrib/piggybank/java/piggybank.jar;
DEFINE LogLoader org.apache.pig.piggybank.storage.apachelog.CombinedLogLoader();

logs = LOAD 'access_log/input' USING LogLoader as
(remoteAddr, remoteLogname, user, time, method, 
uri, proto, status, bytes, referer, userAgent);

 /*
  * Find the record which hacked this site
  */

bad = FILTER logs BY uri MATCHES '.*=http.*';

/*
 * Generate Counts by Date
 */

/* extract the day part of dates */
days = FOREACH logs GENERATE
  org.apache.pig.piggybank.evaluation.string.RegexExtract(time, '^(\\d+/\\w+)/', 1);
/* Group them */
days_grped = GROUP days BY $0;
/* Count how many hits in each day */
day_counts = FOREACH days_grped GENERATE $0, COUNT($1) AS cnt;
day_ordered = ORDER day_counts BY cnt DESC;
day_result = LIMIT day_ordered 10;

/*
 * Sessionize by ip address and user agent
 */

session = GROUP logs BY (remoteAddr, userAgent);
session_counted = FOREACH session GENERATE group, COUNT($1) AS cnt;
session_ordered = ORDER session_counted BY cnt DESC;
session_result = LIMIT session_ordered 10;

STORE bad INTO 'access_log/output-bad';
STORE day_result INTO 'access_log/output-day-result';
STORE session_result INTO 'access_log/output-session-result';
