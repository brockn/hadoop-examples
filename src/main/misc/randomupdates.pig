records = LOAD 'randomupdates/input-small' AS 
  (tranid, timestamp:long, amount:float);

grpd = GROUP records BY tranid;

deduped = FOREACH grpd {
  ordered = ORDER records BY timestamp DESC;
  top1 = LIMIT ordered 1;
  GENERATE top1;
}

