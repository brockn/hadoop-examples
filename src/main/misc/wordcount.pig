lines = LOAD 'wordcount/input-small' AS (line:chararray);
words = FOREACH lines GENERATE FLATTEN(TOKENIZE(line)) AS word;
grpd = GROUP words BY word;
counts = FOREACH grpd GENERATE COUNT(words) AS count, group AS word;
ordered = ORDER counts by count DESC;


