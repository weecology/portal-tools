DROP TABLE IF EXISTS queries.mcdb1;
CREATE TABLE queries.mcdb1 
SELECT main.*
FROM PortalProjectMammals.Species INNER JOIN PortalProjectMammals.main ON Species.SpeciesCode = main.species
WHERE (((main.period) > 6) AND ((main.stake) > 10 AND (main.stake) < 78) 
        AND ((Species.Rodent)=1) AND ((Species.Unknown)=0) AND ((main.plot) = 2 OR (main.plot) = 4 OR (main.plot) = 8 
        OR (main.plot) = 11 OR (main.plot) = 12 OR (main.plot) = 14 OR (main.plot) = 17 OR (main.plot) = 22));

DROP TABLE IF EXISTS queries.mcdb2;
CREATE TABLE queries.mcdb2 
SELECT mcdb1.year AS YEAR, mcdb1.species AS SP, COUNT(mcdb1.ID) AS AB
FROM queries.mcdb1
GROUP BY mcdb1.year, mcdb1.species;

SELECT * FROM queries.mcdb2
INTO OUTFILE '/tmp/portal_mcdb.csv'
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n';

DROP TABLE IF EXISTS queries.mcdb3;
CREATE TABLE queries.mcdb3 
SELECT mcdb1.year AS YEAR, mcdb1.period AS PD
FROM queries.mcdb1
GROUP BY mcdb1.year, mcdb1.period;

DROP TABLE IF EXISTS queries.mcdb4;
CREATE TABLE queries.mcdb4 
SELECT mcdb3.YEAR AS YEAR, COUNT(mcdb3.PD) AS No_pd
FROM queries.mcdb3
GROUP BY mcdb3.year;

SELECT * FROM queries.mcdb4
INTO OUTFILE '/tmp/portal_mcdb_periods.csv'
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n';
