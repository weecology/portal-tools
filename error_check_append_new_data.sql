/* add new data file to queries database */
/* this step is best accomplished via the mysql command line in a putty session connected to serenity  */
DROP TABLE IF EXISTS queries.newdat;
CREATE TABLE queries.newdat
( 
    mo DOUBLE,
    dy DOUBLE,
    yr DOUBLE, 
    period DOUBLE, 
    plot TINYINT(4), 
    note1 VARCHAR(255) DEFAULT NULL, 
    stake DOUBLE DEFAULT NULL, 
    sp VARCHAR(255) DEFAULT NULL,
    sex VARCHAR(255) DEFAULT NULL,
    male1 VARCHAR(255) DEFAULT NULL,
    male2 VARCHAR(255) DEFAULT NULL,
    male3 VARCHAR(255) DEFAULT NULL,
    female1 VARCHAR(255) DEFAULT NULL,
    female2 VARCHAR(255) DEFAULT NULL,
    female3 VARCHAR(255) DEFAULT NULL,
    female4 VARCHAR(255) DEFAULT NULL,
    hfl DOUBLE DEFAULT NULL,
    wgt DOUBLE DEFAULT NULL,
    rtag VARCHAR(255) DEFAULT NULL,
    note2 VARCHAR(255) DEFAULT NULL,
    ltag VARCHAR(255) DEFAULT NULL,
    note3 VARCHAR(255) DEFAULT NULL,
    prevrt VARCHAR(255) DEFAULT NULL,
    note4 VARCHAR(255) DEFAULT NULL,
    prevlet VARCHAR(255) DEFAULT NULL,
    note5 VARCHAR(255) DEFAULT NULL,
    nestdir VARCHAR(255) DEFAULT NULL,
    neststk DOUBLE DEFAULT NULL,
    note6 VARCHAR(255) DEFAULT NULL,
    note7 VARCHAR(255) DEFAULT NULL,
    note8 VARCHAR(255) DEFAULT NULL
) ;

LOAD DATA LOCAL INFILE '/home/kate/data/newdat388.csv'
INTO TABLE queries.newdat
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
IGNORE 0 LINES;

/* create newrata table for queries database which contains only the last 5 years of data */
DROP TABLE IF EXISTS queries.newrata;
CREATE TABLE queries.newrata 
SELECT Rodents.* FROM Portal.Rodents
WHERE Rodents.period > ((SELECT Max(Rodents.period) FROM Portal.Rodents) - 60); 
ALTER TABLE queries.newrata ADD PRIMARY KEY (ID);

/* use newrata table to check that all old tags are NOT indicated with an asterisk */
SELECT newdat.period, newdat.plot, newdat.stake, newdat.sp, newdat.sex, newdat.rtag, newdat.note2, newdat.ltag, newdat.note3
FROM queries.newdat LEFT JOIN queries.newrata ON newdat.rtag = newrata.rtag
WHERE newrata.rtag > 0;

/* use newrata table to check that all new RIGHT tags are indicated with an asterisk */
SELECT newdat.period, newdat.plot, newdat.stake, newdat.sp, newdat.sex, newdat.rtag, newdat.note2, newdat.ltag, newdat.note3
FROM queries.newdat LEFT JOIN queries.newrata USING (rtag)
WHERE newrata.rtag IS NULL AND newdat.rtag <> '';

/* use newrata table to check that all new LEFT tags are indicated with an asterisk */
SELECT newdat.period, newdat.plot, newdat.stake, newdat.sp, newdat.sex, newdat.rtag, newdat.note2, newdat.ltag, newdat.note3
FROM queries.newdat LEFT JOIN queries.newrata USING (ltag)
WHERE newrata.ltag IS NULL AND newdat.ltag <> '';

/* if there is an inconsistency, search newrata for matching tag or a possible typo in the
    tag using a subset of the tag number. MYSQL Workbench has a search box that can 
    be used to try different parts of the tag number, once you run this next query.*/
SELECT * FROM  queries.newrata;

/* use newrata table to check for consistency in species and sex for each tagged individual */
SELECT newrata.period, newrata.plot, newdat.plot, newrata.sp, newdat.sp AS new_sp, newrata.sex, 
newdat.sex AS new_sex, newrata.rtag
FROM queries.newrata INNER JOIN queries.newdat USING (rtag)
WHERE ((newrata.sp<>newdat.sp) AND (newrata.rtag=newdat.rtag)) OR ((newrata.sex <> newdat.sex));

/*Query Results
-	the output will include IDs for which the new data have disparate info regarding species of sex 
-	M vs. F inconsistencies can only be resolved if the animal has been recorded in a reproductive state at some point
-	if the new data are accurate, be sure to make the corrections in both the newrata and Rodents (superb) tables
-	after making all corrections, rerun query to ensure accuracy */

/* Add ID column to clean newdat that starts with the next integer */
ALTER TABLE queries.newdat
ADD ID2 INT AUTO_INCREMENT PRIMARY KEY FIRST;
ALTER TABLE queries.newdat ADD ID INT FIRST; 
UPDATE queries.newdat SET ID = ID2 + (SELECT MAX(Rodents.ID) FROM Portal.Rodents);
ALTER TABLE queries.newdat DROP newdat.ID2;

/* Finally, append clean data to Rodents table */
INSERT INTO Portal.Rodents
SELECT newdat.* FROM queries.newdat;