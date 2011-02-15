USE Portal;

ALTER TABLE `Portal`.`Rodents` CHANGE COLUMN `ID` `Record_ID` INT(10) NOT NULL 
AUTO_INCREMENT, DROP PRIMARY KEY, ADD PRIMARY KEY (`Record_ID`);

SELECT 'Record_ID','mo','dy','yr','period','plot','note1','stake','species','sex',
'age','reprod','testes','vagina','pregnant','nipples',
'lactation','hfl','wgt','tag','note2','ltag','note3',
'prevrt','prevlet','nestdir','neststk','note4','note5'
UNION 
SELECT * FROM Rodents 
WHERE ((Rodents.yr) < 2003)
INTO OUTFILE '/tmp/portal_rodents_19772002_header.csv'
FIELDS TERMINATED BY ',' ENCLOSED BY '"';