#/* add new data file to queries database */
import numpy as np
import csv
import MySQLdb as dbapi

database = 'F:/adv_prog/Assn4/portal_mammals_fake.sqlite'

con = dbapi.connect(database)
cur = con.cursor()

filename1 = 'loc/newdatXXXa.csv'
filename2 = 'loc/newdatXXXb.csv'

# PART ONE: DATA ENTRY ERROR CHECKING !!FIXME
# before importing, make sure that both files have the same number of rows and are input in the same order
# compare double-entered data and write a new datafile to use
newdat1 = genfromtxt(filename1,...)
newdat2 = genfromtxt(filename2,...)

rows = range(len(newdat1))
newdata = []
for row in rows:
    if newdat1[row] == newdat2[row]:
        newdata.append(newdat1[row])
    if newdat1[row] != newdat2[row]:
        opt1 = newdat1[row]
        opt2 = newdat2[row]
        print 'opt1 = ', newdat1[row], ', opt2 = ', newdat2[row], ', or enter a new data line'
        use_data = input('please enter correct data: ')
        newdata.append(use_data)

# Write compared_data to a csv file to be saved in the Portal folders.
# Use compared data to query the database before appending

cur.execute("DROP TABLE IF EXISTS queries.newdata")
cur.execute("""CREATE TABLE queries.newdata
( 
    mo DOUBLE,
    dy DOUBLE,
    yr DOUBLE, 
    period DOUBLE, 
    plot TINYINT(4), 
    note1 VARCHAR(255) DEFAULT NULL, 
    stake DOUBLE DEFAULT NULL, 
    species VARCHAR(255) DEFAULT NULL,
    sex VARCHAR(255) DEFAULT NULL,
    age VARCHAR(255) DEFAULT NULL,
    reprod VARCHAR(255) DEFAULT NULL,
    testes VARCHAR(255) DEFAULT NULL,
    vagina VARCHAR(255) DEFAULT NULL,
    pregnant VARCHAR(255) DEFAULT NULL,
    nipples VARCHAR(255) DEFAULT NULL,
    lactation VARCHAR(255) DEFAULT NULL,
    hfl DOUBLE DEFAULT NULL,
    wgt DOUBLE DEFAULT NULL,
    tag VARCHAR(255) DEFAULT NULL,
    note2 VARCHAR(255) DEFAULT NULL,
    ltag VARCHAR(255) DEFAULT NULL,
    note3 VARCHAR(255) DEFAULT NULL,
    prevrt VARCHAR(255) DEFAULT NULL,
    prevlet VARCHAR(255) DEFAULT NULL,
    note5 VARCHAR(255) DEFAULT NULL,
    nestdir VARCHAR(255) DEFAULT NULL,
    neststk DOUBLE DEFAULT NULL,
    note4 VARCHAR(255) DEFAULT NULL,
    note5 VARCHAR(255) DEFAULT NULL
)""")

cur.execute("""LOAD DATA LOCAL INFILE filename
INTO TABLE queries.newdata
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
IGNORE 0 LINES""")

# Create newrata table for queries which contains only the last 5 year of data
cur.execute("""DROP TABLE IF EXISTS queries.newrat 
CREATE TABLE queries.newrat 
SELECT Rodents.* 
FROM Portal.Rodents 
WHERE Rodents.period > ((SELECT Max(Rodents.period) FROM Portal.Rodents) - 60) 
ALTER TABLE queries.newrat ADD PRIMARY KEY (ID)""")

# Use newrata table to check that all old tags are NOT indicated with an asterisk
cur.execute("""SELECT new.period, new.plot, new.stake, new.species, 
new.sex, new.rtag, new.note2, new.ltag, new.note3
FROM queries.newdata new 
LEFT JOIN queries.newrat ON new.rtag = newrat.rtag
WHERE newrat.rtag > 0""")

oldtags_no_asterisk = cur.fetchall()
print (oldtags_no_asterisk)

# Use newrata table to check that all new RIGHT tags are indicated with an asterisk
cur.execute("""SELECT new.period, new.plot, new.stake, new.species, new.sex, 
new.rtag, new.note2, new.ltag, new.note3
FROM queries.newdata new 
LEFT JOIN queries.newrat USING (rtag)
WHERE newrat.rtag IS NULL AND new.rtag <> ''""")

new_rtags_asterisk = cur.fetchall()
print (new_rtags_asterisk)

# Use newrata table to check that all new LEFT tags are indicated with an asterisk 
cur.execute("""SELECT new.period, new.plot, new.stake, new.species, new.sex, new.rtag, new.note2, 
new.ltag, new.note3
FROM queries.newdata new 
LEFT JOIN queries.newrat USING (ltag)
WHERE newrat.ltag IS NULL AND new.ltag <> ''""")

new_ltags_asterisk = cur.fetchall()
print (new_ltags_asterisk)

#/* if there is an inconsistency, search newrata for matching tag or a possible typo in the
#    tag using a subset of the tag number. MYSQL Workbench has a search box that can 
#    be used to try different parts of the tag number, once you run this next query.
#SELECT * FROM  queries.newrata;
# FIX ANY ERRORS IN THE DATABASE OR IN NEWDAT
con.commit()

# Use newrata table to check for consistency in species and sex for each tagged individual 
cur.execute("""SELECT newrat.period, newrat.plot, newdata.plot, newrat.species, 
newdata.species AS new_sp, newrat.sex, newdata.sex AS new_sex, newrat.rtag
FROM queries.newrat 
INNER JOIN queries.newdata USING (rtag)
WHERE ((newrat.species<>newdata.species) AND (newrat.rtag=newdata.rtag)) 
OR ((newrat.sex <> newdata.sex))""")

spp_sex_issues = cur.fetchall()
print(spp_sex_issues)

# Query Results
# - the output will include IDs for which the new data have disparate info regarding species of sex 
# - M vs. F inconsistencies can only be resolved if the animal has been recorded in a reproductive state at some point
# - if the new data are accurate, be sure to make the corrections in both the newrata and Rodents tables
# - after making all corrections, rerun query to ensure accuracy
# FIX ERRORS IN DATABASE OR IN NEWDAT
con.commit()

# Add ID column to clean newdat that starts with the next integer 
# This step shouldn't be necessary if the Rodents.ID column is properly formatted as AUTO_INCREMENT */
cur.execute("ALTER TABLE queries.newdata ADD ID2 INT AUTO_INCREMENT PRIMARY KEY FIRST")
cur.execute("ALTER TABLE queries.newdata ADD ID INT FIRST")
cur.execute("UPDATE queries.newdata SET ID = ID2 + (SELECT MAX(Rodents.ID) FROM Portal.Rodents)")
cur.execute("ALTER TABLE queries.newdata DROP newdata.ID2")

# Finally, append clean data to Rodents table */
cur.execute("INSERT INTO Portal.Rodents SELECT newdata.* FROM queries.newdata")
con.commit()