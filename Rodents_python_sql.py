#/* add new data file to queries database */
import numpy as np
import csv
import MySQLdb as dbapi

# Functions:
def save_data(data, new_filename):
    '''input the list of data that should be saved and a string with the new filename
    for example, newdat404.csv'''
    data_file = open(new_filename,'wb') 
    w = csv.writer(data_file,delimiter=',')
    w.writerows(data)
    data_file.close()

def compare_lines(line1, line2):
    '''input two lines of data that should be th same and look for differences.
    if the lines are the same, return line 1. If they are different, give the user
    the choice of which line is correct, line1, line2, or a new line that the user inputs'''
    if line1 == line2:
        return line1
    elif line1 != line2:
        opt1 = line1
        opt2 = line2
        print 'opt1 = ', line1, ', opt2 = ', line2, ', or enter a new data line'
        use_data = input('please enter correct data: ')
        return use_data
    
def update_table(table, field, new_data): # FIXME
    '''When a problem is found, update the tables newrat and database with the solution'''
    cur.execute("""UPDATE table SET field = new_data WHERE mo = 'month', dy = 'day', 
    yr = 'year', period = 'period', plot = 'plot', """)
    con.commit()
                
def record_problem(errorType, solution, oldData, newData, where): #FIXME
    '''When a problem is flagged, this records the error raised, if a solution was 
    reached (Y/N), and what the old data was, what it was changed to, and where it 
    was changed (datasheet/database)'''
    cur.execute("""UPDATE ErrorLog SET date = date, error = errorType, solution = solution,
    oldData = oldData, newData = newData, where = database""")
    con.commit()
    
    
# PART ONE: DATA ENTRY ERROR CHECKING 
# before importing, make sure that both files have the same number of rows and are input in the same order
    
''' Data should be in a csv file titles newdatXXXa.csv where XXX should be 
filled in with the period code and a refers to the initials of the person who
entered the data'''     
print ('Before importing data, make sure both files have the same number of rows and are in the same order!')
filename1 = input('please enter location of data entered by recorder #1: ')
filename2 = input('please enter location of data entered by recorder #2: ')

newdat1 = np.genfromtxt(filename1, dtype = None, delimiter = ',', skip_header = 1)
newdat2 = np.genfromtxt(filename2, dtype = None, delimiter = ',', skip_header = 1)

# compare double-entered data and write a new datafile to use
rows = range(len(newdat1))
newdata = []
for row in rows:
    next_line = compare_lines(newdat1[row], newdat2[row])
    newdata.append(next_line)

# Write compared_data to a csv file to be saved in the Portal folders.
new_filename = input('What do you want to call the new file?: ')
save_data(newdata, new_filename)

#practice data
newdata = [[8,6,2011,396,1,None,45,'DM','F',None,None,None,'S',None,None,None,36,31,'000178','*','000179','*',None,None,None,None,None,None],
           [8,6,2011,396,2,None,47,'PP','F','Z',None,None,None,None,None,None,22,20,'0B0D15',None,None,None,None,None,None,None,None,None,None]]

# PART TWO: Connect to the database on the server
# Use new data to query the database for potential problems before appending
user = input('What is your username?: ')
yourpassword = input('Please enter your password: ')

con = dbapi.connect(host = 'serenity.bluezone.usu.edu',
              port = 1995,
              user = user,
              passwd = yourpassword)

cur = con.cursor()

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
    nestdir VARCHAR(255) DEFAULT NULL,
    neststk DOUBLE DEFAULT NULL,
    note4 VARCHAR(255) DEFAULT NULL,
    note5 VARCHAR(255) DEFAULT NULL
)""")

cur.execute("""LOAD DATA LOCAL INFILE filename
INTO TABLE queries.newdata
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
IGNORE 0 LINES""")

# Create newrata table for queries which contains only the last 5 years of data
cur.execute("""DROP TABLE IF EXISTS queries.newrat""") 
cur.execute("""CREATE TABLE queries.newrat 
SELECT Rodents.* 
FROM Portal.Rodents 
WHERE Rodents.period > ((SELECT Max(Rodents.period) FROM Portal.Rodents) - 60) 
ALTER TABLE queries.newrat ADD PRIMARY KEY (ID)""")

# Use newrata table to check that all old tags are NOT indicated with an asterisk
# Problem occurs when an already existing tag HAS an asterisk
cur.execute("""SELECT new.period, new.plot, new.stake, new.species, 
new.sex, new.rtag, new.note2, new.ltag, new.note3
FROM queries.newdata new 
LEFT JOIN queries.newrat ON new.rtag = newrat.tag
WHERE newrat.tag > 0""")

oldtags_no_asterisk = cur.fetchone()   # FIXME
while oldtags_no_asterisk:
    print ('Old tag error: ', oldtags_no_asterisk)
    solution = input('Can you address this problem (y/n)?: ')
    if solution == y:
        where = input('Where will you fix this problem (newdata/database)?: ')
        if where == newdata:
            #find data in newdata and update it
            record_problem('old tag error', 'y', olddata, newdata, where)
            print ("Don't forget to record your change on the hard copy of the datasheet, too!")
        elif where == database:
            #find data in Rodents and in newrat and update it
            record_problem('old tag error', 'y', olddata, newdata, where)
    else:
        record_problem('old tag error', 'n', None, None, None)


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