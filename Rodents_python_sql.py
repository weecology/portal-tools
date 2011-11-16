#/* add new data file to queries database */
import xlrd
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
    while line1:
        if line1 == line2:
            return line1
        elif line1 != line2:
            opt1 = line1
            opt2 = line2
            print opt1
            print opt2
            print 'Do you want opt1, opt2, or a new line of data? '
            use_data = input('please enter correct data: ')
            return use_data

def upload_newdata(newdata):
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
    
def update_newdata():
    '''find rodent information in the newdata list of lists and update it where 
    data was flagged as having a problem'''
    
def update_table(table, field, new_info): # FIXME
    '''When a problem is found, update the tables newrat and database with the solution'''
    cur.execute("""UPDATE table SET field = new_info WHERE mo = 'month', dy = 'day', 
    yr = 'year', period = 'period', plot = 'plot', """)
    con.commit()
                
def record_problem(errorType, solution, oldData, newData, where): #FIXME
    '''When a problem is flagged, this records the error raised, if a solution was 
    reached (Y/N), and what the old data was, what it was changed to, and where it 
    was changed (datasheet/database)'''
    cur.execute("""UPDATE ErrorLog SET date = date, error = errorType, solution = solution,
    oldData = oldData, newData = newData, where = database""")
    con.commit()
    
def find_similar(new_tag, which_ear_index): #FIXME!!
    """look for tags in newrat that match rtag in 4/6 places. return a list of those
    tags along with important information: mo, dy, yr, plot, sp, sex, rtag, ltag"""
    cur.execute("""SELECT newrat.period, newrat.plot, newrat.stake, newrat.species,
    newrat.sex, newrat.tag, newrat.ltag, FROM queries.newrat """)
    newrat = cur.fetchall()
    similar_data = is_similar(newrat, new_tag, which_ear)
    return similar_data
    
def is_similar(data, new_tag, which_ear_index): # FIXME!!!
    '''identify tags which are similar at 4/6 locations'''
    tags = dict(map(lambda i: (i,1),data[which_ear_index])).keys()
    #tag_data = find tags similar at 4/6 locations
    #tag_data = find tags where 8/B or 0/D have been substituted
    return tag_data


# Execute commands if running directly:    
if __name__ == '__main__':        
    
    # PART ONE: DATA ENTRY ERROR CHECKING 
    ''' Data should be in a csv file titles newdatXXXa.csv where XXX should be 
    filled in with the period code and a refers to the initials of the person who
    entered the data'''     
    print 'Before importing data, make sure both sheets in the excel file have the same number of rows and are in the same order!'
    filename = input('please enter location of data: ')
    
    wb = xlrd.open_workbook(filename)

    sh = wb.sheet_by_index(0)
    newdat1 = []
    for row in range(sh.nrows):
        newdat1.append(sh.row_values(row))
    
    sh = wb.sheet_by_index(1)
    newdat2 = []
    for row in range(sh.nrows):
        newdat2.append(sh.row_values(row))
    

    # compare double-entered data and write a new datafile to use
    rows = range(len(newdat1))
    newdata = []
    for row in rows:
        next_line = compare_lines(newdat1[row], newdat2[row])
        newdata.append(next_line)

    # Write compared_data to a csv file to be saved in the Portal folders.
    new_filename = input('What do you want to call the new file?: ')
    save_data(newdata, new_filename)

    # PART TWO: Connect to the database on the server
    # Use new data to query the database for potential problems before appending
    user = input('What is your username?: ')
    yourpassword = input('Please enter your password: ')
        
    con = dbapi.connect(host = 'serenity.bluezone.usu.edu',
                        port = 1995,
                        user = user,
                        passwd = yourpassword)

    cur = con.cursor()
        
    upload_newdata(newdata)

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
        solution = input('Can you address this problem (y/n)? ')
        if solution == 'y':
        #find data in newdata table and in the newdata python file and update it
            update_table(newdata, field, new_info) 
            update_newdata(newdata, field, new_info)
            record_problem('old tag error', 'y', olddata, newdata, 'new data')
            print ("Don't forget to record your change on the hard copy of the datasheet, too!")
        else:
            record_problem('old tag error', 'n', None, None, None)
        

    # Use newrata table to check that all new RIGHT tags are indicated with an asterisk
    #/* if there is an inconsistency, search newrata for matching tag or a possible typo in the
    #    tag using a subset of the tag number. MYSQL Workbench has a search box that can 
    #    be used to try different parts of the tag number, once you run this next query.
    #SELECT * FROM  queries.newrata;
    # FIX ANY ERRORS IN THE DATABASE OR IN NEWDAT
    cur.execute("""SELECT new.period, new.plot, new.stake, new.species, new.sex, 
    new.rtag, new.note2, new.ltag, new.note3
    FROM queries.newdata new 
    LEFT JOIN queries.newrat USING (rtag)
    WHERE newrat.rtag IS NULL AND new.rtag <> ''""")

    new_rtags_asterisk = cur.fetchone()      # FIXME
    while new_rtags_asterisk:
        print ('rtag error: ', new_rtags_asterisk)
        #find similar tags in newrat, return a list of those tags (4/6 similar?)    
        similar_tags = find_similar(new_rtag, 5)
        print similar_tags
        solution = input('Can you address this problem (y/n)? ')
        if solution == 'y':
            update_table(newdata, field, new_info)
            update_newdata(newdata, field, new_info)
            record_problem('rtag asterisk error', 'y', olddata, newdata, 'new data')
            print("Don't forget to record your change on the hard copy of the datasheet, too!")
        else:
            record_problem('rtag asterisk error', 'n', None, None, None)
        
     # Use newrata table to check that all new LEFT tags are indicated with an asterisk 
     #/* if there is an inconsistency, search newrata for matching tag or a possible typo in the
     #    tag using a subset of the tag number. MYSQL Workbench has a search box that can 
     #    be used to try different parts of the tag number, once you run this next query.
     #SELECT * FROM  queries.newrata;
     # FIX ANY ERRORS IN THE DATABASE OR IN NEWDAT
    cur.execute("""SELECT new.period, new.plot, new.stake, new.species, new.sex, new.rtag, new.note2, 
    new.ltag, new.note3
    FROM queries.newdata new 
    LEFT JOIN queries.newrat USING (ltag)
    WHERE newrat.ltag IS NULL AND new.ltag <> ''""")

    new_ltags_asterisk = cur.fetchone()      # FIXME
    while new_ltags_asterisk:
        print ('ltag error: ', new_ltags_asterisk)
        #find similar tags in newrat, return a list of those tags (4/6 similar?)    
        similar_tags = find_similar(new_ltag, 7)
        print similar_tags
        solution = input('Can you address this problem (y/n)? ')
        if solution == 'y':
            update_table(newdata, field, new_info)
            update_newdata(newdata, field, new_info)
            record_problem('ltag asterisk error', 'y', olddata, newdata, 'new data')
            print("Don't forget to record your change on the hard copy of the datasheet, too!")
        else:
            record_problem('ltag asterisk error', 'n', None, None, None)
        
    # Flag any cases where there is an entry for BOTH rtag and ltag AND where they differ 
    # in the presence of an asterisk. Where an individual IS a recapture AND has a NEW tag,
    # the old tag must be updated in the database and in newrat. The old tag can be pushed
    # over to prevrt or prevlt. A record should be made in the ErrorLog.
    cur.execute("""SELECT new.period, new.plot, new.stake, new.species, new.sex, new.rtag, new.note2, 
    new.ltag, new.note3
    FROM queries.newdata new 
    WHERE new.ltag IS NOT NULL AND new.note2 != new.note3""")
    
    changed_tags = cur.fetchone()
    while changed_tags:
        print ('A tag has changed: ', changed_tags)
        # find old tag in the database using the tag that has remained consistent. Change the old tag
        # to the new one. Update the database to push old tag into prev tag.
        # record change in the ErrorLog
        
    # Use newrata table to check for consistency in species and sex for each tagged individual 
    cur.execute("""SELECT newrat.period, newrat.plot, newdata.plot, newrat.species, 
    newdata.species AS new_sp, newrat.sex, newdata.sex AS new_sex, newrat.rtag
    FROM queries.newrat 
    INNER JOIN queries.newdata USING (rtag)
    WHERE ((newrat.species<>newdata.species) AND (newrat.rtag=newdata.rtag)) 
    OR ((newrat.sex <> newdata.sex))""")

    spp_sex_issues = cur.fetchone()
    while spp_sex_issues:
        print('An error in species or sex has been detected: ', spp_sex_issues)
        err = input('Is this a species or a sex problem (spp/sex)? ')
        # find all other records of the individual, return data
        solution = input('Can you address this problem (y/n)? ')
        if solution == 'y':
            where = input('Where will you fix the problem (newdata/Database)? ')
            if where == 'newdata':
                update_table(newdata, field, new_info)
                update_newdata(newdata, field, new_info)
                record_problem(err, 'y', olddata, newdata, where)
                print("Don't forget to record your change on the hard copy of the datasheet, too!")
            elif where == 'database':
                update_table(database, field, new_info)
                update_table(newrat, field, new_info)
                record_problem(err, 'y', olddata, newdata, where)
            else:
                record_problem(err, 'n', None, None, None)
                
    # FINISHED ERROR CHECKING, append to database
    # Add ID column to clean newdat that starts with the next integer 
    # This step shouldn't be necessary if the Rodents.ID column is properly formatted as AUTO_INCREMENT
    cur.execute("ALTER TABLE queries.newdata ADD ID2 INT AUTO_INCREMENT PRIMARY KEY FIRST")
    cur.execute("ALTER TABLE queries.newdata ADD ID INT FIRST")
    cur.execute("UPDATE queries.newdata SET ID = ID2 + (SELECT MAX(Rodents.ID) FROM Portal.Rodents)")
    cur.execute("ALTER TABLE queries.newdata DROP newdata.ID2")

    # Finally, append clean data to Rodents table */
    cur.execute("INSERT INTO Portal.Rodents SELECT newdata.* FROM queries.newdata")
    # numrows = NUMBER OF ROWS IN NEWDATA APPENDED TO RODENTS     FIX ME!
    con.commit()

    print 'Finished checking for problems. You have appended ', numrows, ' to the Rodents on Serenity.'
    