#/* add new data file to queries database */
import xlrd
import numpy as np
import csv
import MySQLdb as dbapi


# Functions:
def get_data(filename,i):
    wb = xlrd.open_workbook(filename)

    sh = wb.sheet_by_index(i)
    newdat = []
    for row in range(sh.nrows):
        newdat.append(sh.row_values(row))
    return newdat  
    
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
    
def last_five_years():
    '''the newrat table contains the last 5 years of rodent data. This data is used
    to check for tag, species and sex errors in the new data. If it exists, this
    function drops the old 'newrat' table in the database and creates a new one to
    use for the queries. In additon, this function returns the new table as a list of
    lists for use in python'''
    cur.execute("DROP TABLE IF EXISTS queries.newrat") 
    cur.execute("""CREATE TABLE queries.newrat 
    SELECT Rodents.* 
    FROM Portal.Rodents 
    WHERE Rodents.period > ((SELECT Max(Rodents.period) FROM Portal.Rodents) - 60) 
    ALTER TABLE queries.newrat ADD PRIMARY KEY (ID)""")
    newrat = cur.fetchall()
    return newrat
    
def find_oldtag_problem():
    '''compares the new data with the recent data to try to identify tags that no
    recaptured individuals have been marked with an asterisk as new. A problem occurs
    when an existing individual HAS an asterisk. These individuals are returned one 
    at a time to the user.'''
    cur.execute("""SELECT new.period, new.plot, new.stake, new.species, 
    new.sex, new.rtag, new.note2, new.ltag, new.note3
    FROM queries.newdata new 
    LEFT JOIN queries.newrat ON new.rtag = newrat.tag
    WHERE newrat.tag > 0""")
    oldtags_no_asterisk = cur.fetchone()
    while oldtags_no_asterisk:
        error = 'old tag error: '
        problem_solve(oldtags_no_asterisk, error)
    print 'Your recaptured tag problems have been addressed.' #print if problems were found
    print 'There were no problems with recaptured tags' # print if no problems were found
    
def find_newtag_problem(ear):
    '''Compares new data to the recent data to find tags that look like they are new and
    makes sure they are indicated with an asterisk. Problems occur when there IS NOT an 
    asterisk next to a new tag. Check to see if this is a recording error on the asterisk, or
    if the tag is a recapture but may have been written down incorrectly.'''
    if ear == 'right':
        which_ear_index = 18 #FIXME
        cur.execute("""SELECT new.period, new.plot, new.stake, new.species, new.sex, 
        new.rtag, new.note2, new.ltag, new.note3
        FROM queries.newdata new 
        LEFT JOIN queries.newrat USING (rtag)
        WHERE newrat.rtag IS NULL AND new.rtag <> ''""")
        error = 'rtags_asterisk: '
    elif ear == 'left':
        which_ear_index = 20 #FIXME, check index numbers
        cur.execute("""SELECT new.period, new.plot, new.stake, new.species, new.sex, 
        new.rtag, new.note2, new.ltag, new.note3
        FROM queries.newdata new 
        LEFT JOIN queries.newrat USING (ltag)
        WHERE newrat.ltag IS NULL AND new.ltag <> ''""")
        error = 'ltags_asterisk: '
    new_tags_asterisk = cur.fetchone()      
    while new_tags_asterisk:
        problem_solve(error, new_tags_asterisk, which_ear_index)
    print 'Your RIGHT/PIT tag errors were addressed'
    print 'There were no RIGHT/PIT tag errors to address. Good work!'
    
def find_changed_tags():
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
        error = 'A tag has changed: '
        problem_solve (changed_tags, error)
        # find old tag in the database using the tag that has remained consistent. Change the old tag
        # to the new one. Update the database to push old tag into prev tag.
        # record change in the ErrorLog
    print 'There were no changed tags to address'
        
def find_species_sex_problems():
    '''Finds recaptured individuals whose species and/or sex is not consistent with
    records in the database. Attempts to resolve these issues. Majority wins, or if
    individual was captured when reproductive. Otherwise, issue remains unresolved.'''
    cur.execute("""SELECT newrat.period, newrat.plot, newdata.plot, newrat.species, 
    newdata.species AS new_sp, newrat.sex, newdata.sex AS new_sex, newrat.rtag
    FROM queries.newrat 
    INNER JOIN queries.newdata USING (rtag)
    WHERE ((newrat.species<>newdata.species) AND (newrat.rtag=newdata.rtag)) 
    OR ((newrat.sex <> newdata.sex))""")
    spp_sex_issues = cur.fetchone()
    while spp_sex_issues:
        error = 'An error in species or sex has been detected: '
        problem_solve(spp_sex_issues, error)
    print 'There were no inconsistencies detected in species or sex of recaptured individuals'

def probelm_solve(data_line, error_message, which_ear_index):
    '''Hopefully, a universal function which will intake a line of data and a specific 
    error message and be able to solve that problem. After solving (or not), it will go to
    another function which will update the appropriate tables and lists and a second function
    to record the error and the fix in a notes table.'''
    while data_line:
        print (error_message, data_line)
        if error_message == 'rtags_asterisk' or 'ltags_asterisk':
            similar_tags = find_similar(data_line[which_ear_index], which_ear_index, newrat)
            print similar_tags
        y,n = ('y','n')        
        solution = input('Can you address this problem (y/n)? ')
        if solution == 'y': # is this going to cause an 'aliasing' problem?
            new_info = input('Please enter the new data line, starting with "species", separated by commas: ')
            record_ID = ('Please enter the dy, mo, yr, period, plot, and stake of the data you wish to change, separated by commas: ') #contains the date, period, plot and stake
            newdata,database = ('newdata','database')
            location_fix = input('Where would you like to address this problem (newdata/database)? ')
            if location_fix == 'newdata':
                update_table(newdata, field, new_info, record)
                update_newdata(newdata, dataline, field, new_info)
                print ("Don't forget to record your change on the hard copy of the datasheet, too!")
            elif location_fix == 'database':
                update_table(newrat, field, new_info, record)
                update_table(portal.Rodents, field, new_info, record) 
            record_problem(error_message, 'y', record, new_info, location_fix) #FIX ME, SEE BELOW
        else:
            record_problem(error_message, 'n', data_line, None, None)
    
def update_newdata(newdata, dataline, field, new_info):
    '''find rodent information in the newdata list of lists and update it where 
    data was flagged as having a problem'''
    for line in newdata:
        if line == dataline:
            line[field] = new_info
            break
    
def update_table(table, field, new_info, r): # FIXME
    '''When a problem is found, update the tables newrat and database with the solution'''
    sql = cur.execute("""UPDATE table SET species = %s, sex = %s, age = %s, reprod = %s, testes = %s,
    vagina = %s, pregnant = %s, nipples = %s, lactation = %s, hfl = %d, wgt = %d, rtag = %s, 
    note2 = %s, ltag = %s, note3 = %s, note4 = %s, note5=%s WHERE mo = r[1], dy = r[0], 
    yr = r[2], period = r[3], plot = r[4], stake = r[5]""" %(new_info))
    print sql
    con.commit()
                
def record_problem(errorType, solution, record, new_info, where): #FIXME, NEED A BETTER SYSTEM
    '''When a problem is flagged, this records the error raised, if a solution was 
    reached (Y/N), and what the old data was, what it was changed to, and where it 
    was changed (datasheet/database)'''
    cur.execute("""INSERT INTO ErrorLog SET date = date, error = errorType, solution = solution,
    oldData = record, newData = new_info, where = database""")
    con.commit()
    
def find_similar(ear_tag, which_ear_index, newrat): #FIXME!!    
    '''identify tags which are similar at 4/6 locations and where tags may be similar if a common typo
    has been made such as 8 for B, 0 for D or vice-versa.'''
    tags = dict(map(lambda i: (i,1),data[which_ear_index])).keys() # returns a set of pre-existing tags
    tag_list = []
    for tag in tags:
        match, sim_tag = is_similar(ear_tag, tag)
        if match >= 4:
            tag_list.append(sim_tag) 
    rep_tags = find_similar_replacement(ear_tag, tag)
    tag_list.extend(rep_tags)
    # What I'm missing is a way to identify and return these lines--need to know if they are the same species, plots, sex, or other information that will help me determine if its truly a similar tag, or actually the same tag disguised as a typo!
    return tag_list

def is_similar(ear_tag, tag):
    '''This function takes the existing ear_tag and compares it to the set of already existing tags 
    from the past five years and compares them place by place. Returns the number of locations at which
    there is a match between the new tag and the existing tag.'''
    match = 0
    index = range(0,6)
    for i in index:
        if ear_tag[i] == tag[i]:
            match += 1
    return match, tag
        
def find_similar_replacement(ear_tag, tag): #FIX ME
    '''This function is supposed to look for similar tags where numbers/letters that are often confused
    may have been written down wrong. 8 and B, 0 and D.'''
    if str.count(ear_tag, 'B') > 0:
        rep_tag = str.replace(ear_tag, 'B', '8')
        match, tag = is_similar(rep_tag, tag)
    elif str.count(ear_tag, '8') > 0:
        rep_tag = str.replace(ear_tag, '8', 'B')
        match, tag = is_similar(rep_tag, tag)
    elif str.count(ear_tag, 'D') > 0:
        rep_tag = str.replace(ear_tag, 'D', '0')
        match, tag = is_similar(rep_tag, tag)
    elif str.count(ear_tag, '0') > 0:
        rep_tag = str.replace(ear_tag, '0', 'D')
        match, tag = is_similar(rep_tag, tag)
    return tag
        
        

if __name__ == '__main__':        
    
    # PART ONE: DATA ENTRY ERROR CHECKING 
    ''' Data should be in a csv file titles newdatXXXa.csv where XXX should be 
    filled in with the period code and a refers to the initials of the person who
    entered the data'''     
    print 'Before importing data, make sure both sheets in the excel file have the same number of rows and are in the same order!'
    filename = input('please enter location of data: ')
    
    newdat1 = get_data(filename, 0)
    newdat2 = get_data(filename, 1)

    # compare double-entered data and write a new datafile to use
    rows = range(len(newdat1))
    newdata = []
    for row in rows:
        next_line = compare_lines(newdat1[row], newdat2[row])
        newdata.append(next_line)

    numrows = len(newdata)
    
    # Write compared_data to a csv file to be saved in the Portal folders.
    new_filename = input('What do you want to call the new file? (Example, "NEWDAT398.csv"): ')
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

    # Create newrata table and python list which contains only the last 5 years of data
    newrat = last_five_years()
    
    # Use newrata table to check that all old tags are NOT indicated with an asterisk
    # Problem occurs when an already existing tag HAS an asterisk
    find_oldtag_problem()

    #Problem occurs when a new tag DOES NOT have an asterisk
    find_newtag_problem('right')
    find_newtag_problem('left')
        
    # Flag any cases where there is an entry for BOTH rtag and ltag AND where they differ in '*'
    find_changed_tags()

    # Use newrata table to check for consistency in species and sex for each tagged individual 
    find_species_sex_problems()
                
    # PART THREE: Finished error checking, append to database
    # Add ID column to clean newdat that starts with the next integer 
    # This step shouldn't be necessary if the Rodents.ID column is properly formatted as AUTO_INCREMENT
    cur.execute("ALTER TABLE queries.newdata ADD ID2 INT AUTO_INCREMENT PRIMARY KEY FIRST")
    cur.execute("ALTER TABLE queries.newdata ADD ID INT FIRST")
    cur.execute("UPDATE queries.newdata SET ID = ID2 + (SELECT MAX(Rodents.ID) FROM Portal.Rodents)")
    cur.execute("ALTER TABLE queries.newdata DROP newdata.ID2")

    # Finally, append clean data to Rodents table 
    cur.execute("INSERT INTO Portal.Rodents SELECT newdata.* FROM queries.newdata")
    con.commit()

    print 'Finished checking for problems. You have appended ', numrows, ' to the Rodents on Serenity.'
    