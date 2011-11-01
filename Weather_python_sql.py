# Weather Data for Portal
import numpy as np
import datetime
import MySQLdb as dbapi

"""What needs to happen when entering WEATHER data:
Get data in .DAT file from palm pilot.
Columns are array (not needed) | year | Julian day | hour | ppt | tempAir | RelHumid
Convert Julian day into two columns, month and day
make sure it converts correctly depending on if it is a leap year or not 
remove extra records
remove  records where ppt == 12.7, tempAir == 2196 and hour == 2400 (battery reading)
add blank tempSoil column and rearrange columns to match database
Year | Month | Day | Hour | TempAir | RelHumid | TempSoil | Precipitation(mm)
Save a copy of the edited data file as 'weathperiodcode'.csv
add data to the 'Hourly' database table
run a query to add daily data to 'Daily' table in database 
run a query to add monthly data to 'Monthly table in database"""

#remove records where ppt == 12.7, tempAir == 2196 and hour == 2400 (battery reading)
def is_battery_reading(data_line):
    if data_line['ppt']==12.7 and data_line['tempAir'] == 2196 and data_line['hour']==2400:
        return ('Y')
    else:
        return ('N')
    
def data_to_list(data_line):
    datalist = []
    for l in data_line:
        datalist.append(l)
    return datalist
    
def add_tempSoil(data_line):
    return data_line.append('NULL')

def add_date(data_line): #FIX ME
    convert Julian day to month and day
    data_line.append(month)
    data_line.append(day)
    return data_line
    
def rearrange_cols(data_line):
    '''where order of data_line is: 
    array, year, julianDay, hour, ppt, tempAir, relHumid, tempSoil, month, day
    and we want:
    Year, Month, Day, Hour, TempAir, RelHumid, TempSoil, Precipitation(mm)'''
    myorder = [1, 8, 9, 3, 5, 6, 7, 4] 
    data_line = [data_line[i] for i in myorder]
    return data_line
    
def prepare_data(data_line):
    is_battery = is_battery_reading(data_line)
    if is_battery == 'N':
        data_line = data_to_list(data_line)
        data_line = add_tempSoil(data_line)
        data_line = add_date(data_line)
        data_line = rearrange_cols(data_line)
        return data_line
    else:
        return ['battery_reading']

#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% DOES STUFF
#location of new weather file
filename = 'F:\AdvProj_Portal\Met132.dat'

#import into python
weather = np.genfromtxt(filename, delimiter = ',', names = ['array','year','julianDay', 'hour', 'ppt', 'tempAir', 'relHumid'])

weather_data = []
for line in weather:
    wx = prepare_data(line)
    if len(wx) > 1: # if line was a battery reading, will return a list of length 1
        weather_data.append(wx)

# Write data to a csv file
weath_file = open('weathXXX.csv','wb') 
w = csv.writer(weath_file,delimiter=',')
w.writerows(weather_data)
weath_file.close()

#DATABASE STUFF: open file to append to database
database = 'loc/weather_db.sqlite'

con = dbapi.connect("""host = 'serenity.bluezone.usu.edu',
                    port = 1995,
                    user = sarahsupp,
                    passwd = yourpassword""")

cur = con.cursor()

#locate new weather file     FIX ME!!
weatherFile = 'loc/weathXXX.csv'

cur.execute(USE weather) #or whatever the name of portal weather database is
cur.execute("DROP TABLE IF EXISTS weath")
cur.execute("""CREATE TABLE queries.weath
( Year DOUBLE,
    Month DOUBLE,
    Day DOUBLE, 
    Hour DOUBLE, 
    TempAir FLOAT
    RelHumid FLOAT
    TempSoil FLOAT
    Precipiation(mm) FLOAT
)""")

cur.execute("""LOAD DATA LOCAL INFILE weatherFile
INTO TABLE queries.weath
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
IGNORE 0 LINES""")
con.commit()

#append data to Hourly weather Table
cur.execute("INSERT INTO Portal.Rodents SELECT newdat.* FROM queries.newdat")
con.commit()

# run a query to add daily data to 'Daily' table in database 

# run a query to add monthly data to 'Monthly table in database
