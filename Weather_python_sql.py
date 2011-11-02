# Weather Data for Portal
import numpy as np
import datetime
import MySQLdb as dbapi

"""What needs to happen when entering WEATHER data:
Convert Julian day into two columns, month and day
make sure it converts correctly depending on if it is a leap year or not 
Save a copy of the edited data file as 'weathperiodcode'.csv
add data to the 'Hourly' database table
run a query to add daily data to 'Daily' table in database 
run a query to add monthly data to 'Monthly table in database"""

def is_battery_reading(data_line):
    '''input a line of data from the weather.dat file. if ppt == 12.7, tempAir == 2196 and
    hour == 2400, then it is a battery reading. Return (Y)es or (N)o.'''
    if data_line['ppt']==12.7 and data_line['tempAir'] == 2196 and data_line['hour']==2400:
        return ('Y')
    else:
        return ('N')
    
def data_to_list(data_line):
    '''input a line of data from the weather.dat file that IS NOT a battery reading.
    converts the data line to a list that can be manipulated.'''
    datalist = []
    for l in data_line:
        datalist.append(l)
    return datalist
    
def add_tempSoil(data_line):
    '''adds a NULL item to the data line. This will go in the tempSoil column later.'''
    return data_line.append('NULL')

def add_date(data_line): #FIX ME
    '''Converts Julian Day to two items: month and day. Appends these to the data_line.'''
    convert Julian day to month and day
    data_line.append(month)
    data_line.append(day)
    return data_line
    
def rearrange_cols(data_line):
    '''where order of data_line is: 
    array, year, julianDay, hour, ppt, tempAir, relHumid, tempSoil, month, day
    and we want:
    Year, Month, Day, Hour, TempAir, RelHumid, TempSoil, Precipitation(mm)
    array and julianDay are left out because they are not necessary for the weather dataset.'''
    myorder = [1, 8, 9, 3, 5, 6, 7, 4] 
    data_line = [data_line[i] for i in myorder]
    return data_line
    
def prepare_data(data_line):
    '''inputs a line of data from weather.dat and determines if it's a battery reading.
    If not, then  the dataline is manipulated and appended to fit the Portal weather database.'''
    is_battery = is_battery_reading(data_line)
    if is_battery == 'N':
        data_line = data_to_list(data_line)
        data_line = add_tempSoil(data_line)
        data_line = add_date(data_line)
        data_line = rearrange_cols(data_line)
        return data_line
    else:
        return ['battery_reading']
    
def compile_weather_data(data):
    '''input weather.dat file. 
    Columns are: array | year | Julian day | hour | ppt | tempAir | RelHumid
    Reads the data into the prepare_data function line by line, determining if 
    the data is a battery reading, in which case it is not added to the new
    list of weather data to be appended to the database.'''
    weather_data = []
    for line in weather:
        wx = prepare_data(line)
        if len(wx) > 1: 
            weather_data.append(wx)
            
def save_weather_file(data, filename):
    '''saves weather as a csv file to a shared location. input the new datafile
    and a string that has the name and extension of the file to which is should be 
    saved.'''
    weatherFile = open(filename,'wb') 
    w = csv.writer(weatherFile,delimiter=',')
    w.writerows(data)
    weatherFile.close()
    

#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% DOES STUFF
#location of new weather file
filename = 'F:\AdvProj_Portal\Met132.dat'

# import into python
weather = np.genfromtxt(filename, delimiter = ',', names = ['array','year','julianDay', 'hour', 'ppt', 'tempAir', 'relHumid'])

# get data to be appended to database
weather_to_add = compile_weather_data(weather)

# save a copy of the finished file to a shared location
save_weather_file(weather_to_add, 'weathperiodcode.csv')

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
