# Weather Data for Portal
import numpy as np
import calendar
from datetime import datetime as dt
from matplotlib.dates import num2date
import MySQLdb as dbapi

"""What needs to happen when entering WEATHER data:
Convert Julian day into two columns, month and day
make sure it converts correctly depending on if it is a leap year or not 
Save a copy of the edited data file as 'weathperiodcode'.csv
add data to the 'Hourly' database table
run a query to add daily data to 'Daily' table in database 
run a query to add monthly data to 'Monthly table in database"""
    
def add_tempSoil(data_line):
    '''adds an empty string to the data line. This will go in the tempSoil column later.'''
    return data_line.append(None)

def jday2caldates(data_line):
    '''takes a year and a julian day (range(0,366)) and returns a 
    calendar month and day. defines a list of months and days for both year types
    on which to index the julian day to calendar day and month.'''
    if calendar.isleap(data_line[1])==True:
        days = sum([range(1,32), range(1,30), range(1,32), range(1,31), range(1,32), range(1,31),range(1,32), range(1,32), range(1,31),range(1,32), range(1,31),range(1,32)],[])
        months = sum([[1]*31,[2]*29,[3]*31,[4]*30,[5]*31,[6]*30,[7]*31,[8]*31,[9]*30,[10]*31,[11]*30,[12]*31],[])
        cal_day = days[int(data_line[2]) - 1]
        cal_month = months[int(data_line[2]) - 1]
        data_line.extend([cal_month, cal_day])
        data_line[0] = int(data_line[0])
        return data_line
    else :
        days = sum([range(1,32), range(1,29), range(1,32), range(1,31), range(1,32), range(1,31),range(1,32), range(1,32), range(1,31),range(1,32), range(1,31),range(1,32)],[])
        months = sum([[1]*31,[2]*28,[3]*31,[4]*30,[5]*31,[6]*30,[7]*31,[8]*31,[9]*30,[10]*31,[11]*30,[12]*31],[])
        cal_day = days[int(data_line[2]) - 1]
        cal_month = months[int(data_line[2]) - 1]
        data_line.extend([cal_month, cal_day])
        data_line[0] = int(data_line[0])
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
    
def prepare_data_row(data_line):
    '''inputs a line of data from weather.dat and determines if it's a battery reading.
    If not, then  the dataline is manipulated and appended to fit the Portal weather database.'''
    add_tempSoil(data_line)
    data_line = jday2caldates(data_line)
    data_line = rearrange_cols(data_line)
    return data_line
    
def compile_weather_data(data):
    '''input weather.dat file. 
    Columns are: array | year | Julian day | hour | ppt | tempAir | RelHumid
    Reads the data into the prepare_data function line by line, determining if 
    the data is a battery reading, in which case it is not added to the new
    list of weather data to be appended to the database.'''
    weather_data = []
    for line in weather:
        wx = prepare_data_row(line)
        weather_data.append(wx)
    return weather_data
            
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
filename = input('Where is your most recent weather file located? ' )
#'F:\AdvProj_Portal\Met395.dat'

# import into python
''' elements in the list:  
0 = array, 1 = year, 2 = Julian day, 3 = hour, 4 = ppt, 5 = tempAir, 6 = RelHumid 
The length check makes sure NOT to import shorter rows, which are battery readings.'''
datafile = open(filename, 'r')
weather = []
for row in datafile:
    row_data = row.strip().split(',')
    if len(row_data) == 7:
        row_data = map(float, row_data)
        weather.append(row_data)
        
# get data to be appended to database
weather_to_add = compile_weather_data(weather)


#DATABASE STUFF: open file to append to database
user = input('What is your username?: ')
yourpassword = input('Please enter your password: ')

con = dbapi.connect(host = 'serenity.bluezone.usu.edu',
              port = 1995,
              user = user,
              passwd = yourpassword)

cur = con.cursor()

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

cur.execute("""LOAD DATA LOCAL INFILE weather_to_add
INTO TABLE queries.weath
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
IGNORE 0 LINES""")
con.commit()

#append data to Hourly weather Table, making sure not to repeat data that already exists
cur.execute("INSERT INTO Portal.Rodents SELECT newdat.* FROM queries.newdat")
con.commit()

# run a query to add daily data to 'Daily' table in database 
cur.execute("""SELECT DISTINCTROW [Hourly (mm)].Year, [Hourly (mm)].Month, [Hourly (mm)].Day, 
Avg([Hourly (mm)].TempAir) AS TempAirAvg, Max([Hourly (mm)].TempAir) AS TempAirMax, 
Min([Hourly (mm)].TempAir) AS TempAirMin, Avg([Hourly (mm)].RelHumid) AS [RH Avg], 
Max([Hourly (mm)].RelHumid) AS [RH Max], Min([Hourly (mm)].RelHumid) AS [RH Min], 
Avg([Hourly (mm)].TempSoil) AS TempSoilAvg, Max([Hourly (mm)].TempSoil) AS TempSoilMax, Min([Hourly (mm)].TempSoil) AS TempSoilMin, Sum([Hourly (mm)].[Precipitation (mm)]) AS [Total Precipitation] INTO [Daily (mm)]
FROM [Hourly (mm)]
GROUP BY [Hourly (mm)].Year, [Hourly (mm)].Month, [Hourly (mm)].Day;""")


# run a query to add monthly data to 'Monthly table in database
