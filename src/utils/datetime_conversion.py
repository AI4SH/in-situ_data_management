'''
Created on 24 Dec 2017

@author: thomasgumbricht
'''

# Standard library imports

import datetime

def Now():
    return datetime.datetime.now()

def Today():
    return datetime.datetime.now().date()

def DateFromTmTime(t):
    return datetime.datetime.fromtimestamp(t).date()

def yyyydoyDate(yyyydoy):
    dt = datetime.datetime(int(yyyydoy[0:4]),1,1)
    dtdelta = datetime.timedelta(days=int(yyyydoy[4:7])-1)
    datum = dt + dtdelta
    return datum.date()

def yyyy_mm_dd_Date(yyyy,mm,dd):
    dt = datetime.datetime(int(yyyy),int(mm),int(dd))
    return dt.date()

def yyyymmdd_str_to_date(yyyymmdd):
    dt = datetime.datetime(int(yyyymmdd[0:4]),int(yyyymmdd[4:6]),int(yyyymmdd[6:8]))
    return dt.date()

def yyyy_mm_dd_Str_ToDate(yyyymmdd):
    dt = datetime.datetime(int(yyyymmdd[0:4]),int(yyyymmdd[5:7]),int(yyyymmdd[8:10]))
    return dt.date()

def DateToStrDate(date):
    return date.strftime("%Y%m%d")

def DateToStrPointDate(date):
    return date.strftime("%Y.%m.%d")

def DateToStrHyphenDate(date):
    return date.strftime("%Y-%m-%d")

def IntYYYYMMDDDate(yyyy,mm,dd):
    dt = datetime.datetime(yyyy,mm,dd)
    return dt.date()

def YYYYDOYStr(date):
    DOY = date.timetuple().tm_yday
    if DOY < 10:
        doyStr = '00%(d)d' %{'d':DOY}
    elif DOY < 100:
        doyStr = '0%(d)d' %{'d':DOY}
    else:
        doyStr = '%(d)d' %{'d':DOY}
    return doyStr

def DoyStr(DOY):
    if DOY < 10:
        doyStr = '00%(d)d' %{'d':DOY}
    elif DOY < 100:
        doyStr = '0%(d)d' %{'d':DOY}
    else:
        doyStr = '%(d)d' %{'d':DOY}
    return doyStr

def MonthToStr(m):
    if m < 10:
        return '0%(m)d' %{'m':m}
    else:
        return '%(m)d' %{'m':m}

def DeltaTime(thisdate,deltadays):
    thisdate += datetime.timedelta(days=deltadays)
    return thisdate

def SetYYYY1Jan(year):
    return datetime.datetime(year=year, month=1, day=1).date()

def ResetDateToYYYYMM01(date):
    return datetime.datetime(year=date.year, month=date.month, day=1).date()

def DateToYYYYDOY(date):
    doy = YYYYDOYStr(date)
    yyyydoyStr = '%(y)d%(doy)s' %{'y':date.year,'doy':doy}
    return yyyydoyStr

def YYYYDOYToDate(year,doy):
    newdate = datetime.datetime(year, 1, 1) + datetime.timedelta(doy - 1)
    return newdate.date()
    
def DateToDOY(date):
    return date.timetuple().tm_yday

def YYYYMMtoYYYYMMDD(yyyymmStr,d):
    from calendar import monthrange
    y = int(yyyymmStr[0:4])
    m = int(yyyymmStr[4:6])
    if d >= 31:
        d = monthrange(y, m)[1]
    dt = datetime.datetime(y,m,d)
    return dt.date()

def DateToYYYYMM(date):
    y = '%(y)d' %{'y':date.year}
    m = MonthToStr(date.month)
    return '%s%s' %(y,m)
    

def GetLastDayOfMonth(y,m):
    from calendar import monthrange
    return monthrange(y, m)
  
def IsDatetime(acqdate): 
    return isinstance(acqdate,datetime.datetime)

def DateTimeFromStartDate(yyyy,mm,dd,hh,mn,ss,sec):  
    start = datetime.datetime(yyyy,mm,dd,hh,mn,ss)
    dt = start + datetime.timedelta(0,sec)
    return dt.date(),dt.time()

def AddMonth(dt,monthsToAdd):
    from dateutil.relativedelta import relativedelta
    mDelta = relativedelta(months=monthsToAdd)
    return dt+mDelta

def AddYear(dt,yearsToAdd):
    from dateutil.relativedelta import relativedelta
    mDelta = relativedelta(years=yearsToAdd)
    return dt+mDelta

def Delta_days(start_date,end_date):   
    if isinstance(start_date,datetime.datetime):
        start_date_date = start_date.date()
        end_date_date = end_date.date()
    elif isinstance(start_date,str):
        if len(start_date) == 8:
            start_date_date = yyyymmdd_str_to_date(start_date)
            end_date_date = yyyymmdd_str_to_date(end_date)
        else:
            start_date_date = yyyy_mm_dd_Str_ToDate(start_date)
            end_date_date = yyyy_mm_dd_Str_ToDate(end_date)
    return (end_date_date-start_date_date).days

def DateDiff(d2,d1):
    return (d2 - d1).days

def IsDate(dt):
    return isinstance(dt, datetime.date)

def DateTimeToNumpyDate(dt):
    from numpy import datetime64
    return datetime64(dt)

def GetDaysInYYYY_MM(y,m):
    from calendar import monthrange
    return monthrange(y, m)

def GetMonthRange(startdate,enddate):
    from dateutil import rrule
    #print(list(rrule.rrule(rrule.MONTHLY, dtstart=startdate, until=enddate)))
    return list(rrule.rrule(rrule.MONTHLY, dtstart=startdate, until=enddate))

def DateRange(date1, date2):
    dateL = []
    for n in range(int ((date2 - date1).days)+1):
        dateL.append(date1 + datetime.timedelta(n))
        #yield date1 + datetime.timedelta(n)
    return dateL

if __name__ == "__main__":
    datum = yyyydoyDate('2012162')
    print (datum)

    yyyydoystr = '2018261'
    datum = yyyydoyDate(yyyydoystr)
    print (datum)
