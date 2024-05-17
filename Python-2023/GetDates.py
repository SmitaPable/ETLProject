from datetime import datetime

InFile = open('/Users/DELL/Desktop/BI/W3SVC1/DimDateUniq.txt', 'r')
OutputFile=open('/Users/DELL/Desktop/BI/W3SVC1/DimDateTable.txt', 'w')
Days=["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
with open('/Users/DELL/Desktop/BI/W3SVC1/DimDateTable.txt', 'w') as file:
    file.write("Date,Year,Month,Day,DayofWeek\n")
Lines= InFile.readlines()
for line in Lines:
    line=line.replace("\n","")
    date=datetime.strptime(line,"%Y-%m-%d").date()
    weekday=Days[date.weekday()]
    out=str(date)+","+str(date.year)+","+str(date.month)+","+str(date.day)+","+weekday+"\n"
    print(out)
    with open('/Users/DELL/Desktop/BI/W3SVC1/DimDateTable.txt', 'a') as file:
       file.write(out)
