
    

def BuildFactShort():
    InFile = open('/Users/DELL/Desktop/BI/W3SVC1/Outputshort.txt','r')
    OutFact1=open('/Users/DELL/Desktop/BI/W3SVC1/OutFact1.txt', 'a')

    Lines= InFile.readlines()
    for line in Lines:
        Split=line.split(" ")
        Browser=Split[9].replace(",","")
        Out=Split[0]+","+Split[1]+","+Browser+","+Split[8]+","+Split[13]

        OutFact1.write(Out)
def BuildFactLong():
    InFile = open('/Users/DELL/Desktop/BI/W3SVC1/Outputlong.txt','r')
    OutFact1=open('/Users/DELL/Desktop/BI/W3SVC1/OutFact1.txt', 'a')

    Lines= InFile.readlines()
    for line in Lines:
        Split=line.split(" ")
        Browser=Split[9].replace(",","")
        Out=Split[0]+","+Split[1]+","+Browser+","+Split[8]+","+Split[16]
        OutFact1.write(Out)

with open('/Users/DELL/Desktop/BI/W3SVC1/OutFact1.txt', 'w') as file:
    file.write("Date,Time,Browser,IP,ResponseTime\n")
BuildFactShort()
BuildFactLong()
