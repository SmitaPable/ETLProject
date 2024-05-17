
InFile = open('/Users/DELL/Desktop/BI/W3SVC1/OutFact1.txt', 'r')
OutputFile=open('/Users/DELL/Desktop/BI/W3SVC1/DimDate.txt', 'w')

Lines= InFile.readlines()
for line in Lines:
    Split=line.split(",")
    Out=Split[0]+"\n"
    OutputFile.write(Out)
