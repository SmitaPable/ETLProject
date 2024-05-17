
InFile = open('/Users/DELL/Desktop/BI/W3SVC1/OutFact1.txt', 'r')
OutputFile=open('/Users/DELL/Desktop/BI/W3SVC1/DimIP.txt', 'w')

Lines= InFile.readlines()
for line in Lines:
    Split=line.split(",")
    Out=Split[3]+"\n"
    OutputFile.write(Out)
