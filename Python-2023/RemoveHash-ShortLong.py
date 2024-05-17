import os

def DeleteFiles():
    OutputFileShort=open('/Users/DELL/Desktop/BI/W3SVC1/Outputshort.txt', 'w')
    OutputFileLong=open('/Users/DELL/Desktop/BI/W3SVC1/Outputlong.txt', 'w')


def CleanHash(filename):
    type=filename[-3:len(filename)]
    if (type=="log"):
    
        OutputFileShort=open('/Users/DELL/Desktop/BI/W3SVC1/Outputshort.txt', 'a')
        OutputFileLong=open('/Users/DELL/Desktop/BI/W3SVC1/Outputlong.txt', 'a')

        InFile = open('/Users/DELL/Desktop/BI/W3SVC1/'+filename,'r')
        print(filename)
    
        Lines= InFile.readlines()
        for line in Lines:
            if (line[0]!="#"):
                Split=line.split(" ")
                
                if (len(Split)==14):
                   
                   OutputFileShort.write(line)
                else:
                   if (len(Split)==18):
                       OutputFileLong.write(line)
                   else:
                       print ("Fault "+str(len(Split)))
                
                

arr=os.listdir("/Users/DELL/Desktop/BI/W3SVC1")
DeleteFiles()
for f in arr:
    CleanHash(f)

