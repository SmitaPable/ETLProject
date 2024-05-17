import requests
import json

InFile=open('/Users/DELL/Desktop/BI/W3SVC1/DimIPUniq.txt', 'r')
OutFile=open('/Users/DELL/Desktop/BI/W3SVC1/DimIPLoc.txt', 'w')





Lines= InFile.readlines()
for line in Lines:
    line=line.replace("\n","")
    # URL to send the request to
    request_url = 'https://geolocation-db.com/jsonp/' + line
    print (request_url)
    # Send request and decode the result
    response = requests.get(request_url)
    result = response.content.decode()
# Clean the returned string so it just contains the dictionary data for the IP address
    result = result.split("(")[1].strip(")")
# Convert this data into a dictionary
    result  = json.loads(result)
    out=line+","+str(result["country_code"])+","+str(result["country_name"])+","+str(result["city"])+","+str(result["latitude"])+","+str(result["longitude"])+"\n"
    print(out)
    with open('/Users/DELL/Desktop/BI/W3SVC1/DimIPLoc.txt', 'a') as file:
       file.write(out)
    
