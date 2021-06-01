import array as timeStamp
import array as deviceID
import struct
import datetime
import time
import sys
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client import InfluxDBClient, Point, WritePrecision

# database variables
token = "btWWE8wrzH_Namd3Bqxn5EUMfBti5Sn4NC731XBvazBCVhaBs4Uzri9bmdonxPQVO3DQtJloZbK_nS9eFq5fXA=="
org = "Equimetrics"
bucket = "test"
url="http://138.68.152.149:8086"

# instantiate the client
client = InfluxDBClient(url=url,token=token,org=org)
    
# instantiate the write api
write_api = client.write_api(write_options=SYNCHRONOUS)

# start the process timer
startTime = time.time()

if (sys.argv[1]):
    print("Converting binary file: ", sys.argv[1])
else:
    print("Error: no binary file specificed.")
    exit()
        
# open the read binary file and extract the data
with open(sys.argv[1], "rb") as fIn:

    # read in the header section, set device and packet0 timestamp
    spacer=struct.unpack('>c',fIn.read(1))
    deviceID = struct.unpack('>9s', fIn.read(9))
   
    s=struct.Struct('9s')
    strX=str(deviceID[0])
    deviceIDString = s.pack(strX.encode('utf-8'))
    
    spacer = struct.unpack('>c',fIn.read(1))
    baseTimeStamp = struct.unpack('<Q', fIn.read(6) + b'\x00\x00')
    epochTime=int(str(baseTimeStamp[0]))
    spacer = struct.unpack('>c',fIn.read(1))
    sampTime= struct.unpack('<c',fIn.read(1))
    Ts=int(sampTime[0])
    spacer = struct.unpack('>c',fIn.read(1))

    # now loop through reading & writing all the data packets
    page=1

    while page<11:
        
        # first 200 packets are page 1, second 200 packets are page 2, and so on
        # packet=1, the mega packet, with all parameters in it
        packet=1

        # need to increment by +5ms for 2nd and subsequent passes through this loop
        if page >1:
            epochTime+=Ts
        
        # read packet 1 parameters from binary file and insert into database
        spacer=struct.unpack('<c',fIn.read(1))
        ecgRaw = struct.unpack('<Hx',fIn.read(3))
        imuRaw = struct.unpack('<HHHHHHx', fIn.read(13))
        staticsRaw = struct.unpack('<fxHxBx',fIn.read(10))
        L=list(imuRaw)
        ecg=int(ecgRaw[0])
        ax=int(L[0])
        ay=int(L[1])
        az=int(L[2])
        gx=int(L[3])
        gy=int(L[4])
        gz=int(L[5])
        M=list(staticsRaw)
        temp=float("{:5.2f}".format(M[0]))
        gsr=int(M[1])
        hr=int(M[2])
        millistime=epochTime/1000
        timeStamp=datetime.datetime.fromtimestamp(millistime)        
        p = Point("test")\
            .tag("horseID", "RockOfGibraltar").tag("sessionID", "2021052617h17-1")\
            .field("ecg",ecg).field("ax",ax).field("ay",ay).field("az",az)\
            .field("gx",gx).field("gy",gy).field("gz",gz).field("temp",temp)\
            .field("GSR",gsr).field("HR",hr)\
            .time(timeStamp, WritePrecision.MS) 
        write_api.write(bucket, org, p)
        
        # read packet 2 and ingest into database
        spacer = struct.unpack('<c', fIn.read(1))
        packet=2
        epochTime+=Ts
        millistime=epochTime/1000
        timeStamp=datetime.datetime.fromtimestamp(millistime)        
        ecgRaw = struct.unpack('<Hx',fIn.read(3))
        p = Point("test").tag("horseID", "RockOfGibraltar")\
            .tag("sessionID", "2021052617h17-1").field("ecg", int(ecgRaw[0]))\
            .time(timeStamp, WritePrecision.MS)
        write_api.write(bucket, org, p)
        
        # read and write 199 more packets

        packet=3
        while packet<201: 
            #read ECG + IMU and insert into the database
            spacer=struct.unpack('>c',fIn.read(1))
            ecgRaw = struct.unpack('<Hx',fIn.read(3))
            imuRaw = struct.unpack('<HHHHHHx', fIn.read(13))
            L=list(imuRaw)
            epochTime+=Ts
            millistime=epochTime/1000
            timeStamp=datetime.datetime.fromtimestamp(millistime)
            p = Point("test").tag("horseID", "RockOfGibraltar")\
                .tag("sessionID", "2021052617h17-1").field("ecg", int(ecgRaw[0])).field("ax",int(L[0]))\
                .field("ay",int(L[1])).field("az",int(L[2])).field("gx",int(L[3])).field("gy",int(L[4]))\
                .field("gz",int(L[5])).time(timeStamp, WritePrecision.MS)
            write_api.write(bucket, org, p)
            packet+=1

            #read ECG only now and insert into database
            spacer=struct.unpack('<c',fIn.read(1))
            ecgRaw= struct.unpack('<Hx',fIn.read(3))
            epochTime+=Ts
            millistime=epochTime/1000
            timeStamp=datetime.datetime.fromtimestamp(millistime)
            p = Point("test").tag("horseID", "RockOfGibraltar")\
                .tag("sessionID", "2021052617h17-1").field("ecg", int(ecgRaw[0]))\
                .time(timeStamp, WritePrecision.MS)
            write_api.write(bucket, org, p)
            packet+=1
        
        page+=1

# calatulte the time elapsed during the process and print out to screen
executionTime = (time.time() - startTime)
print('Execution time in seconds: ' + str(executionTime))

# close out the database client object
client.close()
