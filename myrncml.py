import sys
import os
from xml.sax.handler import ContentHandler
from xml.sax import parse
import MySQLdb
import warnings
from colorama import init
from termcolor import colored, cprint
import config1
import pickle
import datetime

class ManagedObjectHandler(ContentHandler):
    in_headline = False
    def __init__(self,headlines):
        ContentHandler.__init__(self)
        self.isdata = ""
        self.param_value=[]
        self.param_list=[]
        self.headlines=headlines
        #self.grep = ["ADCE","BTS"]
        #**************************POINT1*****************************
        self.RNC=[]
        self.WBTS=[]
        self.WCEL=[]
        self.FMCS=[]
        self.FMCG=[]
        self.FMCI=[]
        self.IPBR=[]
        self.IPNB=[]
        self.IPQM=[]
        self.IUCS=[]
        self.IUCSIP=[]
        self.IUPS=[]
        self.IUPSIP=[]
        self.RNAC=[]
        self.RNFC=[]
        self.RNHSPA=[]

        self.RNMOBI=[]
        self.RNPS=[]
        self.RNRLC=[]
        self.RNTRM=[]
        self.WAC=[]

        self.ADJG=[]
        self.ADJS=[]


        #**************************POINT2*****************************

        self.ALL=[
        self.RNC,
        self.WBTS,
        self.WCEL,
        self.FMCS,
        self.FMCG,
        self.FMCI,
        self.IPBR,
        self.IPNB,
        self.IPQM,
        self.IUCS,
        self.IUCSIP,
        self.IUPS,
        self.IUPSIP,
        self.RNAC,
        self.RNFC,
        self.RNHSPA,

        self.RNMOBI,
        self.RNPS,
        self.RNRLC,
        self.RNTRM,
        self.WAC,
        self.ADJG,
        self.ADJS
        ]

        #**************************POINT3*****************************
        self.grep=[
        "RNC",
        "WBTS",
        "WCEL",
        "FMCS",
        "FMCG",
        "FMCI",
        "IPBR",
        "IPNB",
        "IPQM",
        "IUCS",
        "IUCSIP",
        "IUPS",
        "IUPSIP",
        "RNAC",
        "RNFC",
        "RNMOBI",
        "RNPS",
        "RNRLC",
        "RNTRM",
        "WAC",
        "ADJG",
        "ADJS"
        ]

    def startElement(self, name, Attributes):
        self.param_name=Attributes.values()
        if name == 'managedObject':
            self.param_name = []
            self.isdata = ""
            self.param_value=[]
            self.param_list=[]

            self.class_name = Attributes.values()[(Attributes.keys().index('class'))]
            #print "CLASS_NAME\n\n"
            #print self.class_name
            if self.class_name in self.grep:
                self.in_headline = True
            self.class_address = Attributes.values()[(Attributes.keys().index('distName'))]

    def characters(self, string):
        if self.in_headline:
            string = string.rstrip()
            if(len(string)>0):
                self.isdata = string
            else:
                self.isdata = "No_Data";

    def endElement(self, name):

        self.param_list.append(self.param_name)

        self.param_value.append(self.isdata)

        if name == 'managedObject':
            if self.in_headline:
                #print "\n\nParameter list for "+self.class_name+": "
                self.in_headline = False
                self.array_maker()

        if name =='raml':

            self.file_writer()

    def array_maker(self):
        if self.class_name in self.grep:

            line=""
            for paramName,paramVal in map(None,self.param_list,self.param_value):
                if (paramName==[] or paramVal=="No_Data"):
                    continue
                line =line+str(paramName)[3:-2]+"->"+str(paramVal)+","

            #**************************POINT4*****************************

            def rnc():
                self.RNC.append(str(self.class_address)+","+line)

            def wbts():
                self.WBTS.append(str(self.class_address)+","+line)

            def wcel():
                self.WCEL.append(str(self.class_address)+","+line)

            def fmcs():
                self.FMCS.append(str(self.class_address)+","+line)

            def fmcg():
                self.FMCG.append(str(self.class_address)+","+line)

            def fmci():
                self.FMCI.append(str(self.class_address)+","+line)

            def ipbr():
                self.IPBR.append(str(self.class_address)+","+line)

            def ipnb():
                self.IPNB.append(str(self.class_address)+","+line)

            def ipqm():
                self.IPQM.append(str(self.class_address)+","+line)

            def iucs():
                self.IUCS.append(str(self.class_address)+","+line)

            def iucsip():
                self.IUCSIP.append(str(self.class_address)+","+line)

            def iups():
                self.IUPS.append(str(self.class_address)+","+line)

            def iupsip():
                self.IUPSIP.append(str(self.class_address)+","+line)

            def rnac():
                self.RNAC.append(str(self.class_address)+","+line)

            def rnfc():
                self.RNFC.append(str(self.class_address)+","+line)

            def rnhspa():
                self.RNHSPA.append(str(self.class_address)+","+line)

            def rnmobi():
                self.RNMOBI.append(str(self.class_address)+","+line)

            def rnps():
                self.RNPS.append(str(self.class_address)+","+line)

            def rnrlc():
                self.RNRLC.append(str(self.class_address)+","+line)

            def rntrm():
                self.RNTRM.append(str(self.class_address)+","+line)

            def wac():
                self.WAC.append(str(self.class_address)+","+line)

            def adjg():
                self.ADJG.append(str(self.class_address)+","+line)

            def adjs():
                self.ADJS.append(str(self.class_address)+","+line)




            #**************************POINT5*****************************

            options={
            "RNC":rnc,
            "WBTS":wbts,
            "WCEL":wcel,
            "FMCS":fmcs,
            "FMCG":fmcg,
            "FMCI":fmci,
            "IPBR":ipbr,
            "IPNB":ipnb,
            "IPQM":ipqm,
            "IUCS":iucs,
            "IUCSIP":iucsip,
            "IUPS":iups,
            "IUPSIP":iupsip,
            "RNAC":rnac,
            "RNFC":rnfc,
            "RNMOBI":rnmobi,
            "RNPS":rnps,
            "RNRLC":rnrlc,
            "RNTRM":rntrm,
            "WAC":wac,
            "ADJG":adjg,
            "ADJS":adjs
            }
            options[self.class_name]()



    def file_writer(self):

        for fname in self.grep:

            file_name =config1.PARSED_DB_DIR+fname

            fp = open (file_name,"a+")
            for data in self.ALL[self.grep.index(fname)]:
                fp.writelines(data)
                fp.writelines('\n')
            fp.close()

        '''
        SQL operations

        '''

class SqlHandler:
    def __init__(self):
        self.parameter = []


    '''
    Create Table based on PARSED_DATABASE file

    '''

    def Dbdelcreate(self):
        db = MySQLdb.connect(config1.DB_HOST,config1.DB_USER,config1.DB_PASS,config1.DB_NAME)
        cursor = db.cursor()
        cursor.execute("DROP DATABASE IF EXISTS myrncml; CREATE DATABASE myrncml;")
        cursor.close()
        db.commit()

    def Createtable(self):
        db = MySQLdb.connect(config1.DB_HOST,config1.DB_USER,config1.DB_PASS,config1.DB_NAME)
        self.table_name = os.listdir(config1.PARSED_DB_DIR)
        for table in self.table_name:
            self.parameter = []
            fp = open(config1.PARSED_DB_DIR+table,"r")

            for line in fp.readlines():
                plmn = line.split(",")[0]
                for p in plmn.split("/")[1:]:
                    if p.split("-")[0] not in self.parameter:
                        self.parameter.append(p.split("-")[0])

                for csv in line.split(",")[1:]:
                    if csv.split("->")[0] not in self.parameter:
                        self.parameter.append(csv.split("->")[0])
            fp.close()

            query_1 = "CREATE TABLE IF NOT EXISTS "+table+" (PLMN VARCHAR(60),"
            query_2=""

            self.parameter[:]=(value for value in self.parameter if value != '\n')

            if (len(self.parameter)==0):
                continue
            for p in self.parameter:
                query_2+=p+"  VARCHAR(60),"
            query = query_1+query_2[:-1]+");"
            cursor=db.cursor()

            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                x = "["+table+"]"
                sys.stdout.write("Creating table ")
                cprint("%5s"%table,'blue','on_white',attrs=['bold'])
                cursor.execute(query)
            cursor.close()
        db.commit()

    '''

    Update Table based on PARSED_DATABASE file

    '''
    def Updatetable(self):
        db = MySQLdb.connect(config1.DB_HOST,config1.DB_USER,config1.DB_PASS,config1.DB_NAME)
        cursor = db.cursor()
        for table in self.table_name:
            print table
            fp = open(config1.PARSED_DB_DIR+table,"r")
            data =[]
            param=[]
            plmn = []
            for line in fp.readlines():
                data =[line.split(",")[0]]
                param=['PLMN']
                plmn = line.split(",")[0]
                for p in plmn.split("/")[1:]:
                    data.append(p.split("-")[1])
                    param.append(p.split("-")[0])
                for csv in line.split(",")[1:-1]:
                    if csv.split("->")[0] not in param:
                        data.append(csv.split("->")[1])
                        param.append(csv.split("->")[0])
                if len(param)<2:
                    continue
                data = str(data).replace("[","(")
                param = str(param).replace("[","(").replace("'","")
                query =  "INSERT INTO "+table+" "+param.replace("]",")")+" VALUES"+data.replace("]",")")+";"
                with warnings.catch_warnings(record=True) as w:
                    warnings.simplefilter("always")
                    cursor.execute(query)
            sys.stdout.write("Data inserted to ")
            cprint("%5s"%table,'blue','on_cyan')

        cursor.close()
        db.commit()
        fp.close()

if __name__=='__main__':

    def Mysignature():

        cprint("%-26s"%"Author:\t\tTapon Paul",'blue','on_white',attrs=['bold'])
        cprint("%-31s"%"Program Name:\tMyflexml",'white','on_blue',attrs=['bold'])
        cprint("%-29s"%"Created On:\t15th Oct, 2013",'blue','on_white',attrs=['bold'])
        cprint("%-26s"%"Version:\t2.4",'white','on_blue',attrs=['bold'])
        return 0

    def Drawline():

        cprint("\n----------------x----------------\n",attrs=['bold'])
        return 0

    def wait():

        try:
            input()
        except:
            print "Good BYE"
        return 0

    init()

    print "\n"

    sql = SqlHandler()

    sql.Dbdelcreate()

    obj = datetime.date.today()
    d = str(obj)
    mod_date = d.split("-")[2]+d.split("-")[1]+d[2:4]

    XML_DB_DIR = config1.XML_DB_DIR+"RNC_"+mod_date+"\\"
    print XML_DB_DIR
    for xml_db in os.listdir(XML_DB_DIR):

        for f in os.listdir(config1.PARSED_DB_DIR):
            os.remove(config1.PARSED_DB_DIR+f)
        try:
            fp = open(XML_DB_DIR+xml_db,"r")
            fp2 = open(XML_DB_DIR+"_temp"+xml_db,"w+")
        except:
            cprint("Abnormal quit previously, Run again to fix it!","white","on_red",attrs=['bold'])
            sys.exit()
        for line in fp:
            if (line.find("DOCTYPE")<0):
                fp2.write(line)
        fp.close()
        fp2.close()
        os.remove(XML_DB_DIR+xml_db)

    for xml_db in os.listdir(XML_DB_DIR):
        os.rename(XML_DB_DIR+xml_db,XML_DB_DIR+xml_db.replace("_temp",""))

    #Mysignature()

    Drawline()

    cprint("Parsing Started",'green',attrs=['bold'])

    Drawline()
    headlines=[]
    for xml_db in os.listdir(XML_DB_DIR):
        cprint("Parsing "+xml_db,'white','on_blue')
        parse(XML_DB_DIR+xml_db, ManagedObjectHandler(headlines))
    Drawline()
    cprint("MySQL in action",'cyan',attrs=['bold'])

    Drawline()

    sql.Createtable()

    Drawline()

    cprint("Table creation completed",'magenta',attrs=['bold'])

    Drawline()

    sql.Updatetable()

    Drawline()

    cprint("Table update completed",'blue',attrs=['bold'])

    Drawline()

    cprint(" **** XML File parsing done ****\n",'green',attrs=['bold'])

    wait()
