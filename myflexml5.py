import sys
import os
from xml.sax.handler import ContentHandler
from xml.sax import parse
import MySQLdb
import warnings
from colorama import init
from termcolor import colored, cprint
import config
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
        self.BSC=[]
        self.BCF=[]
        self.BTS=[]
        self.TRX=[]
        self.LAPD=[]
        self.ADCE=[]
        self.ADJW=[]
        self.HOC=[]
        self.POC=[]
        self.DAP=[]
        self.CSDAP = []
        self.ALL=[self.BSC,self.BCF,self.BTS,self.TRX,self.LAPD,self.ADCE,self.ADJW,self.HOC,self.POC,self.DAP,self.CSDAP]


        self.ADCE_ADDR=[]
        self.grep=["BSC","BCF","BTS","TRX","LAPD","ADCE","ADJW","HOC","POC","DAP","CSDAP"]


    def startElement(self, name, Attributes):
        self.param_name=Attributes.values()
        if name == 'managedObject':
            self.param_name = []
            self.isdata = ""
            self.param_value=[]
            self.param_list=[]
            self.class_address=[]
            self.class_name = Attributes.values()[(Attributes.keys().index('class'))]
            if self.class_name in self.grep:
                self.in_headline = True
            self.class_address = Attributes.values()[(Attributes.keys().index('distName'))]

    def characters(self, string):
        if self.in_headline:
            string = string.rstrip()
            if(len(string)>0):
                self.isdata = self.isdata+string
            else:
                self.isdata = "";

    def endElement(self, name):

        self.param_list.append(self.param_name)
        self.param_value.append(self.isdata)

        if name == 'managedObject':
            if self.in_headline:
                self.in_headline = False
                self.array_maker()
        if name =='raml':
            self.file_writer()

    def array_maker(self):

        if self.class_name in self.grep:
            #a =self.class_address+self.param_list+self.param_value
            #print a
            '''
            self.ADCE.append(self.param_list)
            self.ADCE_VAL.append(self.param_value)
            self.ADCE_ADDR.append(self.class_address)
            '''
            line=""
            for paramName,paramVal in map(None,self.param_list,self.param_value):
                #fpp.writelines(str(paramName)+"->"+str(paramVal)+"\n")
                if (paramName==[] or paramVal==""):
                    continue
                line =line+str(paramName)[3:-2]+"->"+str(paramVal)+","
                #print line

            def adce():
                #print "ALL_ARRAY "+self.class_name
                self.ADCE.append(str(self.class_address)+","+line)
                #print self.ADCE
            def bts():
                #print "ALL_ARRAY "+self.class_name
                self.BTS.append(str(self.class_address)+","+line)
                #fpp.writelines(line+"\n")
                #print self.BTS

            def bsc():
                #print line
                #print "ALL_ARRAY "+self.class_name
                self.BSC.append(str(self.class_address)+","+line)
                #print self.BSC
            def bcf():
                #print "ALL_ARRAY "+self.class_name
                self.BCF.append(str(self.class_address)+","+line)
                #print self.BCF
            def trx():
                #print "ALL_ARRAY "+self.class_name
                self.TRX.append(str(self.class_address)+","+line)
                #print self.TRX
            def lapd():
                #print "ALL_ARRAY "+self.class_name
                self.LAPD.append(str(self.class_address)+","+line)
                #print self.LAPD

            def adjw():
                #print "ALL_ARRAY "+self.class_name
                self.ADJW.append(str(self.class_address)+","+line)
                #print self.ADJW
            def hoc():
                #print "ALL_ARRAY "+self.class_name
                self.HOC.append(str(self.class_address)+","+line)
                #print self.HOC

            def poc():
                #print "ALL_ARRAY "+self.class_name
                self.POC.append(str(self.class_address)+","+line)
                #print self.POC

            def dap():
                #print "ALL_ARRAY "+self.class_name
                self.DAP.append(str(self.class_address)+","+line)
                #print self.DAP
            def csdap():
                #print "ALL_ARRAY "+self.class_name
                self.CSDAP.append(str(self.class_address)+","+line)
                #print self.CSDAP

            options={"BSC":bsc,
                     "BCF":bcf,
                     "BTS":bts,
                     "TRX":trx,
                     "LAPD":lapd,
                     "ADCE":adce,
                     "ADJW":adjw,
                     "HOC":hoc,
                     "POC":poc,
                     "DAP":dap,
                     "CSDAP":csdap
                     }
            options[self.class_name]()




    def file_writer(self):

        for fname in self.grep:

            file_name =config.PARSED_DB_DIR+fname

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
        db = MySQLdb.connect(config.DB_HOST,config.DB_USER,config.DB_PASS,config.DB_NAME)
        cursor = db.cursor()
        cursor.execute("DROP DATABASE IF EXISTS myflexml; CREATE DATABASE myflexml;")
        cursor.close()
        db.commit()

    def Createtable(self):
        db = MySQLdb.connect(config.DB_HOST,config.DB_USER,config.DB_PASS,config.DB_NAME)
        self.table_name = os.listdir(config.PARSED_DB_DIR)
        for table in self.table_name:
            self.parameter = []
            fp = open(config.PARSED_DB_DIR+table,"r")

            for line in fp.readlines():
                plmn = line.split(",")[0]
                for p in plmn.split("/")[1:]:
                    if p.split("-")[0] not in self.parameter:
                        self.parameter.append(p.split("-")[0])

                for csv in line.split(",")[1:]:
                    if csv.split("->")[0] not in self.parameter:
                        self.parameter.append(csv.split("->")[0])
            fp.close()

            query_1 = "CREATE TABLE IF NOT EXISTS "+table+" (PLMN VARCHAR(70),"
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
        db = MySQLdb.connect(config.DB_HOST,config.DB_USER,config.DB_PASS,config.DB_NAME)
        cursor = db.cursor()
        #fptest =open("C:\\qry.txt",'w+')
        for table in self.table_name:
            print table
            fp = open(config.PARSED_DB_DIR+table,"r")
            data =[]
            param=[]
            plmn = []
            for line in fp.readlines():
                data =[line.split(",")[0]]          #['PLMN-PLMN/BSC-372586/BCF-9/BTS-99/TRX-9']

                param=['PLMN']
                plmn = line.split(",")[0]           # PLMN-PLMN/BSC-372586/BCF-9/BTS-99/TRX-9

                for p in plmn.split("/")[1:]:       # ['BSC-372586', 'BCF-9', 'BTS-99', 'TRX-9']
                    #print p
                    data.append(p.split("-")[1])    # 372586
                    param.append(p.split("-")[0])   # BSC
                #if table=="BTS":
                    #fptest.writelines(line)
                for csv in line.split(",")[1:-1]:
                    if csv.split("->")[0] not in param:
                        #print csv.split("->")[1]

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
            #fptest.writelines("\n")

        cursor.close()
        db.commit()
        #fptest.close()
        fp.close()

    def modifyTable(self):
        db = MySQLdb.connect(config.DB_HOST,config.DB_USER,config.DB_PASS,config.DB_NAME)
        cursor = db.cursor()
        cursor.execute("ALTER TABLE bts ADD lacCI VARCHAR(60) AFTER bts;")
        cursor.execute("UPDATE bts SET lacCI = concat(bts.locationareaidlac,bts.cellid);")

        cursor.execute("ALTER TABLE trx ADD match_plmn VARCHAR(60) AFTER plmn;")

        cursor.execute("UPDATE trx SET match_plmn = SUBSTRING_INDEX(trx.plmn,'/', 4);")
        cursor.execute("alter table adce ADD tgtBTS varchar(60) after BTS, ADD tgtBSC varchar(60) after BTS;")

        cursor.execute("UPDATE adce SET tgtBTS = SUBSTRING_INDEX(targetCellDN,'/', -3);")
        cursor.execute("UPDATE adce SET tgtBSC = SUBSTRING_INDEX(tgtBTS,'/', 1);")
        cursor.execute("UPDATE adce SET tgtBSC = SUBSTRING_INDEX(tgtBSC,'-', -1);")

        cursor.execute("UPDATE adce SET tgtBTS = SUBSTRING_INDEX(tgtBTS,'/', -1);")
        cursor.execute("UPDATE adce SET tgtBTS = SUBSTRING_INDEX(tgtBTS,'-', -1);")

        #cursor.execute("ALTER TABLE adce ADD match_plmn VARCHAR(60) AFTER plmn;")
        #cursor.execute("UPDATE adce SET match_plmn = SUBSTRING_INDEX(adce.plmn,'/', 4);")
        #cursor.execute("ALTER TABLE adce ADD tgtLacCI VARCHAR(60) AFTER bts, ADD srcLacCI VARCHAR(60) after bts;")
        #cursor.execute("update adce t1 inner join bts t2 on t1.match_plmn = t2.plmn set t1.srcLacCI = t2.lacci,tgtLacCI=concat(adjacentcellidlac,adjacentcellidci);")
        cursor.close()
        db.commit()

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

    #---------------------FILE REMOVE,MODIFY XML FILE,RENAME-----------------------------------

    obj = datetime.date.today()
    d = str(obj)
    mod_date = d.split("-")[2]+d.split("-")[1]+d[2:4]

    XML_DB_DIR = config.XML_DB_DIR+"Flexi_"+mod_date+"\\"
    print XML_DB_DIR
    for xml_db in os.listdir(XML_DB_DIR):

        for f in os.listdir(config.PARSED_DB_DIR):
            os.remove(config.PARSED_DB_DIR+f)
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
    #---------------------------------------------------------------------------


    cprint("Parsing Started",'green',attrs=['bold'])

    Drawline()
    headlines=[]
    #fpp = open("C:\\check.txt","a+")
    #----------------------------FILE PARSING-----------------------------------
    for xml_db in os.listdir(XML_DB_DIR):
        cprint("Parsing "+xml_db,'white','on_blue')
        parse(XML_DB_DIR+xml_db, ManagedObjectHandler(headlines))
    Drawline()
    #---------------------------------------------------------------------------
    #fpp.close()

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

    #sql.modifyTable()

    cprint(" **** XML File parsing done ****\n",'green',attrs=['bold'])

    wait()
