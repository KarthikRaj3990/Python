import psycopg2
import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.utils import COMMASPACE
from email import encoders
from os.path import dirname, join
from datetime import date, timedelta
import datetime
import logging
import configparser
import csv


parser = configparser.ConfigParser()
parser.read('PFpaynowreport.ini')
timenow = datetime.datetime.now()
current_dir = dirname(__file__)
PFcsv_fileList =[]
AIFcsv_fileList =[]



user=parser['DB']['user']
password=parser['DB']['password']
host=parser['DB']['host']
port=parser['DB']['port']
print (parser,user,password)
smtpServer=parser['EMAIL']['smtpServer']
fromAddr=parser['EMAIL']['fromAddr']
PFtoAddr=parser['EMAIL']['PFtoAddr']
AIFtoAddr=parser['EMAIL']['AIFtoAddr']

PFemailmessage='Planet Fitness Paynow invoices '
AIFemailmessage='AIF Report - Pay now invoices '


def PFpaynowreport():

    global PFquerycount

    try:
        connection = psycopg2.connect(user = user,
                                      password = password,
                                      host = host,
                                      port = port,
                                      database = "ezbilling")
        cursor = connection.cursor()
        postgreSQL_select_Query1 = "select m.tradingname, c.firstname, c.lastname, i.id ""invoiceid"", i.invoicenumber, i.invoicedate, i.billattempts, it.status, ii.amount, it.createdon::date ""transactioncreatedon"" from invoicetransaction it "
        postgreSQL_select_Query1 += "join invoice i on i.id = it.invoiceid join merchant m on cast (m.id as varchar) = i.issueby join customer c on cast (c.id as varchar) = i.issueto join invoiceitem ii on ii.invoiceid = i.id where "
        postgreSQL_select_Query1 += "i.issueby in ('f1f61a36-ebea-4128-bd7b-4a1aa085f2ac', '0926fbd7-1c22-4a8c-b873-ad066380c64d', 'c7735cb3-009c-4da5-b21f-08bf13e2ccb2','69f2d99b-25c6-472f-b422-d411553ab135','08596103-bff6-407c-ad7e-670e3af41953','76fc54e9-7149-4d96-9f5a-373c4496d9c4') and i.invoicedate >= ('%s')" % (date.today()-timedelta(days=14))
        postgreSQL_select_Query1 += "and it.channel in ('online_payment_recovery') and it.status ='SUCCESS' and paymentsource in ('payment_processor') and ii.type in ('subscription_payment','setup_payment','on_demand_payment','addon_payment') order by m.tradingname,i.invoicedate, c.firstname, c.lastname"
        cursor.execute(postgreSQL_select_Query1)
        PFquerycount=cursor.rowcount
        print(cursor.rowcount)
        PFpaynowreport = cursor.fetchall() 
        csvTitle = ["tradingname", "firstname", "lastname", "invoiceid", "invoicenumber", "invoicedate", "billattempts", "status", "amount", "transactioncreatedon"]
        populatePFCSV(PFpaynowreport,csvTitle)

    except (Exception, psycopg2.Error) as error :
        print ("Error while fetching data from PostgreSQL - ", error)
    finally:
    #closing database connection.
        if(connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")



def AIFpaynowreport():

    global AIFquerycount

    try:
        connection = psycopg2.connect(user = user,
                                      password = password,
                                      host = host,
                                      port = port,
                                      database = "ezbilling")
        cursor = connection.cursor()
        postgreSQL_select_Query1 = "select m.tradingname, c.firstname, c.lastname,c.membernumber ""referencecode"", i.id ""invoiceid"", i.invoicenumber, i.invoicedate, i.billattempts, it.status, it.amount, it.createdon::date ""transactioncreatedon"" from invoicetransaction it "
        postgreSQL_select_Query1 += "join invoice i on i.id = it.invoiceid join merchant m on cast (m.id as varchar) = i.issueby join customer c on cast (c.id as varchar) = i.issueto where "
        postgreSQL_select_Query1 += "i.issueby in ('d030b494-6fca-4155-bca4-f8a44760c040') and i.invoicedate >= ('%s')" % (date.today()-timedelta(days=14))
        postgreSQL_select_Query1 += "and it.channel in ('online_payment_recovery') and it.status ='SUCCESS' and paymentsource in ('payment_processor') order by m.tradingname,i.invoicedate, c.firstname, c.lastname"
        cursor.execute(postgreSQL_select_Query1)
        AIFquerycount=cursor.rowcount
        print(cursor.rowcount)
        AIFpaynowreport = cursor.fetchall() 
        csvTitle = ["tradingname", "firstname", "lastname","Reference No.", "invoiceid", "invoicenumber", "invoicedate", "billattempts", "status", "amount", "transactioncreatedon"]
        populateAIFCSV(AIFpaynowreport,csvTitle)

    except (Exception, psycopg2.Error) as error :
        print ("Error while fetching data from PostgreSQL - ", error)
    finally:
    #closing database connection.
        if(connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

 
def populatePFCSV(resultSet,csvTitle):
    # Write result to file.
    csv_filenameHead = str(date.today())+" for invoices from "+str(date.today()-timedelta(days=14))+" PFpaynowreport"+".csv"
    csv_file_path = join(current_dir, "PFresults\\") +csv_filenameHead

    with open(csv_file_path, 'w', newline='',encoding='utf-8-sig') as csvfile:
        csvwriter = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csvwriter.writerow(csvTitle)
        for row in resultSet:
            csvwriter.writerow(row)
    logging.info('File generated')
    PFcsv_fileList.append(csv_filenameHead)


def populateAIFCSV(resultSet,csvTitle):
    # Write result to file.
    csv_filenameAIFHead = str(date.today())+" for invoices from "+str(date.today()-timedelta(days=14))+" AIFpaynowreport"+".csv"
    csv_file_path = join(current_dir, "AIFresults\\") +csv_filenameAIFHead

    with open(csv_file_path, 'w', newline='',encoding='utf-8-sig') as csvfile:
        csvwriter = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csvwriter.writerow(csvTitle)
        for row in resultSet:
            csvwriter.writerow(row)
    logging.info('File generated')
    AIFcsv_fileList.append(csv_filenameAIFHead)



def sendPFEmail():
    try:
        msg = MIMEMultipart()
        msg["Subject"] = PFemailmessage + str(date.today())
        msg["From"] = str(fromAddr)
        msg["To"] = PFtoAddr
        body = MIMEText(PFemailmessage + " from " +str(date.today()-timedelta(days=14)) + "\n\n" + "There are " + str(PFquerycount) + " record(s)")
        msg.attach(body)


        #file attachment

        for file in PFcsv_fileList:
            csv_file_path = join(current_dir, "PFresults\\") +file
            part = MIMEBase('application', "octet-stream")
            part.set_payload(open(csv_file_path, "rb").read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', 'attachment', filename=file)  
            msg.attach(part)

        smtp = smtplib.SMTP(smtpServer, 25)
        smtp.sendmail(msg["From"], msg["To"].split(","), msg.as_string())
        smtp.quit()
    except (Exception, smtplib.SMTPException) as error :
        print ("Error while sending email - ", error)
    print ('Completed...PF email sent')




def sendAIFEmail():
    try:
        msg = MIMEMultipart()
        msg["Subject"] = AIFemailmessage + str(date.today())
        msg["From"] = str(fromAddr)
        msg["To"] = AIFtoAddr


        #file attachment

        for file in AIFcsv_fileList:
            csv_file_path = join(current_dir, "AIFresults\\") +file
            part = MIMEBase('application', "octet-stream")
            part.set_payload(open(csv_file_path, "rb").read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', 'attachment', filename=file)  
            msg.attach(part)

        smtp = smtplib.SMTP(smtpServer, 25)
        smtp.sendmail(msg["From"], msg["To"].split(","), msg.as_string())
        smtp.quit()
    except (Exception, smtplib.SMTPException) as error :
        print ("Error while sending email - ", error)
    print ('Completed...AIF email sent')

if __name__ == "__main__":
        PFpaynowreport()
        sendPFEmail()
        if date.today().weekday() == 0:
            AIFpaynowreport()
            sendAIFEmail()

