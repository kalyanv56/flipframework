
import os,sys
import json
import psycopg2
import datetime
import datetime, time
import commands
import logging
import smtplib
import base64
import subprocess
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.MIMEBase import MIMEBase
from email.mime.text import MIMEText
from email import Encoders
import ConfigParser
import re
#import ebcdic

g_test_var = 'test_phase'
g_func_var = 'reporting.fileload'


#############################################################################################################################
# /                                                                                                                         /#
# /     validateEmail :  Validates all the email id in the property file to check whether they are active.                  /#
# /                                                                                                                         /#
# /	                                                                                                                   /#
# /              Retruns a list of valid emails                                                                             /#
# /                                                                                                                         /#
# /                                                                                                                         /#
# /                                                                                                                         /#
# /	                                                                                                                   /#
#############################################################################################################################


def validateEmail(SERVER, validLst):
    host = SERVER
    emailProp = '/home/phdftpuser/%s/project/config/Email.properties' % (g_test_var)
    config = ConfigParser.RawConfigParser()
    config.read(emailProp)
    emailLst = config.get('Email Header', 'email.id').split(',')
    server = smtplib.SMTP()
    server.set_debuglevel(0)
    server.connect(host)
    server.helo(host)
    for i in emailLst:
        server.mail(i)
        code, msg = server.rcpt(str(i))
        if code == 250:
            validLst.append(i)
    server.quit()
    return validLst


#############################################################################################################################
# /                                                                                                                         /#
# /     sendErromail :  Sends the email to the list of active users with the Error and Debug information.                   /#
# /                                                                                                                         /#
# /	                                                                                                                   /#
# /              Retruns nothing                                                                                            /#
# /                                                                                                                         /#
# /                                                                                                                         /#
# /                                                                                                                         /#
# /	                                                                                                                   /#
#############################################################################################################################


def sendErromail(errInfo, job, process):
    print 'Email Started'
    today = datetime.datetime.now().date()
    SERVER = "mailhost.waddell.com"
    BODY = errInfo
    address_book = []
    SUBJECT = ''
    if process == '':
        SUBJECT = 'Tasked Failed -- %s in TEST for process on %s' % (job, str(today))
    else:
        SUBJECT = 'Tasked Failed -- %s in TEST @ %s on %s' % (job, process, str(today))
    address_book = validateEmail(SERVER, address_book)
    FROM = "HDFSSupportTeam@waddell.com"
    TO = ','.join(address_book)
    msg = MIMEMultipart()
    msg['Subject'] = SUBJECT
    msg['From'] = FROM
    msg['To'] = TO
    msg.preamble = 'Multipart massage.\n'
    msg.attach(MIMEText(BODY, 'plain'))
    try:
        server = smtplib.SMTP(SERVER)
        server.sendmail(FROM, address_book, msg.as_string())
        print 'Message Sent'
    except SMTPEXCEPTION:
        print "Error: unable to send Error email with attachment"


#############################################################################################################################
# /                                                                                                                         /#
# /     getConn : fetch the property information from the configuration file and returns .                                  /#
# /                                                                                                                         /#
# /	Retruns host,database,hadoop environment location and network drive environment locations.                         /#
# /                                                                                                                         /#
# /                                                                                                                         /#
#############################################################################################################################


def getConn():
    connProp = '/home/phdftpuser/%s/project/config/host.properties' % (g_test_var)
    config = ConfigParser.RawConfigParser()
    config.read(connProp)
    connLst = config.get('Host Property', 'Host.id').split(',')
    connLst = ','.join(connLst)
    return connLst.split(',')


#############################################################################################################################
# /                                                                                                                         /#
# /     readLayout : Reads the json layout for each key and returns the keys and value for the flags.                       /#
# /                                                                                                                         /#
# /	Retruns keys and value for the flags                                                                               /#
# /                   sequence                                                                                              /#
# /                   sequence_ext                                                                                          /#
# /                   seq_ident_char                                                                                        /#
# /                   sec_col_Flag                                                                                          /#
# /	             record_flag                                                                                           /#
# /                                                                                                                         /#
#############################################################################################################################


def readLayout(load):
    recordFlush = ''
    recordSeq = []
    follw_Rcd_Chars = ''
    recordSeq_ext = ''
    sec_ord = ''
    for key, value in load.items():
        for v in value:
            if "record_flag" in v.keys():
                if v["record_flag"] == 'True' and "sec_col_Flag" not in v.keys():
                    recordFlush = key
                if "sec_col_Flag" in v.keys() and v["record_flag"] == 'True':
                    sec_ord = key
            if "sequence" in v.keys():
                if v["sequence"] == 'True':
                    recordSeq.append(key)
            if "sequence_ext" in v.keys():
                if v["sequence_ext"] == 'True':
                    recordSeq_ext = key
                    follw_Rcd_Chars = v["seq_ident_char"]
    return recordFlush, recordSeq, recordSeq_ext, sec_ord, follw_Rcd_Chars


##############################################################################################################################
# /                                                                                                                          /#
# /                                                                                                                          /#
# /         populateNulls :  populates nulls for the record that doesn't exist in the input file.                            /#
# /                                                                                                                          /#
# /                 Returns   pipe delimited null string                                                                     /#
# /                                                                                                                          /#
##############################################################################################################################


def populateNulls(item, json_load):
    res = ''
    for i in range(1, len(json_load["db_prop"][0]["columns"][0][item].split('|')) + 1):
        res = res + 'null|'
    return res


##############################################################################################################################
# /                                                                                                                          /#
# /                                                                                                                          /#
# /         moveToHDFS :  copies the processed file from the network wip drive to HDFS.                                      /#
# /                                                                                                                          /#
# /                 Returns   None                                                                                           /#
# /                                                                                                                          /#
##############################################################################################################################


def moveToHDFS(progJobPath, jobName, table, g_hdp_loc, nw_Drive):
    print 'end time' + str(datetime.datetime.now())
    fleExst = "/%s/%s/%s.txt" % (g_hdp_loc, jobName, table)
    print fleExst
    res = rmrHdfs(fleExst)
    print res
    fle_outLoc = "/%s/%s/wip/%s.txt" % (nw_Drive, progJobPath, table)
    print fle_outLoc
    fleToHdp = "%s /%s/%s/" % (fle_outLoc, g_hdp_loc, jobName)
    print fleToHdp
    putHdfs(fleToHdp)
    os.remove(fle_outLoc)


def testHdfs(absPth):
    fleExst = "hadoop fs -test -e %s" % (absPth)
    return subprocess.call(fleExst, shell=True)


def rmrHdfs(absPth):
    # fleExst="hadoop fs -test -e %s"%(absPth)
    rtrncd = testHdfs(absPth)
    if int(rtrncd) == 0:
        hadoopfsjob = "hadoop fs -rmr %s" % (absPth)
        os.system(hadoopfsjob)
    return rtrncd


def catHdfs(absPth):
    hadoopFsJob = "hadoop fs -cat %s" % (absPth)
    rsltSt = commands.getoutput(hadoopFsJob)
    return rsltSt


def putHdfs(absPth):
    fleToHdp = "hdfs dfs -put %s" % (absPth)
    os.system(fleToHdp)


def fleCheck(absPth, prcFle, mode):
    if mode == 'r':
        if os.path.isfile(absPth):
            print "%s File Exists, loading the file" % (prcFle)
            fle2Read = open(absPth, mode)
        else:
            print "The %s you loaded doesn't exist in the location please recheck the location" % (prcFle)
            errMsg = "The %s you loaded doesn't exist in the location please recheck the location" % (prcFle)
            deMsg = "Please check the if %s exists for the process in the location -- %s" % (prcFle, absPth)
            fnlStrg = errMsg + '\n' + deMsg
            sendErromail(fnlStrg, '%s doesnt exsist' % (prcFle), '')
            sys.exit(1)
    else:
        try:
            if os.path.isfile(absPth) and mode == 'a+':
                fle2Read = open(absPth, mode)
            elif os.path.isfile(absPth) and mode == 'w':
                os.remove(absPth)
                fle2Read = open(absPth, mode)
            else:
                fle2Read = open(absPth, 'w')
        except IOError as e:
            print "cannot open file ({0}): {1}".format(e.errno, e.strerror)
            errMsg = "cannot open file ({0}): {1}".format(e.errno, e.strerror)
            deMsg = "Please Check the filepath: %s" % (absPth)
            fnlStrg = errMsg + '\n' + deMsg
            sendErromail(fnlStrg, '%s doesnt exsist' % (prcFle), '')
            sys.exit(1)
    return fle2Read


def tmeStmpCheck(fleSzeChk, tsOutput, curTSTMp, table):
    if fleSzeChk:
        print 'Time log is empty'
    else:
        for dte in tsOutput:
            if curTSTMp == dte.strip():
                print 'Process %s' % (table)
                print 'This file was already processed for timestamp in logs %s,program exiting' % (curTSTMp)
                errMsg = "This file was already processed for timestamp in logs -%s,program exiting @ %s " % (
                curTSTMp, str(datetime.datetime.now()))
                deMsg = "Please check the input file passed"
                fnlStrg = errMsg + '\n' + deMsg
                sendErromail(fnlStrg, 'Input File Parsed Already', table)
                sys.exit(1)
    return 'Timestamp Check completed'


##############################################################################################################################
# /                                                                                                                          /#
# /      flushOutput :  flush each of the records in the dictionaries[dict,seqDict] into output file.                        /#
# /                                                                                                                          /#
# /      Note:                                                                                                               /#
# /        Whenever there are records in the seqDict ,while loop executes till cntr is less than length of seqDict.          /#
# /        This method will call populateNulls() method to populate nulls for the keys that doesn't exist in dictionaries.   /#
# /                                                                                                                          /#
# /        Returns res_cnt for the count debugging purpose.                                                                  /#
# /                                                                                                                          /#
# /                                                                                                                          /#
##############################################################################################################################


def flushOutput(as_of_dt,co_Id, bus_flag, dict, seqDict, seq_ext, res_cnt, json_load, seq, op, proc_id, insrt_ts, sec_ord,
                rec_id, rec_id_fl):
    output = ''
    if sec_ord in dict.keys():
        col_order = json_load["db_prop"][0]["sec_col_order"]
    else:
        col_order = json_load["db_prop"][0]["col_order"]
    cnt, lent = 1, 1
    if bool(seqDict):
        for i in seqDict.keys():
            if lent == int(i.split('_')[1]):
                lent = lent
            elif lent < int(i.split('_')[1]):
                lent = int(i.split('_')[1])
            else:
                lent = lent
    while True:
        output = ''
        if cnt > lent:
            break
        for cols in col_order:
            if cols in dict.keys():
                output = output + dict[cols]
            elif cols in seq:
                if cols + '_' + str(cnt) in seqDict.keys():
                    output = output + seqDict[cols + '_' + str(cnt)]
                else:
                    output = output + populateNulls(cols, json_load)
            else:
                output = output + populateNulls(cols, json_load)
        cnt = cnt + 1
        rec_id = rec_id + 1
        if  "cmpny_id_fl" in json_load["db_prop"][0].keys():
            output = co_Id+'|'+output+proc_id + '|' + insrt_ts
        else:
            output = output+proc_id + '|' + insrt_ts

        if bus_flag=='True':
            if rec_id_fl=='True':
                output = str(rec_id)+'|'+as_of_dt+'|'+output +'\n'
            else:
                output = as_of_dt+'|'+output+'\n'
        else:
            output=output +'\n'
        op.write(output)
        res_cnt = res_cnt + 1

    return res_cnt, rec_id


##############################################################################################################################
# /                                                                                                                          /#
# /      parseLine : This method will take the input line and parse with respective to the json layout .                     /#
# /                                                                                                                          /#
# /      Note:                                                                                                               /#
# /        This method iterates over the columns per key in the json layout and parses out the positions and data type       /#
# /        of each column and organize the input line into pipe delimited formatted row.                                     /#
# /                                                                                                                          /#
# /      Returns formatted string of the input line.                                                                         /#
# /                                                                                                                          /#
# /        v.01 updated the code to handle the different date formats                                                        /#
# /                                                                                                                          /#
# /                                                                                                                          /#
##############################################################################################################################


def parseLine(item, line, json_load):
    res_st = ''
    trim_ck = False
    fd_nm_fl = False
    diff_length = 0
    expected_length = int(json_load["db_prop"][0]["len_of_lne"])
    actual_length = int(len(line))

    if expected_length > actual_length:
        diff_length = expected_length - actual_length
    else:
        diff_length = actual_length - expected_length
    for val in json_load["db_prop"][0]["columns"][0][item].split('|'):
        st = ''
        if len(json_load[item][0]["attrib"][val][0].split(',')) > 3:
            start, end, data_type, t = json_load[item][0]["attrib"][val][0].split(',')
            if t == 'strip()':
                trim_ck = True
        else:
            start, end, data_type = json_load[item][0]["attrib"][val][0].split(',')
        if fd_nm_fl == 'true':
            start = str(int(start) + diff_length)
            end = str(int(end) + diff_length)

        if len(line) != expected_length and str(val) == 'fund_nm':
            end = str(int(end) + diff_length)
            fd_nm_fl = 'true'

        line = re.sub(r'\x00', ' ', line)
        if line[int(start):int(end)].strip() == '':
            st = 'null'
        elif data_type in ('%m%d%Y', '%Y%m%d', '%y%m%d', '%Y%j', '%m%d%y', '%m/%d/%Y', '%m/%d/%y'):
            dyn_Zero_flr = '0'
            from datetime import datetime
            if line[int(start):int(end)].strip() != dyn_Zero_flr.zfill(len(line[int(start):int(end)].strip())) and line[
                                                                                                                   int(
                                                                                                                           start):int(
                                                                                                                           start) + 4].strip() != '0000':  # dyn_Zero_flr.zfill(int(end)-int(start)):
                st = str(datetime.strptime(line[int(start):int(end)], data_type).date())
            else:
                st = 'null'
        elif data_type == 'int':
            if line[int(start):int(end)].strip() != '':
                st = line[int(start):int(end)].strip()
            else:
                st = 'null'
        elif data_type.startswith('decimal'):

            pre, sca = data_type.split('(')[1].split('|')
            sca = int(sca.replace(')', ''))
            if len(line[int(start):int(end)]) < int(pre):
                st = line[int(start):int(end)].zfill(int(pre))

            st = line[int(start):int(end) - sca] + '.' + line[int(end) - sca:int(end)]

        elif data_type == 'null':
            st = 'null'
        elif data_type.startswith('Sdecimal'):
            pre, sca = data_type.split('(')[1].split('|')
            sca = int(sca.replace(')', ''))
            intr_Reslt = line[int(start):int(end)]
            spcl_Chr = re.findall(r'[^0-9:]', intr_Reslt)
            for chr in spcl_Chr:
                indx = intr_Reslt.index(chr)
                chr_Conv = chr.encode('cp1140').encode("hex")
                if chr_Conv[0:1] == 'c':
                    intr_Reslt = intr_Reslt.replace(chr, chr_Conv[-1])
                    intr_Reslt = '+' + intr_Reslt[0:int(pre) - sca] + '.' + intr_Reslt[int(pre) - sca:]
                elif chr_Conv[0:1] == 'd':
                    intr_Reslt = intr_Reslt.replace(chr, chr_Conv[-1])
                    intr_Reslt = '-' + intr_Reslt[0:int(pre) - sca] + '.' + intr_Reslt[int(pre) - sca:]
                else:
                    intr_Reslt = intr_Reslt.replace(chr, chr_Conv[-1])
                    intr_Reslt = intr_Reslt[0:int(pre) - sca] + '.' + intr_Reslt[int(pre) - sca:]
            st = intr_Reslt

        else:
            st = line[int(start):int(end)].strip().replace('|', '').replace('\\', '\\\\')

        if trim_ck:
            exec "st = '%s'.%s" % (st, t)
            trim_ck = False
        res_st = res_st + st + '|'
    return res_st


##############################################################################################################################
# /                                                                                                                          /#
# /      process : This method is the decision maker and core of this program it will identify the lines that needs to       /#
# /                be ignored and flush the result set processed.                                                            /#
# /      Note:                                                                                                               /#
# /        This method have all the dictioanries initialization to keep the record set that needs to be flushed every time   /#
# /        it sees the flush item (which mean start of the record set [CIA,CIB,CIC],[CIA,CIB] which is "CIA" in this case).  /#
# /        Line#343 : This method has the logic to identify the sequence flag enabled keys and route them                    /#
# /                   appropriate handling process.                                                                          /#
# /        There are many flags that will help the script to navigate the line to parseLine method for further               /#
# /        transformations.                                                                                                  /#
# /        This method will call the parseLine and flush method to transform and the push the line to output file.           /#
# /        "mul_seq_Cnt" will store the cntr for the key in case of sequence  and update for each occurence of the key       /#
# /                                                                                                                          /#
# /     Returns  the count of lines written to the file and timestamp from file for log                                      /#
# /                                                                                                                          /#
# /                                                                                                                          /#
# /                                                                                                                          /#
##############################################################################################################################


def process(json_load, inp, op, tsOutput, fleSzeChk, table):
    print 'entered in to process'
    flush, seq, seq_ext, sec_ord, follw_Rcd_Chars = readLayout(json_load)
    cnt, seq_cnt, res_cnt, rec_id = 0, 0, 0, 0
    proc_id = json_load["db_prop"][0]["hdfs_proc_id"]
    insrt_ts = str(datetime.datetime.now())
    as_of_dt,co_Id = '',''
    bus_flag = 'False'
    if "bus_proc_dt" in json_load["db_prop"][0].keys():
        bus_flag = json_load["db_prop"][0]["bus_proc_dt"]
    if "cmpny_id_fl" in json_load["db_prop"][0].keys():
        coLneStrts,coStrt,coEnd,cmpny_id_fl=json_load["db_prop"][0]["cmpny_id_fl"][0].split(',')
    rec_id_fl = 'False'
    mul_seq_Cnt = {}
    dflt_Key = ''
    len_of_lne = int(json_load["db_prop"][0]["len_of_lne"])
    lsPos, lePos = json_load["db_prop"][0]["line_default_char"][0].split(',')
    frstDict, seqDict = {}, {}
    hdrLne, sPos, ePos = json_load["db_prop"][0]["ts_valid"][0].split(',')
    trLr = json_load["Trailer"][0]["Seq"]
    print 'start time' + str(datetime.datetime.now())
    isPos, iePos = 0, 0
    item, recrd_ext = '', ''
    for line in inp:
        # if re.search('[\xAE]',line) :
        # print 'reached at AE'
        # line = re.sub('[\xAE]',' ',line)
        # line = re.sub(r'[^\x01-\x7F]','',line)
        # else:

        # line = re.sub(r'[^\x01-\x7F]',' ',line)
        # line = re.sub(r'[^\x00-\x7F]','i',line)

        offDiff = len(line) - len_of_lne
        prev_Indx = None
        uni_Chr = re.findall(r'[^\x01-\x7F]|[\xAE]', line)
        if len(uni_Chr) != 0:
            for chr in uni_Chr:
                chr_Indx = line.index(chr)
                if "encode_flag" in json_load["db_prop"][0].keys():
                    if offDiff != 0 and prev_Indx != chr_Indx:
                        line = ''.join(line[0:chr_Indx] + line[chr_Indx + 1:])
                    else:
                        line = ''.join(line[0:chr_Indx] + ' ' + line[chr_Indx + 1:])
                    prev_Indx = chr_Indx
                    offDiff -= 1
                else:
                    line = ''.join(line[0:chr_Indx] + ' ' + line[chr_Indx + 1:])
        if 'no_rec_typ' in json_load["db_prop"][0].keys() and not line.startswith(hdrLne) and not line.startswith(trLr):
            dflt_Key = flush
        line = line.rstrip().ljust(len_of_lne) + dflt_Key
        if line.strip().startswith(hdrLne):
            timeStamp = line[int(sPos):int(ePos)].strip()
            tsCheck = tmeStmpCheck(fleSzeChk, tsOutput, timeStamp, table)
        if bus_flag == 'True':
            lne, strt_dt, end_dt, rec_id_fl = json_load["db_prop"][0]["as_of_dt"][0].split(',')
            if (line.strip().startswith(lne.strip())):
                if (int(end_dt) - int(strt_dt)) == 8:
                    line = line.strip()
                    if line.startswith('FH'):
                        as_of_dt = line[int(end_dt) - 4:int(end_dt)] + '-' + line[int(strt_dt):int(
                            strt_dt) + 2] + '-' + line[int(strt_dt) + 2:int(strt_dt) + 4]
                    else:
                        as_of_dt = line[int(strt_dt):int(strt_dt) + 4] + '-' + line[int(strt_dt) + 4:int(
                            strt_dt) + 6] + '-' + line[int(strt_dt) + 6:int(end_dt)]

                else:
                    as_of_dt = line[int(end_dt) - 4:int(end_dt)] + '-' + line[
                                                                         int(strt_dt):int(strt_dt) + 2] + '-' + line[
                                                                                                                int(
                                                                                                                    strt_dt) + 3:int(
                                                                                                                    strt_dt) + 5]
        if cmpny_id_fl=='True' and line.startswith(coLneStrts):
            co_Id=line[int(coStrt):int(coEnd)]
        recrd_ext = line[int(lsPos):int(lePos)] + line[int(isPos):int(iePos)]
        if (((line.startswith(flush) or line[int(lsPos):int(lePos)] == flush or recrd_ext == flush) or (
                line.startswith(sec_ord) and sec_ord != '')) or (
                    line.startswith(trLr) or trLr == 'default')) and cnt != 0:
            if len(frstDict.keys()) != 0:
                res_cnt, rec_id = flushOutput(as_of_dt,co_Id,bus_flag, frstDict, seqDict, seq_ext, res_cnt, json_load, seq,
                                              op, proc_id, insrt_ts, sec_ord, rec_id, rec_id_fl)
                frstDict, seqDict = {}, {}
                mul_seq_Cnt = {}
            if line.startswith(trLr):
                cnt, rec_id = 0, 0

        if line[int(lsPos):int(lePos)] in json_load.keys():
            item = line[int(lsPos):int(lePos)]
            if "seq_ident_FLg" in json_load[item][0].keys():
                isPos, iePos = json_load[item][0]["seq_ident_pos"][0].split(',')
            else:
                isPos, iePos = 0, 0

            if line[int(lsPos):int(lePos)] + line[int(isPos):int(iePos)].strip() in json_load.keys():
                item = line[int(lsPos):int(lePos)] + line[int(isPos):int(iePos)].strip()
            else:
                item = item
            if len(seq) == 0 and "attrib" in json_load[item][0].keys():
                result = parseLine(item, line, json_load)
            if len(seq) != 0 and item not in seq:
                result = parseLine(item, line, json_load)
            if item in seq:
                iChars = json_load[item][0]["seq_ident_char"]
                if line[int(isPos):int(iePos)].strip() in iChars or 'default' in iChars:
                    if item not in mul_seq_Cnt:
                        mul_seq_Cnt[item] = 0
                    result = parseLine(item, line, json_load)
                    mul_seq_Cnt[item] = mul_seq_Cnt[item] + 1
                if line[int(isPos):int(iePos)].strip() in iChars or 'default' in iChars:
                    seqDict[item + '_' + str(mul_seq_Cnt[item])] = result
            else:
                if "attrib" in json_load[item][0].keys():
                    if "mul_recrd_frmt" in json_load[item][0].keys():
                        frstDict[item[int(lsPos):int(lePos)]] = result
                    else:
                        frstDict[item] = result
            if flush == item:
                cnt = cnt + 1
    return res_cnt, timeStamp


##############################################################################################################################
# /                                                                                                                          /#
# /      pigprocess : This process is called for the input that has single record type and is more than a gigabyte,the input /#
# /                is copied to HDFS location first and uses pig to process the file.                                        /#
# /      Note:                                                                                                               /#
# /        This method will create pigscripts in the environment(dev,test..etc) project/<project name>/pigscript/folder with /#
# /        the appropriate script with necessary parameters like mapping will be written to this scripts.                    /#
# /                                                                                                                          /#
# /        There is pyudf(parse.py) developed for this process under (/<environment>/project/framework/) that takes mapping  /#
# /        as parameter from the script and pass it to the udf to return a formatted string.                                 /#
# /                                                                                                                          /#
# /        There are 2 pigscripts involved in this process ,the first will fetch the as_of_dt from the file and return to a  /#
# /        file.The later script will send unformatted lines to pyudf to do data cleansing and returns a formatted line      /#
# /        with date from the previous pig script result appended to it.                                                     /#
# /                                                                                                                          /#
# /     Returns  the count of lines written to the file and timestamp from file for log                                      /#
# /                                                                                                                          /#
# /                                                                                                                          /#
##############################################################################################################################


def pigprocess(json_load, inp, jobName, fleNme, fle_nm_Splt, fleSzeChk, tsOutput, table, g_hdp_loc):
    flush, seq, seq_ext, sec_ord, follw_Rcd_Chars = readLayout(json_load)
    mapLst = ''
    for i in json_load['db_prop'][0]['columns'][0][flush].split('|'):
        mapLst += str(json_load[flush][0]['attrib'][i]) + '||'
    mapLst = mapLst.replace("'", '').replace('u', '')
    print 'mapLst string is', mapLst
    len_of_lne = int(json_load['db_prop'][0]["len_of_lne"])
    procId = json_load["db_prop"][0]["hdfs_proc_id"]
    insrt_ts = str(datetime.datetime.now())
    if json_load["db_prop"][0]["bus_proc_dt"] == 'True':
        as_of_dt = str(json_load["db_prop"][0]["as_of_dt"]).replace("'", '').replace('u', '')
    else:
        as_of_dt = str(datetime.date.today())
    print 'as_of_dt before transformation', as_of_dt
    hLne = as_of_dt.replace('[', '').replace(']', '').replace('u', '').split(',')[0]
    tLne = json_load["Trailer"][0]["Seq"]
    tsStmp = str(json_load["db_prop"][0]["ts_valid"]).replace("'", '').replace('u', '')
    # print 'as_of_dt after transformation',as_of_dt.replace('[','').replace(']','').split(',')
    # print 'tsstamp after transformations',tsStmp.replace('[','').replace(']','').split(',')

    fleTsExst = "/%s/%s/datetime/%s_validation" % (g_hdp_loc, jobName, fle_nm_Splt)
    tsExstres = rmrHdfs(fleTsExst)
    fleinHdp = "/%s/%s/inbound/%s" % (g_hdp_loc, jobName, fleNme)
    inHdpres = rmrHdfs(fleinHdp)
    fleToHdp = "%s /%s/%s/inbound/" % (inp, g_hdp_loc, jobName)
    print fleToHdp
    putHdfs(fleToHdp)

    ######################################################################################
    # /                                                                                  /#
    # /                                                                                  /#
    # /         Pig script->1 fetch the as_of_dt and valid timestamp on the file.        /#
    # /                                                                                  /#
    ######################################################################################

    pigReg = "register /home/phdftpuser/%s/project/framework/udf/parse.py using jython as my_special_udfs; \n" % (
        g_test_var)
    pigLine1 = "A = LOAD '/%s/%s/inbound/%s'" % (g_hdp_loc, jobName, fleNme) + "AS (line:chararray);\n"
    pigLine2 = "B = FILTER A BY SUBSTRING(line,0,3)=='%s';\n" % (hLne)
    pigLine3 = "C = FOREACH B GENERATE my_special_udfs.fetchAs_of_dt(line,'%s','%s');\n" % (as_of_dt, tsStmp)
    pigLine4 = "STORE C INTO '/%s/%s/datetime/%s_validation';" % (g_hdp_loc, jobName, fle_nm_Splt)

    tmStmpFile = '/home/phdftpuser/%s/project/%s/pigscript/%s_getdate.pig' % (g_test_var, jobName, fle_nm_Splt)
    prcFle = 'pig timestamp script'
    mode = 'w'
    pigJobFile = fleCheck(tmStmpFile, prcFle, mode)
    pigJobFile.write(pigReg + pigLine1 + pigLine2 + pigLine3 + pigLine4)
    pigJobFile.close()
    pigjob = "pig %s" % (tmStmpFile)
    os.system(pigjob)
    hadoopFsJob = "/%s/%s/datetime/%s_validation/part*" % (g_hdp_loc, jobName, fle_nm_Splt)
    rsltSt = catHdfs(hadoopFsJob)
    print rsltSt.split('|')
    curTSTMp = rsltSt.split('|')[1]
    tsCheck = tmeStmpCheck(fleSzeChk, tsOutput, curTSTMp, table)

    fleTblExst = "/%s/%s/%s.txt" % (g_hdp_loc, jobName, table)
    tblExstres = rmrHdfs(fleTblExst)
    fleCntExst = "/%s/%s/count/%s_count" % (g_hdp_loc, jobName, table)
    cntExstres = rmrHdfs(fleCntExst)

    ######################################################################################
    # /                                                                                  /#
    # /                                                                                  /#
    # /  Pig script->2 Process the input lines and stores the formatted lines and count. /#
    # /                                                                                  /#
    ######################################################################################

    pigprcReg = "register /home/phdftpuser/%s/project/framework/udf/parse.py using jython as my_special_udfs; \n" % (
        g_test_var)
    pigprcLine1 = "A = LOAD '/%s/%s/inbound/%s'" % (g_hdp_loc, jobName, fleNme) + " AS (line:chararray);\n"
    pigprcLine2 = "B = FILTER A BY SUBSTRING(line,0,3)!='%s' AND SUBSTRING(line,0,3)!='%s';\n" % (hLne, tLne)
    pigprcLine3 = "C = FOREACH B GENERATE my_special_udfs.lneProcess(line,'%s','%s','%s','%s','%s');\n" % (
    mapLst, procId, len_of_lne, insrt_ts, rsltSt.split('|')[0])
    pigprcLine4 = "STORE C INTO '/%s/%s/%s.txt' USING PigStorage();\n" % (g_hdp_loc, jobName, table)
    pigprcLine5 = "D = FOREACH (GROUP C ALL) GENERATE COUNT(C);\n"
    pigprcLine6 = "STORE D INTO '/%s/%s/count/%s_count' USING PigStorage();\n" % (g_hdp_loc, jobName, table)

    pigScrpFile = '/home/phdftpuser/%s/project/%s/pigscript/%s_prcfile.pig' % (g_test_var, jobName, fle_nm_Splt)
    prcFle = 'pig prasing script'
    mode = 'w'
    pigprcFile = fleCheck(pigScrpFile, prcFle, mode)
    pigprcFile.write(pigprcReg + pigprcLine1 + pigprcLine2 + pigprcLine3 + pigprcLine4 + pigprcLine5 + pigprcLine6)

    pigprcFile.close()
    pigprcjob = "pig -M %s" % (pigScrpFile)
    os.system(pigprcjob)
    fleTblExst = "/%s/%s/%s.txt" % (g_hdp_loc, jobName, table)
    rtrnCd = testHdfs(fleTblExst)
    print rtrnCd
    if int(rtrnCd) == 1:
        fnlStrg = 'Pig Process failed please check the pig log'
        sendErromail(fnlStrg, 'Pig Process Failed', '')
        sys.exit(1)
    hadoopFscntJob = "/%s/%s/count/%s_count/part*" % (g_hdp_loc, jobName, table)
    rsltCnt = catHdfs(hadoopFscntJob)
    print 'result count is', rsltCnt
    return int(rsltCnt), rsltSt.split('|')[1]


##############################################################################################################################
# /                                                                                                                          /#
# /      validate : This method validates between the counts returned from the process and external table and inserts        /#
# /                rows into the internal table.                                                                             /#
# /      Note:                                                                                                               /#
# /         This method has the flag ts_tl_Flg which will set to "Y" when it is TL and "N" when it is TS base on the         /#
# /           operation variable and will passed as an argument to the hawq function                                         /#
# /                                                                                                                          /#
# /     Returns Nothing                                                                                                      /#
# /                                                                                                                          /#
##############################################################################################################################


def validate(conn, cur, schema, tablename, fCount, operation, func):
    cnt_Chk = 'select count(*) from %s.%s_ext' % (schema, tablename)
    if operation == 'TS':
        ts_tl = 'ts'
        ts_tl_Flg = 'N'
        insrt = "select %s('%s.%s_ext','%s.%s_%s','%s');" % (
        func, schema, tablename, schema, tablename, ts_tl, ts_tl_Flg)
    else:
        ts_tl_Flg = 'Y'
        insrt = "select %s('%s.%s_ext','%s.%s','%s');" % (func, schema, tablename, schema, tablename, ts_tl_Flg)
    try:
        cur.execute(cnt_Chk)
        ext_Cnt = cur.fetchone()[0]
        print ext_Cnt
        if ext_Cnt == fCount:
            cur.execute(insrt)
            resultSet = cur.fetchone()
            conn.commit()
        else:
            print 'counts doesnt match please check the log tables'
            errMsg = "The Counts between the program counter - %s and external table - %s count doesnt match" % (
            str(fCount), str(ext_Cnt))
            deMsg = "Please check the logs.%s table for more info" % (tablename)
            fnlStrg = errMsg + '\n' + deMsg
            sendErromail(fnlStrg, 'Counts Doesnt match', tablename)
            sys.exit(1)
        print 'resultSet is ', resultSet
        print 'insert complete'
    except psycopg2.Error, e:
        print 'Error %s' % e
        errMsg = 'Error: Database operational issue'
        deMsg = 'DEBUG:Please check the query you provided %s' % (e)
        fnlStrg = errMsg + '\n' + deMsg
        sendErromail(fnlStrg, 'Database Operational issue', '')
        sys.exit(1)


##############################################################################################################################
# /                                                                                                                          /#
# /      main : This is the start of the script where all the arguments are read and passed to each of the methods           /#
# /                process and validate.                                                                                     /#
# /      Note:                                                                                                               /#
# /            This methode has the "fle_Rmvl_Flg" flag which will tell the script to not to remove the input file as it has /#
# /               mulitple layouts to be processed.                                                                          /#
# /                                                                                                                          /#
# /                                                                                                                          /#
##############################################################################################################################


def main(arguments):
    print arguments
    srcPrp = arguments[0]
    input_file = arguments[1]
    trnSfrm = int(arguments[2])
    inu_Flnme = input_file.split('/')[-1]
    if '_' in input_file.split('/')[-1]:
        fle_nm_Splt = input_file.split('/')[-1].split('_')[0]
    else:
        fle_nm_Splt = input_file.split('/')[-1].split('.dat')[0]
    outPath = input_file.split('/')
    print str(datetime.datetime.now())
    fleSzeChk = False
    reRun = 0
    execLst = []
    connLst = []
    if len(arguments) > 3:
        if arguments[3].split('#')[0] == 'rerun':
            reRun = int(arguments[3].split('#')[-1])
        else:
            connLst = [int(x) for x in arguments[3].split('#')[-1].split(',')]
    print reRun, connLst
    flePth = '/'.join(srcPrp.split('/')[0:-2]) + '/json/'
    print 'absolute file path', flePth
    # Json pointer Property file check
    prcFle = 'pointer to Json property file'
    mode = 'r'
    jsonSrcPntr = fleCheck(srcPrp, prcFle, mode)

    for prp in jsonSrcPntr:
        flePnter = prp.split('|')[1].strip()
        if prp.split('|')[0] != 'SR':
            srNo = int(prp.split('|')[0].strip())
            if reRun != 0:
                if srNo >= reRun:
                    execLst.append(flePnter)
            elif len(connLst) != 0:
                if srNo in connLst:
                    print 'came here'
                    execLst.append(flePnter)
            else:
                execLst.append(flePnter)
    print execLst
    g_gp_host, g_gp_db, g_hdp_loc, nw_Drive = getConn()
    try:
        conn = psycopg2.connect("hostaddr=%s dbname=%s" % (g_gp_host, g_gp_db))
        conn.autocommit = True
        cur = conn.cursor()
    except  psycopg2.Error, e:
        print 'Unable to connect to the database %s' % e
        errMsg = 'Error: Database connecitivity issues -%s' % e
        deMsg = 'DEBUG:Please check the database connections'
        fnlStrg = errMsg + '\n' + deMsg
        sendErromail(fnlStrg, 'Issue in Database Connection String', '')
        sys.exit(1)
    if trnSfrm == 0:
        # Input file Check
        prcFle = 'input file on nw_drive'
        mode = 'r'
        inp = fleCheck(input_file, prcFle, mode)

    for fle in execLst:
        layout = flePth + fle
        print layout
        # Json layout check
        prcFle = 'Json Layout file'
        mode = 'r'
        layout_File = fleCheck(layout, prcFle, mode)
        json_load = json.load(layout_File)
        schema = json_load["db_prop"][0]["schema"]
        table = json_load["db_prop"][0]["table"]
        operation = json_load["db_prop"][0]["operation"]
        func = g_func_var
        if "mul_fle_Flg" in json_load["db_prop"][0].keys():
            fle_nm_Splt = table
        cnt, tmStmp = 0, ''
        m, strt, stp = 0, 0, 0
        flePath = []
        while m < (len(outPath) - 1):
            if outPath[m] == '%s' % nw_Drive:
                strt = m
            if outPath[m] == 'inbound':
                stp = m
            m += 1
        for n in range(strt + 1, stp):
            flePath.append(outPath[n])

        progJobPath = '/'.join(flePath)
        jobFileName = outPath[len(outPath) - 1].split('.')
        jobName = progJobPath.split('/')[-1].lower()

        print progJobPath, jobName

        tsValid = '/home/phdftpuser/%s/%s/logs/%s_tstmp.txt' % (g_test_var, jobName, fle_nm_Splt)
        print tsValid
        prcFle = 'timestamp log file'
        mode = 'a+'
        tsOutput = fleCheck(tsValid, prcFle, mode)
        if os.path.getsize(tsValid) == 0:
            print 'Gets here'
            fleSzeChk = True

        if trnSfrm == 0:
            # output file location on nw_drive location check
            fOutput = '/%s/%s/wip/%s.txt' % (nw_Drive, progJobPath, table)
            prcFle = 'Output file on nw_drive'
            mode = 'w'
            output = fleCheck(fOutput, prcFle, mode)
            cnt, tmStmp = process(json_load, inp, output, tsOutput, fleSzeChk, table)
            output.close()
            print cnt
            moveToHDFS(progJobPath, jobName, table, g_hdp_loc, nw_Drive)
        else:
            cnt, tmStmp = pigprocess(json_load, input_file, jobName, inu_Flnme, fle_nm_Splt, fleSzeChk, tsOutput, table,
                                     g_hdp_loc)

        validate(conn, cur, schema, table, cnt, operation, func)
        tsOutput.write(tmStmp + '\n')
        tsOutput.close()
        if trnSfrm == 0:
            inp.seek(0)
    os.remove(input_file)
    print 'file removed successfully'
    conn.commit()
    cur.close()
    conn.close()
    if trnSfrm == 0:
        inp.close()
    print 'completed'
    print str(datetime.datetime.now())


if __name__ == "__main__":
    if len(sys.argv) in (2, 3, 4, 5):
        main(sys.argv[1:])

    else:
        print "Needs an layout, input file argument and flip code either0 for mutliple record type files and use 1 for input file with single recordtype and more than a gigabyte for the program to proceed and optional rerun and list argument when program exited abnormally"
        errMsg = 'Error: Arugments error'
        deMsg = 'DEBUG:Needs an layout, input file argument for the program to proceed'
        fnlStrg = errMsg + '\n' + deMsg
        sendErromail(fnlStrg, 'Intial Arugument Error', '')
        sys.exit(1)
