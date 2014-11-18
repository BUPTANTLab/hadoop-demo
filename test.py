#!/usr/bin/python
#encoding=utf-8
import sys
import MySQLdb
import time
import base64
reload(sys)  
sys.setdefaultencoding('utf8')  

host1 = ""
user1 = ""
pass1 = ""
db1 = ""

host2 = ""
user2 = ""
pass2 = ""
db2 = ""

tablename = ("testinfo_db","device","device_db","app_traffic_db","data_connection","cell_strength_db","app_list_db","server_sel_db","gps_wifi_db")
#tablename = ("testinfo_db","testinfo_db")

def singleDB(db_name,limit):
        try:
                conn1 = MySQLdb.connect(host=host1, user=user1, passwd=pass1, db=db1 , charset="utf8" , connect_timeout=30)
                conn2 = MySQLdb.connect(host=host2, user=user2, passwd=pass2, db=db2 , charset="utf8" , connect_timeout=30)
                conn1.ping(True)
                conn2.ping(True)
        except Exception,ex:
                print time.strftime('%Y-%m-%d %X' , time.localtime(time.time())) , ex;
                print time.strftime('%Y-%m-%d %X' , time.localtime(time.time())) , "conn error"
                return False

        print time.strftime('%Y-%m-%d %X' , time.localtime(time.time())) , "Table:" + db_name + " Limit:" + str(limit)
        cursor2 = conn2.cursor()
        cursor1 = conn1.cursor()
        cursor2.execute("SET NAMES utf8")
        cursor1.execute("SET NAMES utf8")
        try:
                cursor1.execute("SELECT max(id) FROM " + db_name)
                id1 = cursor1.fetchall()
                id1 = id1[0][0]
        except Exception,ex:
                print time.strftime('%Y-%m-%d %X' , time.localtime(time.time())) , ex;
                print time.strftime('%Y-%m-%d %X' , time.localtime(time.time())) , "id1 error"
                cursor1.close()
                cursor2.close()
                conn1.close()
                conn2.close()
                return False

        try:
                sql = "desc " + db_name
                print time.strftime('%Y-%m-%d %X' , time.localtime(time.time())) , sql
                cursor2.execute(sql)
                result2 = cursor2.fetchall()
                sql = "insert into " + db_name + "("
                for des in result2:
                        sql = sql + str(des[0]) + ", "
                dbsql = sql[:-2] + ") values ("
        except Exception,ex:
                print time.strftime('%Y-%m-%d %X' , time.localtime(time.time())) , ex;
                print time.strftime('%Y-%m-%d %X' , time.localtime(time.time())) , "desc table error"
                cursor1.close()
                cursor2.close()
                conn1.close()
                conn2.close()
                return False

        while(True):
                try:
                        cursor2.execute("SELECT max(id) FROM " + db_name)
                        id2_1 = cursor2.fetchall()
                        id2 = id2_1[0][0]
                except Exception,ex:
                        print time.strftime('%Y-%m-%d %X' , time.localtime(time.time())) , ex;
                        print time.strftime('%Y-%m-%d %X' , time.localtime(time.time())) , "id2 error"
                        cursor1.close()
                        cursor2.close()
                        conn1.close()
                        conn2.close()
                        return False

                print time.strftime('%Y-%m-%d %X' , time.localtime(time.time())) , "Target:" , id1 , "Base:" , id2
                if id1 <= id2 + 10:
                        print time.strftime('%Y-%m-%d %X' , time.localtime(time.time())) , db_name , "?"
                        cursor1.close()
                        cursor2.close()
                        conn1.close()
                        conn2.close()
                        return False

                try:
                        sql = "SELECT * FROM " + db_name + " where id > " + str(id2) + " limit " + str(limit)
                        print time.strftime('%Y-%m-%d %X' , time.localtime(time.time())) , sql
                        cursor1.execute(sql)
                        result1 = cursor1.fetchall()
                except Exception,ex:
                        print time.strftime('%Y-%m-%d %X' , time.localtime(time.time())) , ex;
                        print time.strftime('%Y-%m-%d %X' , time.localtime(time.time())) , "select1 error"
                        cursor1.close()
                        cursor2.close()
                        conn1.close()
                        conn2.close()
                        return False

                try:
                        print time.strftime('%Y-%m-%d %X' , time.localtime(time.time())) , "insert"
                        for record in result1:
                                sqltmp = dbsql
                                if db_name == "testinfo_db":
                                        sqltmp = sqltmp + "'" + str(record[0]) + "', "
                                        for re in record[1:]:
                                                try:
                                                        sqltmp = sqltmp + "'" + base64.b64decode(re) + "', "
                                                except Exception,ex:
                                                        print time.strftime('%Y-%m-%d %X' , time.localtime(time.time())) , ex;
                                                        sqltmp = sqltmp + "'" + str(re) + "', "
                                else:
                                        for re in record:
                                                sqltmp = sqltmp + "'" + str(re) + "', "
                                sqltmp = sqltmp[:-2] + ")"
#                               print sqltmp
#                               exit()
                                cursor2.execute(sqltmp)
                                conn2.commit()
                except Exception,ex:
                        print time.strftime('%Y-%m-%d %X' , time.localtime(time.time())) , ex;
                        conn2.rollback()
                        print time.strftime('%Y-%m-%d %X' , time.localtime(time.time())) , "select2 error"
                        cursor1.close()
                        cursor2.close()
                        conn1.close()
                        conn2.close()
                        return False
                break

        cursor1.close()
        cursor2.close()
        conn1.close()
        conn2.close()
        return True

if __name__ == '__main__':
        while(True):
                for name in tablename:
                        while(singleDB(name,100)):
                                time.sleep(0.1)
                                continue
                        time.sleep(1)
                time.sleep(60)
