#coding=utf-8
import MySQLdb
import MySQLdb.cursors
import string
from DBUtils.PooledDB import PooledDB
import datetime

class DbManager(object):

    def __init__(self,host,port,db_name,user,pwd):
        # 启动时连接池中创建的的连接数
        #'mincached': 5,
        # 连接池中最大允许创建的连接数
        #'maxcached': 20,
        conn_args = {'host': host,
                      'port': port,
                      'db':db_name,
                      'user':user,
                      'passwd':pwd,
                     'charset':'utf8',
                     'cursorclass':MySQLdb.cursors.DictCursor
                     }
        self._pool = PooledDB(MySQLdb,mincached = 5,maxcached = 20,**conn_args)

    def getConn(self):
        conn = self._pool.connection()
        cursor = conn.cursor(cursorclass = MySQLdb.cursors.DictCursor)
        cursor.execute("set names utf8mb4;")
        cursor.close()
        return  conn



_dbManager = None
def create_db_manager(host,port,db_name,user,pwd):

    global _dbManager
    if _dbManager == None:
        _dbManager = DbManager(host,port,db_name,user,pwd)
    return _dbManager



class DB(object):
    def __init__(self):
        try:
            self.conn = _dbManager.getConn()
        except MySQLdb.Error,e:
            print "MySQL error" + e.__str__()
        self.cursor = self.conn.cursor()

    def __del__(self):
        self.cursor.close()
        self.conn.close()

    #instance methods
    ###########################

    def execute(self,sql, param=None):
        """ 执行sql语句 """
        rowcount = 0
        # print sql
        # try:
        if param == None:
            rowcount = self.cursor.execute(sql)
        else:
            rowcount = self.cursor.execute(sql, param)
        return rowcount
        # except Exception,e:
        #     print '--------------Error--------'
        #     print e
        #     return 0

    def query_one(self,sql,param = None):
        """ 获取一条信息 """
        rowcount = self.cursor.execute(sql,param)
        if rowcount > 0:
            res = self.cursor.fetchone()
        else:
            res = None

        return res

    def query_all(self,sql,param = None):
        """ 获取所有信息 """
        rowcount = self.cursor.execute(sql,param)
        if rowcount > 0:
            res = self.cursor.fetchall()
        else:
            res = []
        return res


    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()



    def select(self,table_name,columns = '*',where = None):
        if isinstance(columns,(str,unicode)):
            columns_str = str(columns)
        elif isinstance(columns,(list,tuple)):
            if not columns:
                columns_str = '*'
            else:
                columns_str = ','.join(c.__str__() for c in columns)

        if isinstance(where,dict) and where:
            items = where.items()
            if len(items) == 1 and isinstance(items[0][0],(list,tuple)) and items[0][0]:
                self.execute("SELECT %s FROM %s WHERE %s in (%s)" % (columns_str,table_name,items[0],','.join(e.__str__() for e in items[1])))
            else:
                # where_str = " AND ".join(["%s='%s'" % (str(x[0]), str(x[1])) for x in where.items()])
                # self.execute("SELECT %s FROM %s WHERE %s" % (columns_str,table_name,where_str))

                sql,values = self.sql_and_values_for_dict(table_name,columns,where)
                self.execute(sql,values)
        elif isinstance(where,(str,unicode)) and where:
            self.execute("SELECT %s FROM %s WHERE %s" % (columns_str,table_name,where))
        else:
            self.execute("SELECT %s FROM %s " % (columns_str,table_name))

    #slect one
    def selectone(self,table_name,columns = '*',where = None):
        self.select(table_name,columns,where)
        return self.cursor.fetchone()

    #select
    def selectall(self,table_name,columns = '*',where = None):
        self.select(table_name,columns,where)
        return self.cursor.fetchall()

    #find by id
    def find_by_id(self,table_name,id):
        return self.query_one("select * from %s WHERE id = '%s'" %(table_name,id))

    #find by dict
    def find_id_by_dict(self,table_name,params_dic):
        return self.find_columns_by_dict(table_name,'id',params_dic)

    def sql_and_values_for_dict(self,table_name,columns,params_dic):
        inputs = ''
        index = 0
        for x in params_dic.keys():
            inputs = inputs + (" AND " if index > 0 else '') + x.__str__() + "=%s"  
            index += 1
        values = map(lambda x: str(x),params_dic.values())
        sql = u"select %s from %s WHERE %s" %(columns,table_name,inputs)
        return sql,values

    #find comumns by dict
    def find_columns_by_dict(self,table_name,columns,params_dic,query_one = True):
        sql,values = self.sql_and_values_for_dict(table_name,columns,params_dic)
        if query_one:
            return self.query_one(sql,values)
        else:
            return self.query_all(sql,values)

    #find by dict
    def find_one_by_dict(self,table_name,params_dic):
        return self.find_columns_by_dict(table_name,'*',params_dic)

    #insert
    def insert(self,table_name,params_dic,update_date = True):
        if update_date:
            now = type(self).get_datetime_string()
            params_dic.update({'created_at':now,'updated_at':now})
        keys   = string.join(params_dic.keys(),"`,`")
        inputs = ','.join(("%s",)*len(params_dic))
        values = map(lambda x: str(x),params_dic.values())
        sql = "INSERT INTO " + table_name + " (`" + keys + "`) VALUES (" + inputs + ")"
        return self.execute(sql,values)

    #update
    def update(self,table_name,params_dic, where = None,update_date = True):
        if update_date:
            params_dic.update({'updated_at':type(self).get_datetime_string()})
        edit_sql = ",".join([("%s" % (str(x)) + "=%s") for x in params_dic.keys()])
        values = map(lambda x: str(x),params_dic.values())
        where_sql = ''

        if where:
            if isinstance(where,(str,unicode)):
                where_sql = str(where)
            elif isinstance(where,dict):
                where_sql = " AND ".join([("%s" % (str(x)) + "=%s") for x in where.keys()])
                where_values = map(lambda x: str(x),where.values())
                values = values + where_values
            sql = "UPDATE %s SET %s WHERE %s" % (table_name, edit_sql,where_sql)
        else:
            sql = "UPDATE %s SET %s " % (table_name, edit_sql)

        return self.execute(sql,values)



    #delete
    def delete(self,table_name,where = None):
        where_sql = ''
        if where:
            if isinstance(where,str):
                where_sql = where
            elif isinstance(where,dict):
                where_sql = " AND ".join(["%s='%s'" % (str(x[0]), str(x[1])) for x in where.items()])
            sql_prefix = "DELETE FROM %s WHERE %s "
            sql = sql_prefix % (table_name,where_sql)
        else:
            sql_prefix = "DELETE FROM %s "
            sql = sql_prefix % (table_name)

        return self.execute(sql)


    def get_inserted_id(self):
        """
        获取当前连接最后一次插入操作[自增长]生成的id,如果没有则为０
        """
        result = self.query_all("SELECT @@IDENTITY AS id")
        if result:
            return result[0].get('id')
        return 0

    #################### classmethods
    @classmethod
    def get_datetime_string(cls):
        return datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")


    @classmethod
    def generate_code(cls):
        import time
        import random
        return '%x%x' % (int(time.time()), random.randint(1, 0x0ffff))

    @classmethod
    def generate_id(cls):
        return "n%s" % cls.generate_code()
