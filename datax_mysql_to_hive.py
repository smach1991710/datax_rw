#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import os
import time
import getopt

#配置datax目录
#datax_home_bin = '/usr/local/datax/bin'
datax_home_bin = '/home/hadoop/app/tools/datax/bin'

#配置默认生成的路径
#default_auto_gen_dir = '/usr/local/datax/job/job_history/'
default_auto_gen_dir = '/home/hadoop/app/tools/datax/job/job_history/'

#配置默认的模板文件路径
#default_template_dir = '/usr/local/datax/job/template/'
default_template_dir = '/home/hadoop/app/tools/datax/job/template/'

#配置默认的数据库文件
#default_jdbc_dir = '/usr/local/datax/job/jdbc_conf.properties'
default_jdbc_dir = '/home/hadoop/app/tools/datax/job/jdbc_conf.properties'

#开始和结束字符
start_str = '${'
end_str = '}'

class Handler:

    def __init__(self, opts):
        self.templateDir = ''
        self.templateName = ''

        self.autoGenDir = ''
        self.isIncrement = ''

        self.srcJdbcDir = ''
        self.srcJdbcCode = ''

        self.srcTableName = ''
        self.srcColName = ''
        self.srcCondition = ''
        self.srcUsername = ''
        self.srcPassword = ''
        self.srcJdbcUrl = ''
        self.srcQuerySql = ''
        self.srcSplitPk = ''

        self.destDbName = ''
        self.destTableName = ''
        self.destPartition = ''
        self.destColName = ''

        #解析获取对应的参数
        for op, value in opts:
            if op == '--templateDir':
                self.templateDir = value
            elif op == '--templateName':
                self.templateName = value
            elif op == '--srcTableName':
                self.srcTableName = value
            elif op == '--srcColName':
                self.srcColName = value
            elif op == '--srcCondition':
                self.srcCondition = value

            elif op == '--destDbName':
                self.destDbName = value
            elif op == '--destTableName':
                self.destTableName = value
            elif op == '--destColName':
                self.destColName = value
            elif op == '--destPartition':
                self.destPartition = value
            elif op == '--autoGenDir':
                self.autoGenDir = value

            elif op == '--srcUsername':
                self.srcUsername = value
            elif op == '--srcPassword':
                self.srcPassword = value
            elif op == '--srcJdbcUrl':
                self.srcJdbcUrl = value
            elif op == '--srcQuerySql':
                self.srcQuerySql = value
            elif op == '--isIncrement':
                self.isIncrement = value
            elif op == '--srcSplitPk':
                self.srcSplitPk = value
            elif op == '--srcJdbcDir':
                self.srcJdbcDir = value
            elif op == '--srcJdbcCode':
                self.srcJdbcCode = value

        if self.isIncrement == '':
            self.isIncrement = 0

        if self.autoGenDir == '':
            self.autoGenDir = default_auto_gen_dir

        if self.templateDir == '':
            self.templateDir = default_template_dir

        #读取配置文件,获取数据库连接信息
        if self.srcJdbcDir == '':
            self.srcJdbcDir = default_jdbc_dir

        if self.srcJdbcCode != '':
            #读取对应的参数信息
            self.setJdbcInfo()

    #设置对应的jdbc信息，比如用户名，密码，url等
    def setJdbcInfo(self):
        try:
            prop = getProperty(self.srcJdbcDir)

            #获取用户名，密码，url等
            self.srcUsername = prop.get(self.srcJdbcCode + '_username')
            self.srcPassword = prop.get(self.srcJdbcCode + '_password')
            self.srcJdbcUrl = prop.get(self.srcJdbcCode + '_jdbcurl')
        except Exception:
            print('读取文件:[' + self.srcJdbcDir + ']获取jdbc:[' + self.srcJdbcCode + ']异常,请检查文件!')
            sys.exit()

    #检查参数信息
    def checkArgv(self):
        if self.templateDir == '':
            print('参数[--templateDir]模板文件路径必填,输入--help显示详细信息')
            return False

        if self.templateName == '':
            print('参数[--templateName]模板文件名称必填,输入--help显示详细信息')
            return False

        if self.srcUsername == '':
            print('参数[--srcUsername]源用户名必填,输入--help显示详细信息')
            return False

        if self.srcPassword == '':
            print('参数[--srcPassword]源密码必填,输入--help显示详细信息')
            return False

        if self.srcJdbcUrl == '':
            print('参数[--srcJdbcUrl]源数据库连接必填,输入--help显示详细信息')
            return False

        #当用户配置querySql时，MysqlReader直接忽略table、column、where条件的配置
        # querySql优先级大于tab
        if self.srcQuerySql == '':
            if self.srcTableName == '':
                print('参数[--srcTableName]源表必填,输入--help显示详细信息')
                return False

            if self.srcColName == '':
                print('参数[--srcColName]源字段必填,输入--help显示详细信息')
                return False

        if self.destDbName == '':
            print('参数[--destDbName]目标数据库必填,输入--help显示详细信息')
            return False

        if self.destTableName == '':
            print('参数[--destTableName]目标表必填,输入--help显示详细信息')
            return False

        if self.destColName == '':
            print('参数[--destColName]目标字段必填,输入--help显示详细信息')
            return False

        if self.templateDir == '':
            print('参数[--templateDir]模板文件字段必填,输入--help显示详细信息')
            return False

        #判断分区信息
        if self.isIncrement == '1':
            #增量信息不能为空
            if self.destPartition == '':
                print('参数:[--destPartition]分区必填,执行增量导数,输入--help显示详细信息')
                return False

        #对目录进行检查，查看是否都存在
        if not os.path.exists(datax_home_bin):
            print('Datax脚本目录:[' + datax_home_bin + ']不存在')
            return False

        if not os.path.exists(os.path.join(self.templateDir,self.templateName)):
            print('模板文件目录:[' + os.path.join(self.templateDir,self.templateName) + ']不存在')
            return False

        if not os.path.exists(self.autoGenDir):
            os.makedirs(self.autoGenDir)

        return True


    #对模板数据进行处理
    def handle(self):
        isChecked = self.checkArgv()
        if not isChecked:
            return

        #打印配置信息
        self.printVariables()

        outputJson = os.path.join(self.autoGenDir, self.destTableName + '_' + getNowTime('%Y%m%d%H%M%S') + '.json')
        print('正在解析文件:[' + os.path.join(self.templateDir,self.templateName) + ']')

        outputFile = open(outputJson, 'w+')
        #读取文件信息
        with open(os.path.join(self.templateDir,self.templateName), 'r') as f:
            for line in f:
                self.replaceTemplate(line,outputFile)
        #关闭文件流
        outputFile.close()

        outputHive = os.path.join(self.autoGenDir,self.destTableName + '_' + getNowTime('%Y%m%d%H%M%S') + '.hql')
        try:
            #执行初始化操作
            command_init_ret = self.setup(outputHive)

            #执行run操作
            command_run_ret = self.dataxRun(outputJson)
            #执行完成后，对脚本进行清理

            if command_init_ret == 0 and command_run_ret == 0:
                print('脚本执行成功,正在对文件:[' + outputJson + ',' + outputHive + ']进行清理...')
                if os.path.exists(outputHive):
                    os.remove(outputHive)
                if os.path.exists(outputJson):
                    os.remove(outputJson)
            else:
                print('脚本执行失败,请检查文件:[' + outputJson + ',' + outputHive + ']')
        except Exception:
            print('脚本执行失败,请检查文件:[' + outputJson + ',' + outputHive + ']')

    # 替代摸板中的内容
    def replaceTemplate(self, line, outputFile):
        tail_val = line
        new_line = line

        keys = []
        while (True):
            if (start_str in tail_val > 0 and end_str in tail_val):
                # 获取开始位置和结束位置
                start = tail_val.index(start_str)
                end = tail_val.index(end_str)

                # 对字符串进行截取
                val = tail_val[start + len(start_str):end]

                if self.getValByKey(val) == '':
                    # 首先添加到未解析的key集合里面去
                    keys.append(start_str + val + end_str)
                else:
                    # 替换掉对应的字符串
                    new_line = new_line.replace(start_str + val + end_str, self.getValByKey(val))
                # 对字符串进行截取
                length = len(tail_val)
                tail_val = tail_val[end + len(end_str):length]
            else:
                break
        #输出内容到文件
        outputFile.write(new_line)
        return keys

    #在跑dataX任务之前进行初始化，比如删除hive信息
    def setup(self,hqlDir):
        if self.destDbName == '' or self.destTableName == '':
            print('目标库:[' + self.destDbName + '],目标表:[' + self.destTableName + ']都不能为空...')
            return
        dbTable = self.destDbName + "." + self.destTableName
        if self.isIncrement == '0':
            print('根据参数:[' + str(self.isIncrement) + ']执行全量导数据')
            #将表的源数据全部删掉，因为是全量
            pre_sql = 'truncate table ' + dbTable + ';'
        else:
            print('根据参数:[' + str(self.isIncrement) + ']执行增量导数据')
            if not self.destPartition == '':
                key = self.destPartition[0:self.destPartition.find('=')]
                value = self.destPartition[self.destPartition.find('=') + 1:len(self.destPartition)]

                if (not value.startswith('"')) and (not value.startswith("'")):
                    value = "'" + value + "'";
                pre_sql = 'alter table ' + dbTable + ' drop if exists partition(' + key + '=' + value + ');\nalter table ' + dbTable + ' add partition(' + key + '=' + value + ');'

        outputFile = open(hqlDir, 'w+')
        # 写文件内容
        outputFile.write(pre_sql)
        # 关闭文件流
        outputFile.close()

        print('执行hql文件:[hive -S -f ' + hqlDir + ']完成...')
        command_ret = os.system('hive -S -f ' + hqlDir)
        return command_ret



    #跑dataX任务信息
    def dataxRun(self,jsonDir):
        print('文件解析完成,写入配置文件:[' + jsonDir + ']')
        print('开始启动DataX任务...')

        #跳转到datax的bin目录
        os.chdir(datax_home_bin)
        command = 'python datax.py ' + jsonDir
        print('执行命令:' + command)
        comman_ret = os.system(command)
        return comman_ret

    #获取到配置里面的key对应的value
    def getValByKey(self,key):
        if key == 'SRC_USER':
            return self.srcUsername
        elif key == 'SRC_PWD':
            return self.srcPassword
        elif key == 'SRC_COLS':
            return self.srcColName
        elif key == 'SRC_TABLE':
            return self.srcTableName
        elif key == 'SRC_URL':
            return self.srcJdbcUrl
        elif key == 'SRC_QUERY_SQL':
            return self.srcQuerySql
        elif key == 'SRC_COND':
            return self.srcCondition
        elif key == 'SRC_SPLITPK':
            return self.srcSplitPk
        elif key == 'HDFS_FILE':
            if self.isIncrement == '0':
                return self.destDbName + '.db/' + self.destTableName
            else:
                return self.destDbName + '.db/' + self.destTableName + '/' + self.destPartition
        elif key == 'TARGET_TABLE':
            return self.destTableName
        elif key == 'TARGET_COLS':
            return self.destColName
        return ''

    #打印配置变量的信息
    def printVariables(self):
        print('---------------------(MysqlToHive配置信息如下)---------------------')
        print('\t templateDir:' + os.path.join(self.templateDir,self.templateName) + '')
        print('\t autoGenDir:' + self.autoGenDir + '')
        print('\t mysql:\n'
                    + '\t\t username:' + self.srcUsername + '\n'
                    + '\t\t password:' + self.srcPassword + '\n'
                    + '\t\t jdbcUrl:' + self.srcJdbcUrl)
        if self.srcQuerySql == '':
            print('\t\t table:' + self.srcTableName + '\n'
                    + '\t\t column:' + self.srcColName + '\n'
                    + '\t\t condition:' + self.srcCondition)
        else:
            print('------querySql:[' + self.srcQuerySql + ']')

        print('\t hive:\n'
                    + '\t\t db:' + self.destDbName + '\n'
                    + '\t\t table:' + self.destTableName + '\n'
                    + '\t\t column:' + self.destColName)

class Properties:

    def __init__(self, file_name):
        self.file_name = file_name
        self.properties = {}
        try:
            fopen = open(self.file_name, 'r')
            for line in fopen:
                line = line.strip()
                if line.find('=') > 0 and not line.startswith('#'):
                    key = line[0:line.find('=')]
                    value = line[line.find('=') + 1:len(line)]
                    self.properties[key.strip()] = value.strip()
        except Exception, e:
            raise e
        else:
            fopen.close()

    def has_key(self, key):
        return self.properties.has_key(key)

    def get(self, key, default_value=''):
        if self.properties.has_key(key):
            return self.properties[key]
        return default_value

#获取Handler处理类
def getHandler(opts):
    return Handler(opts)

def getProperty(file_name):
    return Properties(file_name)

# 获取格式化后的当前时间
def getNowTime(format):
    nowTime = time.strftime(format, time.localtime(int(time.time())))
    return nowTime

#打印参数信息
def printArgs():
    print('Options:')
    print(' --templateDir         模板文件路径,默认为[' + default_template_dir + ']')
    print(' --templateName        模板文件名称\n')

    print(' --srcJdbcDir          源数据库文件路径,默认为[' + default_jdbc_dir + ']')
    print(' --srcJdbcCode         源数据库的信息,配置后忽略srcUsername,srcPassword,srcJdbcUrl参数')
    print(' --srcUsername         源数据库用户名')
    print(' --srcPassword         源数据库密码')
    print(' --srcJdbcUrl          源数据库的URL信息')

    print(' --srcQuerySql         源表的查询语句,配置后忽略srcTableName、srcColName、srcCondition参数')
    print(' --srcTableName        源数据库的表名')
    print(' --srcColName          源表字段的名称')
    print(' --srcCondition        源表的查询条件')
    print(' --srcSplitPk          对数据分片的字段\n')

    print(' --destDbName          目标库的名称')
    print(' --destTableName       目标表的名称')
    print(' --destColName         目标表的字段')
    print(' --destPartition       目标分区的名称\n')

    print(' --autoGenDir          自动生成Json文件的路径,默认为[' + default_auto_gen_dir + ']')
    print(' --isIncrement         是否增量[0表示全量,1表示增量]')

if __name__ == '__main__':

    #获取参数信息
    opts, args = getopt.getopt(sys.argv[1:], "-h-f:-v",[
                                                      'templateDir=',
                                                      'templateName=',
                                                      'srcJdbcDir=',
                                                      'srcJdbcCode=',
                                                      'srcUsername=',
                                                      'srcPassword=',
                                                      'srcJdbcUrl=',
                                                      'srcQuerySql=',
                                                      'srcTableName=',
                                                      'srcColName=',
                                                      'srcCondition=',
                                                      'destDbName=',
                                                      'destTableName=',
                                                      'destPartition=',
                                                      'autoGenDir=',
                                                      'destColName=',
                                                      'isIncrement=',
                                                      'srcSplitPk=',
                                                      'help'])
    for op, value in opts:
        if op in ("-h", "--help"):
            printArgs()
            sys.exit()

    handler = getHandler(opts)
    handler.handle()

