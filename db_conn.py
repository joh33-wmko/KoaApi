from os.path import isfile
import yaml
import pymysql.cursors
import subprocess

class db_conn(object):
    """
    Simple database connection and query layer.  
    Define db connection params in config file.

    Config file follows yaml format and should contain one dict entry per database:
    {
        "(database name)":
        {
            "server" : (server name/ip),
            "user"   : (db username),
            "pwd"    : (db password,
            "port"   : (db server port),
            "type"   : (db type: mysql, postgresql),
        },
    }
    Inputs:
    - configFile: Filepath to yaml config file
    - configKey: Optionally define a config dict key if config is within a larger yaml file.
    """

    def __init__(self, configFile, database):

        self.error = None
        self.readonly = False
 
        #parse config file
        if not isfile(configFile):
            self.error = "DATABASE_CONFIG_ERROR"
            return None
        with open(configFile) as f: self.config = yaml.safe_load(f)
        if database not in self.config:
            self.error = "DATABASE_CONFIG_ERROR"
            return None
        self.config = self.config[database]

        self.conn = None
        self.cursor = None
        self.connect()

    def connect(self):
        """
        Connect to the specified database.  
        """

        #get db connect data
        server         = self.config["server"]
        database       = self.config["database"]
        try:
            readonlyserver = self.config["readonlyserver"]
        except:
            readonlyserver = server
        user           = self.config["user"]
        pwd            = self.config["pwd"]
        port           = int(self.config["port"]) if "port" in self.config else 0
        type           = self.config["type"]

        cmd = ["timeout", "0.5", "ping", "-c", "1", server]
        try:
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
            p.wait()
            output = p.stdout.readlines()
            if len(output) == 0:
                server = readonlyserver
                self.readonly = True
        except:
            server = readonlyserver
            self.readonly = True
        finally:
            p.stdout.close()

        #connect
        try:
            conv = pymysql.converters.conversions.copy()
            dates = ['convert_datetime','convert_timedelta','convert_date']
            for i,val in conv.items():
                if val.__name__ in dates:
                    conv[i] = str
            self.conn = pymysql.connect(user=user, password=pwd, host=server, 
                                        database=database, autocommit=True, 
                                        conv=conv)
            self.cursor = self.conn.cursor(pymysql.cursors.DictCursor)
        except Exception as e:
            print(e)
            self.conn = None
            self.error = "DATABASE_CONNECT_ERROR"

    def close(self):
        if self.conn:
            self.cursor.close()
            self.conn.close()


    def start_transaction(self):
        self.conn.autocommit(False)

    def commit_transaction(self):
        self.conn.commit()
        self.conn.autocommit(True)

    def rollback_transaction(self):
        self.conn.rollback()
        self.conn.autocommit(True)

    def query(self, query, values=False):
        """
        Executes basic query.  Determines query type and returns fetchall on
        select, otherwise rowcount on other query types.
        Returns false on any exception error.  Opens and closes a new
        connection each time.
        """

        qtype = query.strip().split()[0]
        if self.readonly and qtype.lower() in ('insert', 'update'):
            return False

        try:
            if not values:
                self.cursor.execute(query)
            else:
                self.cursor.execute(query, values)
        except Exception as e:
            print(f"ERROR executing query: {query} {values}", e)
            return []

        if qtype.lower() == "select":
            return self.cursor.fetchall()
        else:
            return self.cursor.rowcount
