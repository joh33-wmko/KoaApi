from flask import Flask, jsonify, request
import datetime as dt
import json
from koa_functions import *
import db_conn
from os.path import isfile
import requests
from socket import gethostname
import yaml

app = Flask(__name__)

def do_api_call(classCall):
    """
    Entry point for all routes
    """

    schedule = Koa(classCall)
    output = schedule.output
    del schedule
    return output

#---------------------------------------
# KOA Class
#---------------------------------------

class Koa:
    def __init__(self, classCall):
        """
        Initialize the class.  Instanciated from do_api_call().

        :param classCall: The route/function to execute.
        :type classCall: str
        :return: Nothing
        :rtyp: Nothing
        """

        if not isfile("config.live.ini"):
            print("Config file does not exist")
            return
        with open("config.live.ini") as f:
            self.config = yaml.safe_load(f)

        self.output = [{"status":"ERROR"}]

        # Connect to DB...            
        self.rti1Db = db_conn.db_conn("config.live.ini", "rti1")
        if self.rti1Db.error:
            self.output[0]["message"] = self.rti1Db.error
            return

        self.rti2Db = db_conn.db_conn("config.live.ini", "rti2")
        if self.rti2Db.error:
            self.output[0]["message"] = self.rti2Db.error
            self.rti1Db.close()
            return

        # Execute the specified route
        call = getattr(self, classCall)
        self.output = call()

        # Close the database connection
        self.rti1Db.close()
        self.rti2Db.close()

    def get_num_lev0_files(self):
        """
        Returns the number of lev0 files archived.

        Input utdate (yyyy-mm-dd) and instrument (HIRES), both optional

        :param request.args: Input request arguments.
        :type request.args: Dictionary
        :return: List of dictionaries with status for all active instrument.
        :rtype: list
        """

        telNr = 0

        utdate     = request.args.get("utdate", None)
        if utdate == None:
            utdate = dt.datetime.utcnow().strftime("%Y-%m-%d")
        instrument = request.args.get("instrument", "").upper()
        
        if instrument != "" and instrument in self.config["telnr"].keys():
            telNr = self.config["telnr"][instrument]

        return do_get_num_lev0_files(utdate, instrument, self.rti1Db, self.rti2Db)

#---------------------------------------

#---------------------------------------
# KOA API Routes
#---------------------------------------

@app.route("/", methods=["GET"])
def home():
    """
    Just display an error
    """

    return {"status":"ERROR", "message":"something about usage"}

@app.route("/koa/getNumLev0Files", methods=["GET"])
def get_num_lev0_files():
    return jsonify(do_api_call("get_num_lev0_files"))

#---------------------------------------

if __name__ == "__main__":
    api = "koa"

    # Parse config file
    config = True
    if not isfile("config.live.ini"):
        print("Config file does not exist")
        config = False
    with open("config.live.ini") as f: config = yaml.safe_load(f)
    if api not in config.keys():
        prinT("API definition not in the configuration file")
        config = False
    if not config:
        exit()

    host  = gethostname()
    port  = config[api]["port"]
    debug = config[api]["debug"]

    # Start the Flask server
    app.run(host=host, port=port, debug=debug)

