import datetime as dt

def do_get_num_lev0_files(utdate, instrument, rti1Db, rti2Db):
    """
    Returns the number of lev0 files archived.

    Input utdate (yyyy-mm-dd) and instrument (HIRES), both optional

    :param utdate: The utdate to search
    :type utdate: str (YYYY-MM-DD)
    :param instrument: The instrument to search
    :type instrument: str
    :param rti1Bb: Database connection object for RTI1
    :type rti1Db: class
    :param rti2Bb: Database connection object for RTI2
    :type rti2Db: class
    :return: Dictionary with results
    :rtype: dict
    """

    output = {}
    output["status"]     = "SUCCESS"
    output["utdate"]     = utdate
    output["instrument"] = instrument
    output["message"]    = ""

    colVal = (utdate, utdate)
    query = "select koaid from koa_status where \
             date(utdatetime)>=%s and date(utdatetime)<=%s and \
             status='COMPLETE'"

    if instrument != "":
        query = f"{query} and instrument=%s"
        colVal = colVal + (instrument,)

    result1 = rti1Db.query(query, colVal)
    result2 = rti2Db.query(query, colVal)

    num = len(result1) + len(result2)

    output["num_lev0_files"] = num

    return output

