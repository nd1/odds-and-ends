'''
Load Avaya IQ report data to a MS SQL db.

Nicole Donnelly 20170530
'''

import os
import sys
import pandas as pd

from sqlalchemy import create_engine, text, types


def convert_time(x):

    # change a hh:mm:ss string to number of seconds
    times = x.split(':')
    return (360*int(times[0])+60*int(times[1]))+int(times[2])


def get_count(sql_connection):

    # get the current number of rows in the table, if it exists
    try:
        sql = text('select count(1) from call_data')
        result = sql_connection.execute(sql)
        names = []
        for row in result:
            names.append(row[0])
            current_row_count = names[0]
    except:
        current_row_count = 0

    return current_row_count


def load_data(data):

    # define variables
    # labels for the dataframe
    col_label = ['start', 'duration', 'customer', 'direction',
                 'first_routing', 'first_queue', 'disposition', 'wait',
                 'self_service', 'active', 'on_hold', 'contact_id', 'source',
                 'agent']
    # column types for the database using sqlalchemy types
    col_types = {'start': types.DateTime, 'duration': types.Integer,
                 'customer': types.String(32), 'direction': types.String(32),
                 'first_routing': types.String(128),
                 'first_queue': types.String(128),
                 'disposition':  types.String(128), 'wait': types.Integer,
                 'self_service': types.Integer, 'active': types.Integer,
                 'on_hold': types.Integer, 'contact_id': types.Integer,
                 'source': types.String(8), 'agent': types.String(128)}

    # define the db connection
    # replace host, port, db to run
    engine = create_engine('mssql+pyodbc://@HOST:PORT/DB?driver=SQL+Server')
    connection = engine.connect()

    # load the file to a dataframe. skip header and footer data in the report.
    # convert the time fields to number of seconds
    df = pd.read_excel('./exported_data/' + data, header=None, skiprows=13,
                       skip_footer=2, names=col_label,
                       converters={1: convert_time, 7: convert_time,
                                   8: convert_time, 9: convert_time,
                                   10: convert_time})

    # file contains merged cells. use fillna to fill in the missing values
    # by default, pandas will put the value in the first row of the merge
    # and populate the rest as na. ffill will fill down to next value
    df.disposition = pd.Series(df.disposition).fillna(method='ffill')

    # get the current row count in table for error checking purposes
    current_row_count = get_count(connection)
    print("The current table has {0} rows. You are adding {1} rows."
          .format(current_row_count, len(df)))

    # write the data to the db using pandas to_sql
    print("Updating the db.")
    df.to_sql(name='call_data', con=connection, index=False,
              if_exists='append', dtype=col_types)
    new_row_count = get_count(connection)

    # close the connection and veirfy the results
    connection.close()
    print("The table now has {0} rows.".format(new_row_count))
    if (current_row_count + len(df)) == new_row_count:
        print("Table updated as expected.")
        return 0
    else:
        print("Something went wrong in the update. Expected {0} rows but have\
        {1}.".format((current_row_count + len(df)), new_row_count))
        return 1


if __name__ == '__main__':
    # check if there are files to Load
    if len(os.listdir('./exported_data')) == 0:
        print("There are no files to load.")
        sys.exit()
    else:
        for new_report in os.listdir('./exported_data'):
            # load all excel files
            if new_report.endswith('.xls') or new_report.endswith('.xlsx'):
                print("Begin processing: {0}".format(new_report))
                load_status = load_data(new_report)
                if load_status == 0:
                    # if the load was successful, move the loaded file
                    print("Moving {0} to the processed directory."
                          .format(new_report))
                    os.rename('./exported_data/' + new_report,
                              './processed_data/' + new_report)
                else:
                    # if the load was not successful, do not move the file
                    print("Left {0} in the exported_data directory. Please rev\
                    iew for errors.".format(new_report))
                    # create an error log of files that did not load properly
                    log = 'error.log'
                    if os.path.exists(log):
                        append_write = 'a'
                    else:
                        append_write = 'w'
                    f = open(log, append_write)
                    f.write(new_report + '\n')
                    f.close()
