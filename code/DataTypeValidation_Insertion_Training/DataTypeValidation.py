import shutil
from cassandra.query import SimpleStatement
from cassandra import Timeout, Unavailable, ConsistencyLevel, AuthenticationFailed, OperationTimedOut
from cassandra.policies import ConstantSpeculativeExecutionPolicy
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.auth import PlainTextAuthProvider
from cassandra.concurrent import execute_concurrent_with_args
from os import listdir
import os
import json
import csv
import queue
import time
import pandas as pd
from application_logging.logger import App_Logger


class dBOperation:
    """
      This class shall be used for handling all the database related operations.

    """

    def __init__(self):
        self.badFilePath = "Training_Raw_files_validated/Bad_Raw"
        self.goodFilePath = "Training_Raw_files_validated/Good_Raw"
        self.fileFromDb = 'Training_FileFromDB/'
        self.logger = App_Logger()

        # prelim checks
        if not os.path.isdir(self.goodFilePath):
            error = NotADirectoryError('Good file path is not a directory, exiting.')
            raise error
        elif not [f for f in os.listdir(self.goodFilePath)
                  if os.path.isfile(os.path.join(self.goodFilePath, f))]:
            error = FileNotFoundError('Good file path does not contain any files, exiting.')
            raise error

        if not os.path.isdir(self.badFilePath):
            error = NotADirectoryError('Good file path is not a directory, exiting.')
            raise error

    def dataBaseConnection(self, DatabaseName):

        """
                Method Name: dataBaseConnection
                Description: This method opens the connection to the DB with given name.
                Output: Connection to the DB
                On Failure: Raise ConnectionError or AuthenticationFailed

    """
        try:
            file = open("Training_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, f"Trying to connect to database {DatabaseName}")

            if not os.path.isfile('astradb/secure-connect-training.zip'):
                self.logger.log(file, "Connection config file 'astradb/secure-connect-training.zip' not found.")
                raise FileNotFoundError("Connection config file 'astradb/secure-connect-training.zip' not found.")

            cloud_config = {
                'secure_connect_bundle': 'astradb/secure-connect-training.zip'
            }
            self.logger.log(file, "Successfully loaded connection config file.")

            if not os.path.isfile("astradb/training_token.csv"):
                self.logger.log(file, "The token file astradb/training_token.csv not found.")
                raise FileNotFoundError("The token file astradb/training_token.csv not found.")

            with open('astradb/training_token.csv', 'r', encoding='utf-8-sig') as token:
                credentials = csv.reader(token)
                next(credentials)  # skip the header
                for line in credentials:
                    db_id = line[0]
                    db_pass = line[1]
            self.logger.log(file, "Successfully loaded token for connection.")

            auth_provider = PlainTextAuthProvider(db_id, db_pass)

            # speculative execution policy
            sepolicy = ExecutionProfile(
                speculative_execution_policy=ConstantSpeculativeExecutionPolicy(delay=.5, max_attempts=10),
                request_timeout=30
            )

            cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider, connect_timeout=20,
                              execution_profiles={EXEC_PROFILE_DEFAULT: sepolicy})
            self.logger.log(file, f"Attemption to open database {DatabaseName}.")
            conn = cluster.connect(DatabaseName)
            self.logger.log(file, "Opened %s database successfully" % DatabaseName)
            file.close()

        except FileNotFoundError as notfound:
            file = open("Training_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, notfound)
            file.close()
            raise notfound
        except AuthenticationFailed as authfail:
            file = open("Training_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, f"Authentication error occurred while trying to connect to Database : {authfail}")
            self.logger.log(file, 'Please check your token.')
            file.close()
            raise authfail
        except ConnectionError:
            file = open("Training_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Error while connecting to database: %s" % ConnectionError)
            file.close()
            raise ConnectionError
        except Exception as e:
            file = open("Training_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, e)
            raise e
        else:
            return conn

    def createTableDb(self, DatabaseName, column_names, timeout_retry=True):
        """
                        Method Name: createTableDb
                        Description: This method creates a table in the given database
                                    which will be used to insert the Good data after raw data validation.
                        Output: None
                        On Failure: Raise Exception
        """
        table_name_list = ['goodrawdata_00' + str(k) for k in range(1, 5)]
        conn = None  # because we must define conn outside try statement
        try:

            log_file = open("Training_Logs/DbTableCreateLog.txt", 'a+')
            self.logger.log(log_file, 'Initialise table creation')
            self.logger.log(log_file, 'Connecting to database.')
            conn = self.dataBaseConnection(DatabaseName)
            self.logger.log(log_file, 'Connection established')
            # create prepared statements for optimisation
            # find_stmt = "SELECT COUNT(table_name) FROM system_schema.tables \
            #                        WHERE keyspace_name={DatabaseName} AND table_name=?"
            # find_prep = conn.prepare(find_stmt)

            # create the schema same for all tables
            table_schema = ''
            for item in column_names.items():
                ele = ' '.join(item)
                table_schema = ', '.join((table_schema, ele))
            else:
                table_schema = table_schema[2::]  # remove the initial ', '

            # create statements and execute for tables
            future1 = []
            self.logger.log(log_file, f"Checking if tables exit, if so drop the existing tables.")
            for name in table_name_list:
                # del statement
                del_text = f"DROP TABLE IF EXISTS {DatabaseName}.{name}"
                del_stmt = SimpleStatement(del_text)
                del_stmt.is_idempotent = True

                # build  list of futures
                future1.append(conn.execute_async(del_stmt))

            for res in future1:
                res.result()
            else:
                self.logger.log(log_file, "Successfully dropped old tables.")
                del future1

            # find relevant table names to create according to good data files
            self.logger.log(log_file, "Creating new tables")
            table_exist = set()
            for file in os.listdir(self.goodFilePath):
                k = file.split('.')[0][-1]
                if k in list('1234'):  # only certain tables are allowed
                    table_exist.add('goodrawdata_00'+k)

                else:  # that's a bad file!
                    self.logger.log(log_file, f"Found bad file {file}. Move to bad data directory.")
                    try:
                        shutil.move(os.path.join(self.goodFilePath, file), self.badFilePath)
                    except OSError:
                        self.logger.log(
                            log_file, f"Failed to move {file}. Perhaps it already exists in bad data directory.")
                        self.logger.log(log_file, f"Deleting {file}.")
                        os.remove(os.path.join(self.goodFilePath, file))

            for name in table_exist:
                # create statement
                create_stmt = f"CREATE TABLE {DatabaseName}.{name} ({table_schema}" + \
                              ", PRIMARY KEY (unit_nr, time_cycles)) WITH CLUSTERING ORDER BY (time_cycles ASC);"

                # build  list of futures
                self.logger.log(log_file, f"Creating new table {name}")
                # print(create_stmt)
                create_stmt = SimpleStatement(create_stmt)
                create_stmt.consistency_level = ConsistencyLevel.ONE
                conn.execute(create_stmt)
                self.logger.log(log_file, f"Created {name} successfully.")
                # print(f'done for {name}')
                # future2.append(conn.execute_async(create_stmt))
            else:
                self.logger.log(log_file, "Created new tables.")

        #    for res in future2:
        #        res.result()
        #    else:
        #        self.logger.log(file, 'New tables created successfully!!')
        #        del future2

        except (OperationTimedOut, Timeout) as timeout:
            log_file = open("Training_Logs/DbTableCreateLog.txt", 'a+')
            self.logger.log(log_file, f'Operation timed out while creating tables: str{timeout}')
            # retry
            if conn is not None:
                conn.shutdown()
            if timeout_retry:
                self.logger.log(log_file, f'Retrying table creation.!!')
                return self.createTableDb(DatabaseName, column_names, timeout_retry=False)
            else:
                self.logger.log(log_file, f'Operation timed out while creating tables, quitting!!: str{timeout}')
                log_file.close()
                raise timeout

        except Unavailable as unavailable:
            log_file = open("Training_Logs/DbTableCreateLog.txt", 'a+')
            self.logger.log(log_file, f'Error: Nodes unavailable while creating tables: {unavailable}', )
            # retry
            if conn is not None:
                conn.shutdown()
            if timeout_retry:
                self.logger.log(log_file, f'Retrying table creation!!')
                return self.createTableDb(DatabaseName, column_names, timeout_retry=False)

            else:
                self.logger.log(log_file, f"Error: Nodes unavailable, terminating: {unavailable}")
                log_file.close()
                raise unavailable

        except Exception as e:
            log_file = open("Training_Logs/DbTableCreateLog.txt", 'a+')
            if conn is not None:
                conn.shutdown()
            self.logger.log(log_file, f"Error: {e}")
            log_file.close()
            raise e

        else:
            # self.logger.log(file, "Successfully created new tables.")
            log_file.close()
            return conn

    def insertIntoTableGoodData(self, Database, timeout_retry=True):

        """
                               Method Name: insertIntoTableGoodData
                               Description: This method inserts the Good data files from the
                                            Good_Raw folder into database tables.
                               Output: connection object
                               On Failure: Raise Exception
        """
        log_file = open("Training_Logs/DbInsertLog.txt", 'a+')
        self.logger.log(log_file, 'Starting data insertion into database')

        if not os.path.isfile('./schema_training.json'):
            error = FileNotFoundError('Required schema file schema_training.json not found in current directory.')
            self.logger.log(log_file, error)
            raise error

        with open('./schema_training.json', 'r') as f:
            dic = json.load(f)
            column_names = dic['ColName']
        cols = ', '.join(column_names.keys())  # used later in query

        self.logger.log(log_file, 'Connecting to database and waiting for  table creation.')
        conn = self.createTableDb(Database, column_names)
        if conn is None:  # if connection fails abort immediately
            error = ConnectionError(f'Failed to create required tables in {Database} database.')
            self.logger.log(log_file, error)
            raise error

        goodFilePath = self.goodFilePath
        badFilePath = self.badFilePath
        onlyfiles = [f for f in os.listdir(self.goodFilePath)
                  if os.path.isfile(os.path.join(self.goodFilePath, f))]
        # count = 1
        conc_level = 5000

        # construct insertion query
        qmarks = ', '.join('?' * 26)

        # select table name according to file names
        self.logger.log(log_file, "Uploading files.")
        for file in onlyfiles:
            self.logger.log(log_file, f'Trying to upload file {file}.')
            try:
                end = file.split(sep='.')[0][-1]
                if end in list('1234'):
                    table_name = 'goodrawdata_00' + end
                    self.logger.log(log_file, f"Uploading {file} to {table_name} table.")
                else:
                    self.logger.log(log_file, f'File {file} does not have valid name!! Moving to bad files')
                    shutil.move(goodFilePath + '/' + file, badFilePath)
                    self.logger.log(log_file, f'{file} moved to bad files directory successfully.')
                    continue

                # let us check if this table exists..in db
                check_query = "SELECT table_name FROM system_schema.tables " + \
                              f"WHERE keyspace_name='{Database}' AND table_name='{table_name}'"
                check_stmt = SimpleStatement(check_query)
                check_stmt.is_idempotent = True

                self.logger.log(log_file, f"Checking table name {table_name} in database.")
                check = conn.execute(check_stmt)
                if not check[0].table_name == table_name:
                    error = Exception('The required table for insertion does not exist in database.')
                    self.logger.log(log_file, error)
                    conn.shutdown()
                    if timeout_retry:
                        self.logger.log(log_file, 'Retrying data insertion.')
                        return self.insertIntoTableGoodData(Database, timeout_retry=False)
                    else:
                        self.logger.log(log_file, 'Aborting!')
                        raise error

                # prepare insert stmt
                insert_stm = f"INSERT INTO {Database}.{table_name} ({cols}) VALUES ( {qmarks} )"
                insert_prep = conn.prepare(insert_stm)

                # load file into pandas
                data = pd.read_csv(goodFilePath + '/' + file, sep=r'\s+', header=None)
                params = zip(*[data[c] for c in range(data.shape[1])])  # list of all rows
                total_queries = data.shape[0]

                # set up queue and start timer
                start = time.time()
                future_q = queue.Queue(maxsize=conc_level)

                # start insertion
                for value in params:
                    future = conn.execute_async(insert_prep, value)
                    try:
                        future_q.put_nowait(future)
                    except queue.Full:
                        # clear queue since it's full
                        while True:
                            try:
                                future_q.get_nowait().result()
                            except queue.Empty:
                                break
                        future_q.put_nowait(future)

                else:
                    # process the last queries in the queue
                    while True:
                        try:
                            future_q.get_nowait().result()
                        except queue.Empty:
                            break

                    end = time.time()
                    self.logger.log(log_file, f"Finished uploading {file} {total_queries} queries with a " +
                                    f"concurrency level of {conc_level} in {int(end - start)} seconds.")

            except (Timeout, OperationTimedOut) as timeout:
                self.logger.log(log_file, f'Operation timed out while uploading {file} str{timeout}', )
                # retry
                conn.shutdown()
                if timeout_retry:
                    self.logger.log(log_file, f'Retrying to upload {file}!!')
                    return self.insertIntoTableGoodData(Database, timeout_retry=False)

                else:
                    self.logger.log(log_file, f'Operation timed out while trying to upload file. Quitting: {timeout}')
                    log_file.close()
                    raise timeout

            except Unavailable as unavailable:
                self.logger.log(log_file, f'Error: Nodes unavailable while trying to upload {file}: {unavailable}', )
                # retry
                conn.shutdown()
                if timeout_retry:
                    self.logger.log(log_file, f'Retrying to upload {file}!!')
                    return self.insertIntoTableGoodData(Database, timeout_retry=False)

                else:
                    self.logger.log(log_file, f"Error: Nodes unavailable, terminating: {unavailable}")
                    log_file.close()
                    raise unavailable

            except Exception as e:
                conn.shutdown()
                self.logger.log(log_file, f"Error: {e}")
                log_file.close()
                raise e
        else:
            self.logger.log(log_file, "All files are inserted successfully.")
            log_file.close()
            return conn

        # self.logger.log(log_file, 'An unexpected error has occurred while trying to upload files to database.')
        # raise Exception('An unexpected error has occurred while trying to upload files to database.')

    def selectingDatafromtableintocsv(self, Database, timeout_retry=True, flush=True):

        """
                        Method Name: selectingDatafromtableintocsv
                        Description: This method exports the data in GoodData table as a CSV file. in a given location.
                                    above created .
                        Output: None
                        On Failure: Raise Exception

        """

        # self.fileFromDb = 'Training_FileFromDB/'
        log_file = open("Training_Logs/ExportToCsv.txt", 'a+')
        self.logger.log(log_file, "Staring to import csv from database.")
        table_name_list = ['goodrawdata_00' + str(k) for k in range(1, 5)]
        conn = None
        try:
            self.logger.log(log_file, "Connecting to database...")
            if flush:
                self.logger.log(log_file, "Flush is True, flush old tables and initiate database insertion.")
                conn = self.insertIntoTableGoodData(Database)
                self.logger.log(log_file, "Database insertion completed.")
            else:
                self.logger.log(log_file, "Flush is False, so program will pull existing tables without insertion")
                conn = self.dataBaseConnection(Database)
                self.logger.log(log_file, "Connection established.")

            if conn is None:
                error = ConnectionError('Connection failed, check relevant logs in log directory. Aborting!')
                self.logger.log(log_file, error)
                raise error

            # fetch existing tables from database
            stmt = f"SELECT table_name FROM system_schema.tables WHERE keyspace_name='{Database}'"
            stmt = SimpleStatement(stmt)
            stmt.is_idempotent = True
            self.logger.log(log_file, "Finding existing tables in database.")
            tables = conn.execute(stmt)

            tables_exist = set()
            for result in tables:
                name = getattr(result, 'table_name')
                if name in table_name_list:
                    tables_exist.add(name)
                    self.logger.log(log_file, f"Found relevant table {name}.")
            if not tables_exist:  # no tables exist that are relevant to us
                error = Exception(f'No relevant existing tables found in database {Database}. Quitting!')
                self.logger.log(log_file, f"Error: {error}")
                raise error

            # make the output directory if not exists
            self.logger.log(log_file, 'Trying to create output directory for csv files if not exists.')
            if not os.path.isdir(self.fileFromDb):
                os.makedirs(self.fileFromDb)
            else:
                # empty the directory
                self.logger.log(log_file, 'Trying to empty existing output directory.')
                for file in os.listdir(self.fileFromDb):
                    rm_path = os.path.join(self.fileFromDb, file)
                    try:
                        if os.path.islink(rm_path) or os.path.isfile(rm_path):
                            os.remove(rm_path)
                        elif os.path.isdir(rm_path):
                            shutil.rmtree(rm_path)
                    except OSError as ose:
                        self.logger.log(log_file, f"An error occurred while emptying existing output directory.")
                        self.logger.log(log_file, ose)
                        raise ose
            self.logger.log(log_file, 'Output directory configured successfully!')

            self.logger.log(log_file, "Writing all relevant tables to csv files.")
            for name in tables_exist:
                # choose file name
                file_name = 'train_input_' + name.split('_')[1] + '.csv'
                self.logger.log(log_file, f"Writing table {name} to {file_name}")

                with open(os.path.join(self.fileFromDb, file_name), 'w+', newline='', encoding='utf-8') as csvfile:
                    # prepare select statement
                    select_stmt = f"SELECT * FROM {Database}.{name} WHERE unit_nr=? ORDER BY time_cycles ASC"
                    select_prep = conn.prepare(select_stmt)
                    select_prep.consistency_level = ConsistencyLevel.ONE
                    select_prep.is_idempotent = True

                    # writer object
                    writer = csv.writer(csvfile, delimiter=',')

                    params = [(k,) for k in range(1, 101)]
                    res_set = execute_concurrent_with_args(conn, select_prep, params, concurrency=1000)

                    write_header = True  # write header flag
                    for (success, result) in res_set:
                        if not success:
                            conn.shutdown()
                            self.logger.log(log_file,
                                            f"An error occurred in writing table {name} to csv file: {result}")
                            if timeout_retry:
                                self.logger.log(log_file, f"Retrying importing csv from table {name}")
                                return self.selectingDatafromtableintocsv(Database, timeout_retry=False, flush=flush)
                            else:
                                self.logger.log(log_file,
                                                f"Failed to write table {name} to csv file: {result} Quitting")
                                log_file.close()
                                raise result
                        else:
                            if write_header:  # write header
                                header = result.column_names
                                writer.writerow(header)
                                write_header = False  # make sure we write header only the first time

                            writer.writerows(result)
                    else:
                        self.logger.log(log_file, f"Completed writing table {name} to file {file_name}.")
            else:
                self.logger.log(log_file, f"Completed writing all tables to csv files.")

        except (OperationTimedOut, Timeout) as timeout:
            self.logger.log(log_file, f'Operation timed out while writing tables: str{timeout}', )
            # retry
            if conn is not None:
                conn.shutdown()
            if timeout_retry:
                self.logger.log(log_file, f'Retrying write tables to csv!!')
                return self.selectingDatafromtableintocsv(Database, timeout_retry=False, flush=flush)
            else:
                self.logger.log(log_file, f'Unable to export table to csv file. Quitting: {timeout}')
                # if conn is not None: conn.shutdown()
                log_file.close()
                raise timeout

        except Unavailable as unavailable:
            self.logger.log(log_file,
                            f'Error: Nodes unavailable while trying to write tables to csv: {unavailable}')
            # retry
            if conn is not None:
                conn.shutdown()
            if timeout_retry:
                self.logger.log(log_file, f'Retrying write tables to csv!!')
                return self.selectingDatafromtableintocsv(Database, timeout_retry=False, flush=flush)

            else:
                self.logger.log(log_file, f"Error: Nodes unavailable, terminating: {unavailable}")
                log_file.close()
                raise unavailable

        except Exception as e:
            if conn is not None:
                conn.shutdown()
            self.logger.log(log_file, f"Error: {e}")
            log_file.close()
            raise e

        else:
            conn.shutdown()
            log_file.close()
