import shutil
from cassandra.query import SimpleStatement
from cassandra import Timeout, Unavailable, ConsistencyLevel, AuthenticationFailed, OperationTimedOut
from cassandra.policies import ConstantSpeculativeExecutionPolicy
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.auth import PlainTextAuthProvider
from cassandra.concurrent import execute_concurrent_with_args
import os
import json
import csv
import queue
import time
import pandas as pd
import decimal
from application_logging.logger import App_Logger


class dBOperation:
    """
      This class shall be used for handling all the database related operations.

    """

    def __init__(self):
        self.badFilePath = "Prediction_Raw_files_validated/Bad_Raw"
        self.goodFilePath = "Prediction_Raw_files_validated/Good_Raw"
        self.fileFromDb = 'Prediction_FileFromDB/'
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
            file = open("Prediction_Logs/DataBaseConnectionLog.txt", 'a+')
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

            # make a execution profile with pandas row factory and no timeout, used in exporting to csv
            def pandas_factory(colnames, rows):
                return pd.DataFrame(rows, columns=colnames)

            pandaspolicy = ExecutionProfile(
                speculative_execution_policy=ConstantSpeculativeExecutionPolicy(delay=.5, max_attempts=10),
                request_timeout=None,
                row_factory=pandas_factory
            )

            sepolicy = ExecutionProfile(
                speculative_execution_policy=ConstantSpeculativeExecutionPolicy(delay=.5, max_attempts=10),
                request_timeout=30,
            )

            profiles = {
                EXEC_PROFILE_DEFAULT: sepolicy,
                'pandas_profile': pandaspolicy
            }

            cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider, connect_timeout=20,
                              execution_profiles=profiles)
            self.logger.log(file, f"Attemption to open database {DatabaseName}.")
            conn = cluster.connect(DatabaseName)
            self.logger.log(file, "Opened %s database successfully" % DatabaseName)
            file.close()

        except FileNotFoundError as notfound:
            file = open("Prediction_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, notfound)
            file.close()
            raise notfound
        except AuthenticationFailed as authfail:
            file = open("Prediction_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, f"Authentication error occurred while trying to connect to Database : {authfail}")
            self.logger.log(file, 'Please check your token.')
            file.close()
            raise authfail
        except ConnectionError:
            file = open("Prediction_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Error while connecting to database: %s" % ConnectionError)
            file.close()
            raise ConnectionError
        except Exception as e:
            file = open("Prediction_Logs/DataBaseConnectionLog.txt", 'a+')
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
        log_file = open("Prediction_Logs/DbTableCreateLog.txt", 'a+')
        try:

            # log_file = open("Training_Logs/DbTableCreateLog.txt", 'a+')
            self.logger.log(log_file, 'Initialise table creation')
            self.logger.log(log_file, 'Connecting to database.')
            conn = self.dataBaseConnection(DatabaseName)
            self.logger.log(log_file, 'Connection established')

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

            # check for metadata table
            del_meta = f'DROP TABLE IF EXISTS {DatabaseName}.prediction_meta_data'
            del_meta = SimpleStatement(del_meta)
            del_meta.is_idempotent = True
            conn.execute(del_meta)
            self.logger.log(log_file, "Old meta data table also dropped")

            # find relevant table names to create according to good data files
            self.logger.log(log_file, "Creating new tables")
            table_exist = set()
            for file in os.listdir(self.goodFilePath):
                k = file.split('.')[0][-1]
                if k in list('1234'):  # only certain tables are allowed
                    table_exist.add('goodrawdata_00' + k)

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
            else:
                self.logger.log(log_file, "Created new tables.")

            # create meta table as well, meta table contains data about the tables
            stmt = f"CREATE TABLE {DatabaseName}.prediction_meta_data " + \
                   "(table_name VARCHAR, unit_nr_range INT, total_rows INT, PRIMARY KEY (table_name))"
            query = SimpleStatement(stmt)
            self.logger.log(log_file, "Creating meta data table training_meta_data.")
            conn.execute(query)
            self.logger.log(log_file, "Meta data table created.")

        except (OperationTimedOut, Timeout) as timeout:
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
            self.logger.log(log_file, f'Error: Nodes unavailable while creating tables: {unavailable}')
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
            # log_file = open("Prediction_Logs/DbTableCreateLog.txt", 'a+')
            if conn is not None:
                conn.shutdown()
            self.logger.log(log_file, f"Error: {e}")
            log_file.close()
            raise e

        else:
            # self.logger.log(file, "Successfully created new tables.")
            log_file.close()
            return conn  # column names contains the schema file

    def insertIntoTableGoodData(self, Database, timeout_retry=True):

        """
                               Method Name: insertIntoTableGoodData

                               Description: This method inserts the Good data files from the
                                            Good_Raw folder into database tables.

                               Output: connection object

                               On Failure: Raise Exception
        """
        log_file = open("Prediction_Logs/DbInsertLog.txt", 'a+')
        self.logger.log(log_file, 'Starting data insertion into database')

        if not os.path.isfile('./schema_prediction.json'):
            error = FileNotFoundError('Required schema file schema_prediction.json not found in current directory.')
            self.logger.log(log_file, error)
            raise error

        with open('./schema_prediction.json', 'r') as f:
            dic = json.load(f)
            column_names = dic['ColName']
        cols = ', '.join(column_names.keys())  # used later in query

        self.logger.log(log_file, 'Connecting to database and waiting for  table creation.')
        conn = self.createTableDb(Database, column_names)
        if conn is None:  # if connection fails abort immediately
            error = ConnectionError(f'Failed to create required tables in {Database} database.')
            self.logger.log(log_file, error)
            raise error

        self.logger.log(log_file, 'Verifying existence of meta data table.')
        check_meta = "SELECT table_name FROM system_schema.tables " + \
                      f"WHERE keyspace_name='{Database}' AND table_name='prediction_meta_data'"
        check_meta = SimpleStatement(check_meta)
        check_meta.is_idempotent = True
        ismeta = conn.execute(check_meta)
        if not ismeta[0].table_name == 'prediction_meta_data':
            self.logger.log(log_file, 'Required metadata table not found in database.')
            conn.shutdown()
            if timeout_retry:
                self.logger.log(log_file, 'Retrying....!')
                return self.insertIntoTableGoodData(Database=Database, timeout_retry=False)
            else:
                error = Exception('Required metadata table not found in database.')
                self.logger.log(log_file, f'Error: {error}')
                raise error
        self.logger.log(log_file, "Required metadata table exists.")

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

                params = zip(*[data[c] for c in range(data.shape[1])])
                total_queries = data.shape[0]
                # used later
                unit_nr_range = data[0].unique().size

                # set up queue and start timer
                start = time.time()
                future_q = queue.Queue(maxsize=conc_level)

                # start insertion
                for value in params:
                    value_decimal = [decimal.Decimal(str(k)) for k in value]
                    future = conn.execute_async(insert_prep, value_decimal)
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

                    # now insert a metadata table too
                    self.logger.log(log_file, f"Uploading meta data for table {table_name}.")
                    meta_insrt = "INSERT INTO prediction_meta_data (table_name, unit_nr_range, total_rows) VALUES " + \
                        f"('{table_name}', {unit_nr_range}, {total_queries})"
                    meta_insrt = SimpleStatement(meta_insrt)

                    conn.execute(meta_insrt)
                    self.logger.log(log_file, "Uploaded table metadata.")

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
            return conn, column_names

    def selectingDatafromtableintocsv(self, Database, timeout_retry=True, flush=True):

        """
                        Method Name: selectingDatafromtableintocsv
                        Description: This method exports the data in GoodData table as a CSV file. in a given location.
                                    above created .
                        Output: None
                        On Failure: Raise Exception

        """

        # self.fileFromDb = 'Training_FileFromDB/'
        log_file = open("Prediction_Logs/ExportToCsv.txt", 'a+')
        self.logger.log(log_file, "Staring to import csv from database.")
        table_name_list = ['goodrawdata_00' + str(k) for k in range(1, 5)]
        conn = None
        try:
            self.logger.log(log_file, "Connecting to database...")
            if flush:
                self.logger.log(log_file, "Flush is True, flush old tables and initiate database insertion.")

                # column names is the schema dictionary
                conn, column_names = self.insertIntoTableGoodData(Database)
                self.logger.log(log_file, "Database insertion completed.")
            else:
                self.logger.log(log_file, "Flush is False, so program will pull existing tables without insertion")
                conn = self.dataBaseConnection(Database)
                # self.logger.log(log_file, "Connection established.")
                self.logger.log(log_file, "Manually fetching column names from schema file.")
                # manually fetch column names, if fetched from database col names can be out of order
                if not os.path.isfile('./schema_prediction.json'):
                    error = FileNotFoundError(
                        'Required schema file schema_prediction.json not found in current directory.')
                    self.logger.log(log_file, f"Error: {error}")
                    raise error

                with open('./schema_prediction.json', 'r') as f:
                    dic = json.load(f)
                    column_names = dic['ColName']
                self.logger.log(log_file, 'Successfully fetched column names from schema file')

            if conn is None:
                error = ConnectionError('Connection failed, check relevant logs in log directory. Aborting!')
                self.logger.log(log_file, error)
                raise error

            self.logger.log(log_file, "Connection established to database. Now begin reading tables.")
            # fetch existing tables from database

            stmt = f"SELECT table_name FROM system_schema.tables WHERE keyspace_name='{Database}'"
            stmt = SimpleStatement(stmt)
            stmt.is_idempotent = True
            self.logger.log(log_file, "Finding existing tables in database.")
            tables = conn.execute(stmt)

            tables_exist = set()
            for result in tables:
                name = getattr(result, 'table_name') # result.table_name
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

            conn.default_fetch_size = None  # fetch all results at once disable paging

            for name in tables_exist:
                # fetch meta data
                self.logger.log(log_file, f"Loading metadata for table {name}.")
                get_meta = \
                    f"SELECT unit_nr_range,total_rows FROM {Database}.prediction_meta_data WHERE table_name='{name}'"
                get_meta = SimpleStatement(get_meta)
                get_meta.is_idempotent = True
                metadata = conn.execute(get_meta)
                self.logger.log(log_file, "Metadata loaded.")

                # assign attributes from metadata
                try:
                    unit_nr_range, total_rows = metadata[0]
                except Exception as e:
                    self.logger.log(log_file, f"Error occurred in meta data processing: {e}")
                    raise e

                select_stmt = f"SELECT * FROM {Database}.{name} WHERE unit_nr=? ORDER BY time_cycles ASC"
                select_prep = conn.prepare(select_stmt)
                select_prep.is_idempotent = True
                select_prep.consistency_level = ConsistencyLevel.ONE  # set low consistency level
                select_prep.fetch_size = None  # fetch all results at once disable paging

                # choose file name
                file_name = 'test_input_' + name.split('_')[1] + '.csv'
                self.logger.log(log_file, f"Writing table {name} to {file_name}")

                # find the corresponding prediction data file

                conc_level = 1000

                params = [(k,) for k in range(1, unit_nr_range+1)]

                # starting writing
                start = time.time()
                future_q = queue.Queue(maxsize=conc_level)

                # datafram to hold results
                df = pd.DataFrame()

                for value in params:
                    future = conn.execute_async(select_prep, value, execution_profile='pandas_profile')
                    try:
                        future_q.put_nowait(future)
                    except queue.Full:
                        # clear queue
                        while True:
                            try:
                                r = future_q.get_nowait().result()
                                df = pd.concat([df, r._current_rows], ignore_index=True)
                            except queue.Empty:
                                break
                        future_q.put_nowait(future)
                else:
                    while True:
                        try:
                            r = future_q.get_nowait().result()
                            df = pd.concat([df, r._current_rows], ignore_index=True)
                        except queue.Empty:
                            break

                    df = df[list(column_names.keys())]
                    self.logger.log(log_file, 'Verifying against meta data.')
                    if not df.shape[0] == total_rows:
                        self.logger.log(log_file,
                                        f'Warn: Verification failed for table {name}, please check the received file.')

                    df.to_csv(os.path.join(self.fileFromDb, file_name), index=False)
                    end = time.time()
                    self.logger.log(log_file, f"Finished writing {name} to {file_name} in {int(end - start)} seconds.")

            else:
                self.logger.log(log_file, "Successfully wrote all tables to csv files.")

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
