import optparse
import json
import os
from datetime import datetime, timedelta

import psycopg2

from DBToFileWriter import DBToFileWriter
from config import db_settings, query_batch_size, use_gps_finder, supported_questions_version, destination_archive


def run_query(settings, min_date, max_date, num_of_records=100):
    connection = None
    cursor = None
    try:
        fetched_rows = []
        connection = psycopg2.connect(host=settings['host'], user=settings['username'], password=settings['password'],
                                      sslmode='verify-ca', sslrootcert=settings['sslrootcert'],
                                      sslcert=settings['sslcert'], sslkey=settings['sslkey'], database="reports")
        cursor = connection.cursor()
        supported_versions_string = ''
        for version in supported_questions_version:
            supported_versions_string += f'\'{version}\','
        cursor.execute(
            f'SELECT * FROM reports '
            f'where created > \'{min_date}\' and created < \'{max_date}\''
            # f'and data->>\'version\' in ({supported_versions_string[0:-1]}) '
            f' limit {num_of_records}'
        )
        records = cursor.fetchall()
        for record in records:
            if record[2] is not None and len(record[2]) > 0:
                data_dict = record[2]
                data_dict['id'] = record[0]
                data_dict['created'] = record[1].isoformat()
                fetched_rows.append(data_dict)
        return fetched_rows
    except (Exception, psycopg2.Error) as err:
        print("Database error", err)
    finally:
        if connection:
            cursor.close()
            connection.close()

            print('Database connection closed')


def get_process_arguments():
    parser = optparse.OptionParser()
    parser.add_option('-s', action='store', dest='source', default='db')
    parser.add_option('-f', action='store', dest='file_path', default='')
    opts, args = parser.parse_args()
    if opts.source not in ('db', 'file'):
        print('source (-s) argument must be either "file" or "db"')
        exit(1)
    if opts.source == 'file' and opts.file_path == '':
        print('If you declare a file as your source, you must provide a path to the file you want to read')
        exit(1)
    return opts, args


def get_initial_db_dates():
    yesterday = datetime.now() - timedelta(days=1)
    start_date = yesterday.strftime('%Y-%m-%d' + ' 00:00:00')
    end_date = datetime.now().strftime('%Y-%m-%d') + ' 00:00:00'
    return start_date, end_date


if __name__ == '__main__':
    try:
        output_file = datetime.now().strftime('%Y-%m-%d_%H%M')
        db_to_file_writer = DBToFileWriter(f'corona_bot_answers_{output_file}.csv')
        options, arguments = get_process_arguments()
        if options.source == 'file':
            print(f'Loading data from {options.file_path}')
            with open(options.file_path, "r") as data_source_file:
                db_to_file_writer.resultSet = json.load(data_source_file)
            db_to_file_writer.log_database_data()
            source_filename = options.file_path.split('/')[-1]
            os.rename(options.file_path, f'{destination_archive}/{source_filename}')
        elif options.source == 'db':
            from_date, to_date = get_initial_db_dates()
            collected_rows = run_query(db_settings, from_date, to_date, query_batch_size)
            if collected_rows is not None and len(collected_rows) > 0:
                db_to_file_writer.resultSet = collected_rows
                from_date = collected_rows[-1]['created']
                db_to_file_writer.log_database_data()
        print('Adding GPS coordinates to records selected')
        db_to_file_writer.add_gps_coordinates(use_gps_finder)
        db_to_file_writer.clear_output_files()
        print('Operation cycle completed successfully')
    except Exception as err:
        print('failed to run data collector: ', err)
        continue_running = False
    exit()
