import psycopg2
import time
from datetime import datetime, timedelta
from config import db_settings, query_batch_size, process_max_rows, use_gps_finder, supported_questions_version,\
    query_from_date
from DBToFileWriter import DBToFileWriter, write_answer_keys


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


if __name__ == '__main__':
    last_created = query_from_date
    continue_running = True
    while continue_running:
        try:
            file_unique_signature = datetime.now().strftime('%Y-%m-%d_%H%M')
            db_to_file_writer = DBToFileWriter()
            db_to_file_writer.target_filename = 'corona_bot_answers_{}.csv'.format(file_unique_signature)
            write_answer_keys(db_to_file_writer.target_filename)
            counter = 0
            yesterday = datetime.now() - timedelta(days=1)
            from_date = yesterday.strftime('%Y-%m-%d' + ' 00:00:00')
            to_date = datetime.now().strftime('%Y-%m-%d') + ' 00:00:00'
            while counter < process_max_rows:
                today_string = datetime.now().strftime('%Y-%m-%d')
                collected_rows = run_query(db_settings, from_date, to_date, query_batch_size)
                if collected_rows is not None and len(collected_rows) > 0:
                    db_to_file_writer.resultSet = collected_rows
                    counter += len(collected_rows)
                    lowest_id = collected_rows[- 1]['id']
                    from_date = collected_rows[-1]['created']
                    db_to_file_writer.log_database_data()
                else:
                    break
            print('Adding GPS coordinates to records selected')
            if counter > 0:
                db_to_file_writer.add_gps_coordinates(use_gps_finder)
                print('Operation cycle completed successfully')
                process_last_created = db_to_file_writer.get_last_created()
                if process_last_created != '':
                    last_created = process_last_created
                else:
                    print('could not get the last record\'s create date. Next process will run with old date')
            print(f'the next cycle will start at {datetime.now() + timedelta(hours=1)} with the first record created'
                  f' after {last_created}')
            time.sleep(60 * 60)
        except Exception as err:
            print('failed to run data collector: ', err)
            continue_running = False
