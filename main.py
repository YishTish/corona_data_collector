import psycopg2
from datetime import datetime
from config import db_settings, query_batch_size, process_max_rows, use_gps_finder, supported_questions_version,\
    query_from_date
from DBToFileWriter import DBToFileWriter


def run_query(host, user, password, num_of_records=100, created_greater_than='1970-01-01 00:00:00'):
    connection = None
    cursor = None
    try:
        fetched_rows = []
        connection = psycopg2.connect(host=host, user=user, password=password, database="reports")
        cursor = connection.cursor()
        cursor.execute(
            "SELECT * FROM reports where created > '{}' order by id limit {}".format(created_greater_than,
                                                                                     num_of_records))
        records = cursor.fetchall()
        for record in records:
            if record[2] is not None and len(record[2]) > 0\
                    and record[2]['version'] in supported_questions_version:
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
    file_unique_signature = datetime.today().strftime('%Y-%m-%d_%H%M')
    db_to_file_writer = DBToFileWriter()
    db_to_file_writer.target_filename = 'corona_bot_answers_{}.csv'.format(file_unique_signature)
    db_to_file_writer.write_answer_keys()
    counter = 0
    while counter < process_max_rows:
        collected_rows = run_query(db_settings['host'], db_settings['username'], db_settings['password'],
                                   query_batch_size, last_created)
        if collected_rows is not None and len(collected_rows) > 0:
            db_to_file_writer.resultSet = collected_rows
            counter += len(collected_rows)
            lowest_id = collected_rows[- 1]['id']
            last_created = collected_rows[-1]['created']
            db_to_file_writer.log_database_data()
        else:
            break
    db_to_file_writer.add_gps_coordinates(use_gps_finder)
    print('Operation completed successfully')
