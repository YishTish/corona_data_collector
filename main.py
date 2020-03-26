import psycopg2
from datetime import datetime
import json
from config import db_settings, answer_titles, values_to_convert, gps_source_file
from gps_generator import GPSGenerator


def run_query(host, user, password, num_of_records=100, id_greater_than=0):
    connection = None
    cursor = None
    try:
        fetched_rows = []
        connection = psycopg2.connect(host=host, user=user, password=password, database="reports")
        cursor = connection.cursor()
        cursor.execute(
            "SELECT * FROM reports where id > {} order by id limit {}".format(id_greater_than, num_of_records))
        records = cursor.fetchall()
        for record in records:
            if record[2] is not None and len(record[2]) > 0\
                    and record[2]['version'] not in ['1.0.1', '1.1.0']:
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


def collect_row(row):
    returned_array = []
    for key, value in answer_titles.items():
        val = row.get(key, 0)
        if isinstance(val, str):
            val = val.replace(',', ' - ')
        returned_array.append(val)
    return ','.join([str(x) for x in returned_array])


def get_answer_keys():
    answer_keys = ''
    for key, value in answer_titles.items():
        answer_keys = answer_keys + value + ','
    answer_keys = answer_keys[:len(answer_keys) - 1] + '\n'
    return answer_keys


def convert_values(db_row):
    for key, value in db_row.items():
        for answer_key, answer_value in values_to_convert.items():
            if key == answer_key:
                db_row[key] = answer_value[value]
        if type(db_row[key]) == bool:
            db_row[key] = int(db_row[key])
    return db_row


def log_database_data(rows, filename):
    try:
        lines = []
        for row in rows:
            fixed_row = convert_values(row)
            lines.append(collect_row(fixed_row) + "\n")
        with open(filename, 'a', encoding="utf-8") as file:
            file.writelines(lines)
    except Exception as err:
        print('failed to write data to file - ', err)
    finally:
        print(f'data written to file {filename}. Number of rows: {len(rows)}. last id: {rows[len(rows) - 1]["id"]}')


def add_gps_coordinates(data_filename):
    try:
        gps_generator = GPSGenerator(False)
        data_with_coords = gps_generator.load_gps_coordinates(data_filename)
        # load_gps_coordinates(data_filename)
        filename_with_coords = data_filename[:data_filename.find('.')] + '_with_coords.csv'
        with open(filename_with_coords, 'w') as file_with_coords:
            titles_line = get_answer_keys()
            titles_line = titles_line[:len(titles_line) - 1] + \
                          ',address_longitude,address_latitude,address_street_accurate\n'
            file_with_coords.write(titles_line)
            file_with_coords.writelines(data_with_coords)
        print(f'Data with GPS coordinates was written to {filename_with_coords}')
    except Exception as err:
        print('failed to load coordinates', err)


if __name__ == '__main__':
    lowest_id = 145647
    batch_size = 100
    has_more_rows = True
    max_rows = 200
    counter = 0
    file_unique_signature = int(datetime.timestamp(datetime.now()));
    target_filename = 'corona_bot_answers_{}.csv'.format(file_unique_signature)
    with open(target_filename, 'w') as target_file:
        target_file.write(get_answer_keys())
    while has_more_rows and counter < max_rows:
        collected_rows = run_query(db_settings['host'], db_settings['username'], db_settings['password'], batch_size,
                                   lowest_id)
        if collected_rows is not None and len(collected_rows) > 0:
            counter += len(collected_rows)
            lowest_id = collected_rows[len(collected_rows) - 1]['id']
            log_database_data(collected_rows, target_filename)
        else:
            has_more_rows = False
    add_gps_coordinates(target_filename)
    print('Operation completed successfully')
