from config import answer_titles, values_to_convert, keys_to_convert
from gps_generator import GPSGenerator


def collect_row(row):
    returned_array = []
    for key, value in answer_titles.items():
        val = row.get(key, 0)
        if isinstance(val, str):
            val = val.replace(',', ' - ')
        returned_array.append(val)
    return ','.join([str(x) for x in returned_array])


def manipulate_values_versions(db_row):
    if 'exposure_status' in db_row and db_row['exposure_status'] == 'insulation_with_family':
        db_row['exposure_status'] = 'diagnosed'
        db_row['isolated_with_family'] = 1
    return db_row


def convert_values(db_row):
    db_row = manipulate_values_versions(db_row)
    for convert_key in keys_to_convert:
        if convert_key in db_row:
            db_row[keys_to_convert[convert_key]] = db_row[convert_key]
            db_row.pop(convert_key)
    for key, value in db_row.items():
        if key in values_to_convert:
            db_row[key] = values_to_convert[key][value]
        if type(db_row[key]) == bool:
            db_row[key] = int(db_row[key])
    return db_row


class DBToFileWriter:
    resultSet = []
    target_filename = ''

    def write_answer_keys(self, prefix='', suffix=''):
        answer_keys_line = ''
        for key, value in answer_titles.items():
            if len(answer_keys_line) == 0:
                answer_keys_line = value
            else:
                answer_keys_line = f'{answer_keys_line},{value}'
        if len(prefix) > 0:
            prefix = f'{prefix},'
        if len(suffix) > 0:
            suffix = f',{suffix}'
        with open(self.target_filename, 'w') as target_file:
            target_file.write(f'{prefix}{answer_keys_line}{suffix}\n')

    def log_database_data(self):
        fixed_row = ''
        try:
            lines = []
            for row in self.resultSet:
                fixed_row = convert_values(row)
                lines.append(collect_row(fixed_row) + "\n")
            with open(self.target_filename, 'a', encoding="utf-8") as file:
                file.writelines(lines)
        except Exception as err:
            print('failed to write data to file - ', err)
            print('failing row: ', fixed_row)
            exit(1)
        finally:
            print(f'data written to file {self.target_filename}. Number of rows: {len(self.resultSet)}.'
                  f' last id: {self.resultSet[- 1]["id"]}')

    def add_gps_coordinates(self):
        try:
            gps_generator = GPSGenerator(False)
            data_with_coords = gps_generator.load_gps_coordinates(self.target_filename)
            dot_index = self.target_filename.find('.')
            filename_with_coords = self.target_filename[:dot_index] + '_with_coords.csv'
            with open(filename_with_coords, 'w') as file_with_coords:
                self.write_answer_keys(suffix='address_longitude,address_latitude,address_street_accurate')
                file_with_coords.writelines(data_with_coords)
            print(f'Data with GPS coordinates was written to {filename_with_coords}')
        except Exception as err:
            print('failed to load coordinates', err)
