import datetime
import json
import uuid
import psycopg2
from psycopg2.extras import execute_values

locationPrefix = "loc-"  # Location ID prefix


def uid(id_length):
    # Generate a UUID
    unique_id = str(uuid.uuid4()).replace('-', '')
    return unique_id[:id_length]


def insert_location_data(mapping):
    conn = psycopg2.connect(
        dbname="svc_location",
        user="postgres",
        password="darlene202509",
        host="127.0.0.1",
        port="25432"
    )
    cursor = conn.cursor()

    def insert_data(data, level):
        records = []
        current_time = datetime.datetime.now(datetime.timezone.utc)
        for item in data:
            record = (
                item['id'],  # Use the generated unique ID
                item.get('parent_id'),
                level,
                item['name'],
                'system',
                'system',
                'active',
                1,
                item['code'],
                current_time,
                current_time,
                item['full_path']
            )
            records.append(record)

        sql = """
            INSERT INTO public.svc_location (id, parent_id, level, name, created_by, updated_by, status, version, code, created_at, updated_at, full_path)
            VALUES %s
            ON CONFLICT (id) DO NOTHING
        """

        print(cursor.mogrify(sql, (records,)).decode('utf-8'))
        execute_values(cursor, sql, records)

    def process_data(data, parent_id=None, parent_path="", level='province'):
        for item in data:
            item['id'] = locationPrefix + uid(8)
            item['parent_id'] = parent_id
            item['full_path'] = f"{parent_path}/{item['name']}".strip("/")
            if level == 'province':
                process_data(item['cities'], item['id'], item['full_path'], 'city')
            elif level == 'city':
                process_data(item['counties'], item['id'], item['full_path'], 'county')
            elif level == 'county':
                process_data(item['towns'], item['id'], item['full_path'], 'town')
            elif level == 'town':
                process_data(item['committees'], item['id'], item['full_path'], 'committee')

    process_data(mapping)
    insert_data(mapping, 'province')
    for province in mapping:
        insert_data(province['cities'], 'city')
        for city in province['cities']:
            insert_data(city['counties'], 'county')
            for county in city['counties']:
                insert_data(county['towns'], 'town')
                for town in county['towns']:
                    insert_data(town['committees'], 'committee')

    conn.commit()
    cursor.close()
    conn.close()


def read_json_to_dict(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data


if __name__ == '__main__':
    file_path = 'data/location.json'
    data_dict = read_json_to_dict(file_path)
    insert_location_data(data_dict)
