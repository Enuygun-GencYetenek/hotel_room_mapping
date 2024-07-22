import mysql.connector
from mysql.connector import Error
import requests
import json

def group_rooms_by_hotel(hotel_room_data):
    grouped_rooms = {}
    for room in hotel_room_data:
        hotel_id = room['hotel_id']
        if hotel_id not in grouped_rooms:
            grouped_rooms[hotel_id] = []
        grouped_rooms[hotel_id].append(room)
    return grouped_rooms

def call_llm_api(hotel_id, hotel_room_text, mapping_room_text):
    url = '<API_URL>'
    headers = {
        'Authorization': 'Bearer <API_KEY>',
        'Content-Type': 'application/json'
    }
    room_data = f"Hotel ID: {hotel_id}\nHotel Room Text: {hotel_room_text}\nMapping Room Text: {mapping_room_text}"
    data = {
        "inputs": {
            "room_data": room_data
        },
        "response_mode": "blocking",
    }
    response = requests.post(url, headers=headers, data=json.dumps(data))
    return response.json()

def process(grouped_hotel_rooms, grouped_mapping_rooms, cursor):
    for hotel_id, rooms in grouped_mapping_rooms.items():
        if hotel_id in grouped_hotel_rooms:
            hotel_rooms = grouped_hotel_rooms[hotel_id]
            for mapping_room in rooms:
                mapping_room_text = mapping_room['title']
                for hotel_room in hotel_rooms:
                    hotel_room_text = hotel_room['title']
                    response = call_llm_api(hotel_id, hotel_room_text, mapping_room_text)
                    # Assume the API response has a field 'approved' to indicate match
                    if response.get('approved', False):
                        update_query = "UPDATE mapping_hotel_room SET approved = 1 WHERE id = %s"
                        cursor.execute(update_query, (mapping_room['id'],))
                        print(f"Matching room found for room id {mapping_room['id']} in hotel id {hotel_id}. Approved status updated.")
                        break

def fetch_hotel_room_data(cursor):
    query = "SELECT * FROM hotel_room"
    cursor.execute(query)
    return cursor.fetchall()

def fetch_disapproved_data(cursor):
    query = "SELECT * FROM mapping_hotel_room WHERE approved = 0"
    cursor.execute(query)
    return cursor.fetchall()

def main():
    try:
        connection = mysql.connector.connect(
            host='localhost',
            database='enuygun_hotel_db',
            user='root',
            password='password',  # Ensure to provide the correct password
            port='3306'
        )

        if connection.is_connected():
            cursor = connection.cursor(dictionary=True)
            
            hotel_room_data = fetch_hotel_room_data(cursor)
            disapproved_data = fetch_disapproved_data(cursor)
            
            grouped_hotel_rooms = group_rooms_by_hotel(hotel_room_data)
            grouped_mapping_rooms = group_rooms_by_hotel(disapproved_data)
            
            process(grouped_hotel_rooms, grouped_mapping_rooms, cursor)
            
            # Commit the changes after processing
            connection.commit()
                
    except Error as e:
        print("Error while connecting to MySQL", e)
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

if __name__ == "__main__":
    main()
