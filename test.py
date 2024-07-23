import mysql.connector
from collections import defaultdict

# Database connection
db = mysql.connector.connect(
    host="localhost",
    user="root",
    database="enuygun_hotel_db",
)

cursor = db.cursor(dictionary=True)

# Step 1: Extract a limited subset of room data for testing
cursor.execute("""
    SELECT hr.hotel_id, hr.id AS hotel_room_id, hr.name AS hotel_room_name, 
           mhr.id AS mapping_room_id, mhr.source_name AS mapping_room_name, 
           mhr.match_rate, mhr.approved, mhr.data_used, mhr.status
    FROM hotel_room hr
    JOIN mapping_hotel_room mhr ON hr.id = mhr.hotel_room_id
    LIMIT 100  -- Fetch only 1000 rows for testing
""")
data = cursor.fetchall()

# Print the fetched data for verification
print("\nJoined Data:")
for row in data:
    print(row)

# Step 2: Group rooms by hotel
hotel_rooms = defaultdict(list)
mapping_rooms_by_hotel = defaultdict(list)
mapping_rooms = defaultdict(list)

for row in data:
    hotel_rooms[row['hotel_id']].append({
        'hotel_room_id': row['hotel_room_id'],
        'hotel_room_name': row['hotel_room_name']
    })
    if row['mapping_room_id']:
        mapping_rooms[row['hotel_room_id']].append({
            'mapping_room_id': row['mapping_room_id'],
            'mapping_room_name': row['mapping_room_name'],
            'match_rate': row['match_rate'],
            'approved': row['approved'],
            'data_used': row['data_used'],
            'status': row['status']
        })
        mapping_rooms_by_hotel[row['hotel_id']].append({
            'mapping_room_id': row['mapping_room_id'],
            'mapping_room_name': row['mapping_room_name'],
            'match_rate': row['match_rate'],
            'approved': row['approved'],
            'data_used': row['data_used'],
            'status': row['status']
        })

combined_data = {}
for hotel_id in hotel_rooms.keys():
    combined_data[hotel_id] = {
        'hotel_rooms': hotel_rooms[hotel_id],
        'mapping_rooms': mapping_rooms_by_hotel.get(hotel_id, [])
    }

# Step 3: Prepare data for further processing or analysis
def prepare_llm_input(hotel_id, hotel_rooms, mapping_rooms):
    return {
        "hotel_id": hotel_id,
        "hotel_rooms": hotel_rooms,
        "mapping_rooms": mapping_rooms
    }

# Print the list of rooms for each hotel for debugging
for hotel_id, data in combined_data.items():
    print(f"\nHotel ID: {hotel_id}")
    print("Hotel Rooms:")
    for room in data['hotel_rooms']:
        print(f"  Room ID: {room['hotel_room_id']}, Room Name: {room['hotel_room_name']}")
    print("Mapping Rooms:")
    for room in data['mapping_rooms']:
        print(f"  Mapping Room ID: {room['mapping_room_id']}, Room Name: {room['mapping_room_name']}, Match Rate: {room['match_rate']}")

# Example of preparing data for one hotel
example_hotel_id = list(combined_data.keys())[0]
llm_input = prepare_llm_input(example_hotel_id, combined_data[example_hotel_id]['hotel_rooms'], combined_data[example_hotel_id]['mapping_rooms'])

# Print summaries for verification
print(f"\nExample Hotel ID: {llm_input['hotel_id']}")
print(f"Number of Hotel Rooms: {len(llm_input['hotel_rooms'])}")
print(f"Number of Mapping Rooms: {len(llm_input['mapping_rooms'])}")
if llm_input['hotel_rooms']:
    print(f"First Hotel Room: {llm_input['hotel_rooms'][0]}")
if llm_input['mapping_rooms']:
    print(f"First Mapping Room: {llm_input['mapping_rooms'][0]}")

db.commit()
cursor.close()
db.close()
