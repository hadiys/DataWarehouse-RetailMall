import random
import pandas as pd
from shapely.geometry import Point, Polygon
import json
import csv
from faker import Faker
from pyproj import Transformer
from datetime import timedelta, datetime, time

faker = Faker()
num_zones = 6
num_shops = 10
num_visitors = 100
num_journeys = 30
generated_transactions = 0
generated_journeys = 0

zone_data = []
shop_data = []
visitor_data = [] 
journey_data = []
transaction_data = []

shop_dept = [
    {"shop": "Food Court", "department": "Food & Beverages"},
    {"shop": "Superdry", "department": "Apparel & Accessories"},
    {"shop": "JD Sports", "department": "Footwear"},
    {"shop": "Sephora", "department": "Beauty & Personal Care"},
    {"shop": "Apple", "department": "Electronics & Gadgets"},
    {"shop": "IKEA", "department": "Home & Furniture"},
    {"shop": "Decathlon", "department": "Sports & Outdoor"},
    {"shop": "Gamestop", "department": "Toys & Games"},
    {"shop": "Carrefour", "department": "Food & Beverages"},
    {"shop": "Tiger", "department": "Gifts & Novelties"},
    {"shop": "Swarovski", "department": "Jewelry & Watches"},
]

with open("../geo_data/DubaiMallShops.json", "r") as file, \
    open("../geo_data/DubaiMallZones.json", "r") as file2:
    shop_geodata = json.load(file)
    zone_geodata = json.load(file2)

transformer = Transformer.from_crs("EPSG:4326", "EPSG:32633", always_xy=True)

mall_boundary = Polygon(shop_geodata[0]["area"]["boundary"]["coordinates"][0])
entrances = [transformer.transform(e[0], e[1]) for e in [[55.2796850, 25.1987192, 0.0], [55.2783326, 25.1988360, 0.0], [55.2805884, 25.1975599, 0.0]]]


def transform_to_epsg4326(p):
    transformer = Transformer.from_crs("EPSG:32633", "EPSG:4326", always_xy=True)
    
    if(type(p) == Polygon):
        p = Polygon([transformer.transform(point[0], point[1]) for point in p.exterior.coords])
    else:
        p = Point(transformer.transform(p.x, p.y))
    return p

def get_random_entrance():
    transformer = Transformer.from_crs("EPSG:4326", "EPSG:32633", always_xy=True)
    e = Point(entrances[random.randint(0, 2)])

    return e.x, e.y

def generate_random_location_within_polygon(polygon):
    min_x, min_y, max_x, max_y = polygon.bounds

    while True:
        p = Point(random.uniform(min_x, max_x), random.uniform(min_y, max_y))
        if polygon.contains(p):
            return p.x, p.y

def generate_zones():
    for index, row in enumerate(zone_geodata):
        zone = {
            "id": index + 1,
            "name": f'Z{index+1}' ,
            "boundary": Polygon(row["zone"]["boundary"]["coordinates"][0]),
            "has_entrance": 0 if index+1 == 6 else 1,
            "has_adverts": 0 if index+1 == 6 else 1
        }
        zone_data.append(zone)

def generate_shops():   
    for index, row in enumerate(shop_geodata):
        if row["area"]["name"] != "Mall Bounds":
            index -= 1
            name = shop_dept[index]["shop"]
            department = shop_dept[index]["department"]
            boundary = Polygon(row["area"]["boundary"]["coordinates"][0])
            zone_id = next((zone["id"] for zone in zone_data if zone["boundary"].intersects(boundary)), 0)
            
            shop = {
                "id": index + 1,
                "name": name,
                "zone_id": zone_id,
                "department": department,
                "boundary": boundary,
            }
            shop_data.append(shop)

def generate_visitors():
    for i in range(num_visitors):
        visitor = {
            "visitor": {
                "id": i+1,
                "gender": random.choice(["M", "F"]),
                "age": random.choice([0, random.randint(18, 65)]),
                "income_level": random.choice([0, random.randint(20000, 200000)]),
                "device_id": faker.sha1()
            },
            "help_info": {
                "cc": faker.sha1()
            }
        }

        visitor_data.append(visitor)
        
def generate_transaction(visitor, journey):
    global generated_transactions
    generated_transactions += 1
    
    transaction = {
        "id": generated_transactions,
        "shop_id": journey["shop_id"],
        "visitor_id": visitor["visitor"]["id"],
        "amount": random.choices([random.randint(10, 100), random.randint(100, 200),  random.randint(200, 300), random.randint(300, 500), random.randint(500, 1000)], weights=[0.8, 0.025, 0.025, 0.025, 0.025], k=1)[0],
        "datetime": journey["timestamp"],
        "payment_type": random.choice(["Cash", "CreditCard"]),
    }

    if transaction["payment_type"] == "CreditCard":
        transaction["hashed_creditcard"] = random.choices([visitor["help_info"]["cc"], faker.sha1()], weights=[0.95, 0.05], k=1)[0]

    transaction_data.append(transaction)

def generate_journey():
    global generated_journeys
   
    for i in range(num_visitors):
        visitor = visitor_data[random.randint(0, 99)]

        # determine entry_timestamp
        entry_timestamp = datetime.combine(faker.date_this_year(after_today=True), time(9, 0, 0)) + timedelta(minutes=random.randint(1, 720), seconds=random.randint(0, 59))
        
        # generate exit_timestamp
        exit_timestamp = entry_timestamp + timedelta(minutes=random.randint(1, ((20 - entry_timestamp.hour) * 60) + (61 - entry_timestamp.minute)), seconds=random.randint(0, 59))
        
        #calculate valid time window so that its within entry and exit
        valid_time_window = exit_timestamp - entry_timestamp
        
        # determine num_journey (or num of pings the visitor will accumulate during their journey in the mall)
        num_journeys = round((valid_time_window.seconds/60)/3, 1) # ping a visitor every 3 mins which determine the num_journey points
        
        # set the times_increment based on how many pings are needed to generate consistent ping frequency for every user based on how much time they spend in the mall
        time_increment = valid_time_window / num_journeys

        for j in range(round(num_journeys)):
            generated_journeys += 1 
            timestamp = entry_timestamp if j == 0 else timestamp + time_increment
            visitor_id = visitor["visitor"]["id"]
            current_location = Point(get_random_entrance()) if j+1 == num_journeys or j+1 == 1 else Point(generate_random_location_within_polygon(mall_boundary))
            shop_id = next((shop["id"] for shop in shop_data if current_location.intersects(shop["boundary"])), 0)
            
            journey = {
                "id": generated_journeys,
                "visitor_id": visitor_id,
                "shop_id": shop_id,
                "timestamp": timestamp,
                "current_location": current_location
            }
            journey_data.append(journey)

            choice = random.choices([generate_transaction, None], weights=[0.1, 0.9], k=1)[0] if j > 3 else 0
            # If visitor's current location is in a shop
            if shop_id != 0:
                # and visitor decides to buy, then generate a transaction
                if choice:
                    choice(visitor, journey)






generate_zones()
generate_shops()
generate_visitors()
generate_journey()

for zone in zone_data:
    zone["boundary"] = Polygon(transform_to_epsg4326(zone["boundary"])).wkt

for shop in shop_data:
    shop["boundary"] = Polygon(transform_to_epsg4326(shop["boundary"]))

for journey in journey_data:
    journey["current_location"] = Point(transform_to_epsg4326(journey["current_location"]))

zone_df = pd.DataFrame(zone_data)
shop_df = pd.DataFrame(shop_data)
visitor_df = pd.DataFrame([visitor["visitor"] for visitor in visitor_data])
transaction_df = pd.DataFrame(transaction_data)
journey_df = pd.DataFrame(journey_data)

zone_df.to_csv("../generated_data/zones.csv", index=False, quotechar="'", quoting=csv.QUOTE_NONNUMERIC)
shop_df.to_csv("../generated_data/shops.csv", index=False, quotechar="'", quoting=csv.QUOTE_NONNUMERIC)
visitor_df.to_csv("../generated_data/visitors.csv", index=False, quotechar="'", quoting=csv.QUOTE_NONNUMERIC)
journey_df.to_csv("../generated_data/journeys.csv", index=False, quotechar="'", quoting=csv.QUOTE_NONNUMERIC)
transaction_df.to_csv("../generated_data/transactions.csv", index=False, quotechar="'", quoting=csv.QUOTE_NONNUMERIC)