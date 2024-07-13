import random
import csv

# Lists of item components to generate varied names
adjectives = ['Large', 'Small', 'Deluxe', 'Basic', 'Premium', 'Eco-friendly', 'Compact', 'Heavy-duty', 'Lightweight', 'Foldable']
items = ['Shovel', 'Hose', 'Gloves', 'Pruner', 'Rake', 'Trowel', 'Sprayer', 'Pot', 'Planter', 'Fertilizer', 'Seeds', 'Mulch', 'Soil', 'Tool Set', 'Watering Can', 'Wheelbarrow', 'Sprinkler', 'Hedge Trimmer', 'Lawn Mower', 'Leaf Blower']
brands = ['GardenPro', 'EcoGrow', 'GreenThumb', 'NatureCare', 'BloomMaster', 'YardKing', 'PlantPal', 'GrowWell', 'EarthMate', 'LeafLover']

def generate_item():
    product = f"{random.choice(adjectives)} {random.choice(items)} - {random.choice(brands)}"
    stock = random.randint(1, 200)
    sold = random.randint(1, 100)
    revenue = f'£{round(random.uniform(0.1, 100), 2)}'
    print(f'{product},{stock},{sold},{revenue}')

for _ in range(10000):
    generate_item()
