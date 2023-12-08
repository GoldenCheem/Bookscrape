# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import pymongo

class BookPipeline:
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        # Strip all whitespaces from strings
        field_names = adapter.field_names()
        for field_name in field_names:
            value = adapter.get(field_name)
            adapter[field_name] = value.strip()

        # Price --> convert string to float
        price_keys = ['price_excl_tax', 'price_incl_tax', 'tax']
        for price_key in price_keys:
            value = adapter.get(price_key)
            value = value.replace('£', '')
            adapter[price_key] = float(value)
        
        # Availability --> extract the number of books in stock
        availability_string = adapter.get('availability')
        split_string_array = availability_string.split('(')
        if len(split_string_array) < 2:
            adapter['availability'] = 0
        else:
            availability_value = split_string_array[1].split(' ')[0]
            adapter['availability'] = int(availability_value)

        # Reviews --> convert string to int
        num_reviews_string = adapter.get('num_reviews')
        adapter['num_reviews'] = int(num_reviews_string)

        # Star_rating --> convert string to int
        star_rating_string = adapter.get('star_rating')
        star_string = star_rating_string.split()[1]
        star_dict = {
            'Zero'  : 0,
            'One'   : 1,
            'Two'   : 2,
            'Three' : 3,
            'Four'  : 4,
            'Five'  : 5,
        }
        star_rating = star_dict[star_string]
        adapter['star_rating'] = star_rating

        return item

class SaveToMongoDBPipeline():
    def __init__(self):
        self.conn = pymongo.MongoClient(
            'localhost',
            27017
        )

        db = self.conn['Booksdata']
        self.collection = db['Books']
        self.collection.delete_many({})

    def process_item(self, item, spider):
        self.collection.insert_one(dict(item))
        return item
        
    def close_spider(self, spider):
        self.conn.close()
