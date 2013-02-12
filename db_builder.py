import time, pickle, lxml.etree, key_secret
from pymongo import Connection
import pandas as pd
import numpy as np

pkl_file = open('data.pkl', 'rb')
user_ids = pickle.load(pkl_file)
pkl_file.close()

connection = Connection('mongodb://localhost:27017')['test']
db_1 = connection.goodreads_reviews
#db_2 = connection.goodreads_books

key = key_secret.all['key']
url = 'http://www.goodreads.com/review/list/%s.xml?key=%s&v=2&shelf=read&per_page=200&page=%s'
parser = lxml.etree.HTMLParser(encoding='utf-8')

def get_data_from_page(user_id):
    end_of_list = False
    i = 1
    while not end_of_list:
        tree = lxml.etree.parse(url % (user_id, key, i), parser)
        book_id = [x.text for x in tree.xpath('//book//id') if x.getparent().tag=='book']
        rating = [x.text for x in tree.xpath('//rating')]
        temp_dict = [{'user_id': user_id, 
                      'book_id': float(book_id[i]),
                      'rating': float(rating[i])} for i in range(len(book_id))]
        for element in temp_dict:
            db_1.save(element)
        end_of_list = tree.xpath('//review[@end]') == tree.xpath('//review[@total]')
        i += 1
        time.sleep(1)

# store all the data in mongoDB. The exception captures error messages from public profiles 
for user_id in user_ids:
    try:
        get_data_from_page(user_id)
        print "user %s done!" % user_id
    except:
        print "user %s non-public!" % user_id

# put all the data from the mongoDB into a pandas dataframe.
cursor = db_1.find()
fields = ['user_id', 'book_id', 'rating']
result = pd.DataFrame(list(cursor), columns=fields)

# => 285601 rows, 1998 users, 135097 books.

result.rating = result.rating.map(lambda x: float(x))
result.rating = result.rating.replace(0, np.nan)
result.rating.value_counts()
#  4:    90866
#  5:    73009
#  3:    64875
#  2:    19880
#  1:     6075

# drop non-availables and duplicates
result = result.dropna()
result = result.drop_duplicates()

# => 254705 rows, 1943 users, 121303 books.

# going to long form, slice up to make pivoting bearable.
temp  = result[:25000].pivot('user_id', 'book_id', 'rating')
#Index: 222 entries, 37501 to 3165440 => 222 users.
#Columns: 19481 entries, 1 to 999954 => 19481 books.
#dtypes: float64(19481)

