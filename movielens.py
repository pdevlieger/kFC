from pymongo import Connection
from math import sqrt

connection = Connection('mongodb://localhost:27017')['test']
db_ratings = connection.movielens_ratings
db_movies = connection.movielens_movies

def clean_string(string):
    # Look at doing this with decoding the strings!
    return string.replace('\xe9','e').replace('\xe8','e').replace('\xc1','a').replace('\xf6','o')

def reviews_to_db(path='ml-100k/'):
    for line in open(path+'u.data'):
        user_id, movie_id, rating, ts = line.split('\t')
        db_ratings.save({'user_id': int(user_id), 'movie_id': int(movie_id), 'rating': float(rating)})

def movies_to_db(path='ml-100k/'):
    for line in open(path+'u.item'):
        movie_id, movie_title = line.split('|')[0:2]
        db_movies.save({'movie_id': int(movie_id), 'title': clean_string(movie_title)})

def get_rating_dict(corr_item, across_item, id):
    return {i[across_item]: i['rating'] for i in db_ratings.find({corr_item: id})}

def sim_pearson(ratings_1, ratings_2):
    si={}
    for item in ratings_1:
        if item in ratings_2: si[item]=1.0
    
    n=len(si)
    if n==0: return 0.0
    
    sum1=sum([ratings_1[it] for it in si])
    sum2=sum([ratings_2[it] for it in si])
    sum1Sq=sum([pow(ratings_1[it],2) for it in si])
    sum2Sq=sum([pow(ratings_2[it],2) for it in si])
    pSum=sum([ratings_1[it]*ratings_2[it] for it in si])
    
    num=pSum-(sum1*sum2/n)
    den=sqrt((sum1Sq-pow(sum1,2)/n)*(sum2Sq-pow(sum2,2)/n))
    if den==0: return 0.0
    r=num/den
    
    return r

def sim_matrix(corr_item, across_item, sim=sim_pearson):
    item_vector = [i[corr_item] for i in db_movies.find()]
    corr_matrix = np.zeros(shape=(len(item_vector), len(item_vector)))
    for item in item_vector:
        db_movies.find({corr_item: item})
    for i in range(len(item_vector)):
        j=i
        while j<len(item_vector):
            ratings_1 = get_rating_dict(corr_item, across_item, item_vector[i])
            ratings_2 = get_rating_dict(corr_item, across_item, item_vector[j])
            corr_matrix[i][j]=corr_matrix[j][i]=sim(ratings_1, ratings_2)
            j+=1
        print 'Done! %s / %s' % (i, len(item_vector))
    return corr_matrix

def update_corr(corr_item, across_item, movie_id, sim):
    item_vector = [i[corr_item] for i in db_movies.find()]
    corr = []
    for item in item_vector:
        if item!=movie_id:
            ratings_1 = get_rating_dict(corr_item, across_item, item)
            ratings_2 = get_rating_dict(corr_item, across_item, movie_id)
            corr.append((item, sim(ratings_1, ratings_2)))
    db_movies.update({corr_item: movie_id},
                     {'$set': {'distances': corr}})

def set_correlations_per_movie():
    item_vector = [i['movie_id'] for i in db_movies.find()]
    for item in item_vector: 
        update_corr('movie_id', 'user_id', item, sim=sim_pearson)
        print 'Done!'
    print 'All done!'