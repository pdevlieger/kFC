from pymongo import Connection
from math import sqrt
import numpy as np
from sys import argv

class Connect(self):
    def __init_(self):
        self.connection = Connection('mongodb://localhost:27017')['test']
        self.db_ratings = connection.movielens_ratings
        self.db_movies = connection.movielens_movies
    
    def clean_string(self, string):
    # Look at doing this with decoding the strings!
        return string.replace('\xe9','e').replace('\xe8','e').replace('\xc1','a').replace('\xf6','o')
    
    def set_database(self, path='ml-100k'):
        for line in open(path+'u.data'):
            user_id, movie_id, rating, ts = line.split('\t')
            self.db_ratings.save({'user_id': user_id, 'movie_id': movie_id, 'rating': float(rating)})

        for line in open(path+'u.item'):
            movie_id, movie_title = line.split('|')[0:2]
            self.db_movies.save({'movie_id': movie_id, 'title': clean_string(movie_title)})

class Ratings(self):
    def __init__(self):
        pass
    
    def sim_pearson(self, ratings_1, ratings_2):
        si={}
        for item in ratings_1:
            if item in ratings_2: si[item]=1.0
        
        n=len(si)
        if n==0: return 0.0
        
#       Added this line because some weird stuff happened with the original definition.
#        if [ratings_2[it] for it in si]==[ratings_1[it] for it in si]: return 1.0
    
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

    def list_movies(self, database_movies):
        return [id['title'] for id in database_movies.find()]

    def dataset(self, database_ratings):
        return [(movie, {i['user_id']: i['rating'] for i in database_ratings.find({'movie_id': movie})}) for movie in list_movies]
    
    def correlation_matrix(self):
        corr_matrix = np.zeros(shape=(len(self.list_movies), len(self.list_movies)))
       
        i=0
        
        for movie in self.list_movies:
            j=0
            for other in list_movies:
                corr_matrix[i][j]=corr_matrix[j][i]=self.sim_pearson(self.dataset[i][1], self.dataset[j][1])
                j+=1
            i+=1
        
        return corr_matrix

    def kFC(title, k=10, p=0.25):
        corr_index = self.list_movies.index(title)
        
        score_list = zip(self.list_movies, corr[corr_index])
        score_list.sort(key=lambda tup: tup[1])
        
        slice = int(len(self.list_movies)*p)
        cut_scores = score_list[slice:]
        
        selector = np.random.randint(len(cut_scores), size=k)
        output = [cut_scores[index] for index in selector]
        output.sort(key=lambda tup: tup[1])
        
        if output[-1][0] == title:
            return output[-2][0]
        return output[-1][0]