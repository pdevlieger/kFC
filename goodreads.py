import lxml.etree
import pickle

url = 'http://www.goodreads.com/group/220-goodreads-librarians-group/members?format=html&page=%s&sort=first_name'
parser = lxml.etree.HTMLParser(encoding='utf-8')

user_ids = []

def extract_id(string):
	try:
		start = string.index('/user/show/') + len('/user/show/')
		end = start + 1
		while 47 < ord(string[end]) < 58:
			end += 1
		return int(string[start:end])
	except ValueError:
		return None

for number in range(1, 491):
    tree = lxml.etree.parse(url % number, parser)
    for usernode in tree.xpath('//a[@class="userName"]'):
        user_ids.append(extract_id(usernode.attrib['href']+'/'))
        # => adding '/' is necessary for the extract_id to run smoothly (as end can get out of the index.)

# To save this file.
# ------------------
# output = open('data.pkl', 'wb')
# pickle.dump(user_ids, output)
# output.close()