import urllib2, base64
from bs4 import BeautifulSoup
import os

URL = raw_input('Enter URL: ')
userName = raw_input('Enter User Name: ')
password = raw_input('Enter ypassword: ')

request = urllib2.Request(URL)
base64string = base64.encodestring('%s:%s' % (userName, password)).replace('\n', '')
request.add_header("Authorization", "Basic %s" % base64string)   
html = urllib2.urlopen(request)

soup = BeautifulSoup(html)

for a in soup.find_all('a', href=True):
    print "Found the URL:", a['href']
    if "equinix-chicago.dirA." in a['href'] and ".gz" in a['href']:
    	cmd = 'wget --user='+userName+' --password='+password+ ' '+URL+a['href']
    	os.system(cmd)
    else:
    	print "Not Founf Matching " + a['href']
