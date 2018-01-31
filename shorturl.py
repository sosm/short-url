import cherrypy
import sqlite3
import urllib
import json
import mod_wsgi
from threading import Lock

"""
Minimaliztic short URL service.

Works on a site base, that is, you need to define base URLs that are
allowed to use the service in the configuration.
"""

# first letter made up of 32 letters (5bit)
URL_FIRST_LEN = 32
URL_FIRST_BITS = 5
URL_FIRST = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ012345'
# other positions made up of 64 characters (6bit)
URL_OTHERS_LEN = 64
URL_OTHERS_BITS = 6
URL_OTHERS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz0123456789-'

# database connecton, one per thread
def connect(thread_index): 
    # Create a connection and store it in the current thread 
    cherrypy.thread_data.db = sqlite3.connect(cherrypy.config['db.database'])
 
cherrypy.engine.subscribe('start_thread', connect)

# make sure the database has the correct schema
def setup_db(k, v):
    """Check if the DB already exist or create a new one
       setting up the required table.
    """
    if k == 'database':
        conn = sqlite3.connect(v)
        c = conn.cursor()
        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='urls'")
        if c.fetchone() is None:
            c.execute("CREATE TABLE urls (siteid INTEGER, url TEXT)")
            conn.commit()
        conn.close()


cherrypy.config.namespaces['db'] = setup_db

class ShortUrl(object):

    def __init__(self):
        self.writelock = Lock()


    def id2url(self, num):
        """Converts a numerical id into a short URL string.
        """
        out = URL_FIRST[num & (URL_FIRST_LEN-1)]
        num = num >> URL_FIRST_BITS

        while num > 0:
            out = out + URL_OTHERS[num & (URL_OTHERS_LEN-1)]
            num = num >> URL_OTHERS_BITS

        return out

    def url2id(self, url):
        out = 0
        for c in url[:0:-1]:
            newid = URL_OTHERS.find(c)
            if newid < 0:
                return None
            out = (out << URL_OTHERS_BITS) + newid

        out = (out << URL_FIRST_BITS) + URL_FIRST.find(url[0])
        if out < 0:
            return None


        return out

    def sanatize_url(self, url):
        # TODO actually implement this
        return url


    @cherrypy.expose
    def default(self, *query, **kwargs):
        if len(query) > 1:
            raise cherrypy.HTTPError(404)
        urlid = self.url2id(query[0])
        if urlid is None:
            raise cherrypy.HTTPError(404)

        c = cherrypy.thread_data.db.cursor()
        c.execute('SELECT siteid, url FROM urls WHERE rowid = ?', (urlid,))
        res = c.fetchone()
        if res is None:
            raise cherrypy.HTTPError(404)

        fullurl = 'https://%s%s' % (
                 cherrypy.request.app.config['ShortUrls'][str(res[0])],
                 res[1])

        raise cherrypy.HTTPRedirect(fullurl)

    @cherrypy.expose
    def get(self, **kwargs):
        if 'url' not in kwargs:
            raise cherrypy.HTTPError(400)

        url = kwargs['url']
        if url.startswith('http://'):
            url = url[7:]
        if url.startswith('https://'):
            url = url[8:]

        siteid = None
        for k,v in cherrypy.request.app.config['ShortUrls'].iteritems():
            if url.startswith(v):
                siteid = int(k)
                url = self.sanatize_url(url[len(v):])
                break
        if siteid is None or not url:
            raise cherrypy.HTTPError(403)

        c = cherrypy.thread_data.db.cursor()
        with self.writelock:
            c.execute('INSERT INTO urls (siteid, url) VALUES(?,?)',
                        (siteid, url))
            c.execute('SELECT last_insert_rowid()')
            row = c.fetchone()
            cherrypy.thread_data.db.commit()
            
        if row is None:
            raise cherrypy.HTTPError(500)


        shortid = row[0]
        shorturl = self.id2url(shortid)
        backid = self.url2id(shorturl)

        cherrypy.response.headers['Content-Type'] = 'text/json'
            
        result = json.dumps({'ShortURL' : "%s/%s" % (
                             cherrypy.request.base, shorturl)})

        if 'jsonp' in kwargs:
            result = '%s(%s)' % (kwargs['jsonp'], result)

        return result

        
# Setup WSGI stuff


if cherrypy.__version__.startswith('3.0') and cherrypy.engine.state == 0:
    cherrypy.engine.start(blocking=False)
    atexit.register(cherrypy.engine.stop)

cherrypy.config.update(mod_wsgi.process_group + '.cfg')
application = cherrypy.Application(ShortUrl(), script_name=None, 
                config=mod_wsgi.process_group + '.cfg')

