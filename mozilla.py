
import os
import md5
import urllib
import urllib2
import mimetypes
# from gzip import GzipFile
import cStringIO
from cPickle import loads, dumps
import cookielib
import pandas as pd


class MozillaCacher(object):

    def __init__(self, cachedir='.cache'):
        self.cachedir = cachedir
        if not os.path.isdir(cachedir):
            os.mkdir(cachedir)

    def name2fname(self, name):
        return os.path.join(self.cachedir, name)

    def __getitem__(self, name):
        if not isinstance(name, str):
            raise TypeError()
        fname = self.name2fname(name)
        if os.path.isfile(fname):
            return file(fname, 'rb').read()
        else:
            raise IndexError()

    def __setitem__(self, name, value):
        if not isinstance(name, str):
            raise TypeError()
        fname = self.name2fname(name)
        if os.path.isfile(fname):
            os.unlink(fname)
        f = file(fname, 'wb+')
        try:
            f.write(value)
        finally:
            f.close()

    def __delitem__(self, name):
        if not isinstance(name, str):
            raise TypeError()
        fname = self.name2fname(name)
        if os.path.isfile(fname):
            os.unlink(fname)

    def __iter__(self):
        raise NotImplementedError()

    def has_key(self, name):
        return os.path.isfile(self.name2fname(name))


class MozillaEmulator(object):
    def __init__(self, cacher={}, trycount=0):

        self.cacher = cacher
        self.cookies = cookielib.CookieJar()
        self.debug = False
        self.trycount = trycount

    def _hash(self, data):
        h = md5.new()
        h.update(data)
        return h.hexdigest()

    def build_opener(self, url, postdata=None, extraheaders={}, forbid_redirect=False):
        txheaders = {
            'Accept': 'text/xml,application/xml,application/xhtml+xml,text/html;q=0.9,text/plain;q=0.8,image/png,*/*;q=0.5',
            'Accept-Language': 'en,hu;q=0.8,en-us;q=0.5,hu-hu;q=0.3',
            #            'Accept-Encoding': 'gzip, deflate',
            'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.7',
            #            'Keep-Alive': '300',
            #            'Connection': 'keep-alive',
            #            'Cache-Control': 'max-age=0',
        }
        for key, value in extraheaders.iteritems():
            txheaders[key] = value
        req = urllib2.Request(url, postdata, txheaders)
        self.cookies.add_cookie_header(req)

        redirector = urllib2.HTTPRedirectHandler()

        http_handler = urllib2.HTTPHandler(debuglevel=self.debug)
        https_handler = urllib2.HTTPSHandler(debuglevel=self.debug)

        u = urllib2.build_opener(http_handler, https_handler, urllib2.HTTPCookieProcessor(self.cookies), redirector)
        u.addheaders = [
            ('User-Agent', 'Mozilla/5.0 (Windows; U; Windows NT 5.1; hu-HU; rv:1.7.8) Gecko/20050511 Firefox/1.0.4')]
        if not postdata is None:
            req.add_data(postdata)
        return (req, u)

    def download(self, url, postdata=None, extraheaders={}, forbid_redirect=False,
                 trycount=None, fd=None, onprogress=None, only_head=False):

        if trycount is None:
            trycount = self.trycount
        cnt = 0
        openerdirector = None
        while True:
            try:
                key = self._hash(url)
                if (self.cacher is None) or (not self.cacher.has_key(key)):
                    req, u = self.build_opener(url, postdata, extraheaders, forbid_redirect)
                    openerdirector = u.open(req)
                    if self.debug:
                        print req.get_method(), url
                        print openerdirector.code, openerdirector.msg
                        print openerdirector.headers
                    self.cookies.extract_cookies(openerdirector, req)
                    if only_head:
                        return openerdirector
                    if openerdirector.headers.has_key('content-length'):
                        length = long(openerdirector.headers['content-length'])
                    else:
                        length = 0
                    dlength = 0
                    if fd:
                       pass
                else:
                    data = self.cacher[key]
                # try:
                #    d2= GzipFile(fileobj=cStringIO.StringIO(data)).read()
                #    data = d2
                # except IOError:
                #    pass
                return openerdirector
            except urllib2.URLError:
                cnt += 1
                if (trycount > -1) and (trycount < cnt):
                    raise
                # Retry :-)
                if self.debug:
                    print "MozillaEmulator: urllib2.URLError, retryting ", cnt









def get_content_type(filename):
    return mimetypes.guess_type(filename)[0] or 'application/octet-stream'


# HOW TO USE

dl = MozillaEmulator()
df = pd.DataFrame.from_csv('test.csv')
print df
for index, row in df.iterrows():
    try:
        res = dl.download(index).code
        row['status'] = True if res == 200 else False

    except urllib2.URLError:
        row['status'] = False

df.to_csv('output2.csv')

