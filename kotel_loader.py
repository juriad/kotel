import hashlib
import xml.etree.ElementTree as etree
from io import BytesIO
from urllib.parse import urlencode

import pycurl


class KotelLoader:
    def __init__(self, domain, password, type_override, logger):
        self.domain = domain
        self.password = password
        self.logger = logger
        self.type_override = type_override

        self.curl = None

    def _create_curl(self):
        self.logger.debug('Creating curl')
        c = pycurl.Curl()
        c.setopt(pycurl.COOKIEFILE, '')
        c.setopt(pycurl.TIMEOUT, 3)
        c.setopt(pycurl.WRITEFUNCTION, lambda x: None)
        return c

    def _login_curl(self, c):
        c.setopt(pycurl.URL, self.domain + 'syswww/login.xml')
        c.setopt(pycurl.FOLLOWLOCATION, True)
        c.perform()
        token = c.getinfo(pycurl.INFO_COOKIELIST)[0].split()[-1]
        self.logger.debug('The login token is %s', token)

        c.setopt(pycurl.URL, self.domain + 'syswww/LOGIN.XML')
        sha1 = hashlib.sha1()
        sha1.update(token.encode('utf-8'))
        sha1.update(self.password.encode('utf-8'))
        sha1_hash = sha1.hexdigest()
        post_data = {'USER': 'user',
                     'PASS': sha1_hash}
        post_fields = urlencode(post_data)
        c.setopt(pycurl.POSTFIELDS, post_fields)
        c.perform()
        self.logger.debug('Logged in using pass=%s', sha1_hash)

    def _page_curl(self, c, page, type_override):
        buffer = BytesIO()
        c.setopt(pycurl.URL, self.domain + page)
        c.setopt(c.WRITEDATA, buffer)
        c.perform()
        content = buffer.getvalue().decode('utf-8')
        self.logger.debug('Loaded page %s', page)

        def retype(_n, v):
            if _n in type_override:
                return type_override[_n](v)

            if 'BOOL' in _n:
                return int(v)
            elif 'REAL' in _n:
                return float(v)
            elif 'INT' in _n:
                return int(v)
            else:
                return v

        inputs = {
            i.attrib['NAME']: retype(i.attrib['NAME'], i.attrib['VALUE'])
            for i in etree.fromstring(content).findall('INPUT')
        }
        self.logger.debug('Found %d inputs in page %s', len(inputs), page)
        return inputs

    def _destroy_curl(self, c):
        self.logger.debug('Destroying curl')
        c.close()

    pages = {
        'h': 215,  # heating
        'w': 213,  # hot water
        'c': 214,  # compressor
        'b': 218,  # electric boiler
        't': 210,  # temperatures
        's': 211  # statuses
    }

    def _do_load(self, pages, type_override, c):
        pages_dict = {
            p: self._page_curl(c, 'PAGE%d.XML' % _n, type_override)
            for (p, _n) in pages.items()
        }
        return pages_dict

    def _load(self, pages, type_override):
        if self.curl is not None:
            try:
                return self._do_load(pages, type_override, self.curl)
            except Exception:
                self.curl = None

        self.curl = self._create_curl()
        self._login_curl(self.curl)
        return self._do_load(pages, type_override, self.curl)

    def load_pages(self, pages=None, type_override=None):
        if pages is None:
            pages = self.pages
        if type_override is None:
            type_override = self.type_override

        self.logger.info('Loading pages from %s', self.domain)

        pages_dict = self._load(pages, type_override)

        # self._destroy_curl(c)
        self.logger.info('Loaded %d pages into dicts from %s', len(pages_dict), self.domain)
        return pages_dict
