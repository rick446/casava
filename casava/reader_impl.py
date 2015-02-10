import re
import csv
import logging
from itertools import chain

import chardet


log = logging.getLogger(__name__)


class reader(object):
    ENC_DETECTION_SIZE = 100 * 1024
    SEP_DETECTION_SIZE = 1024
    SEP_CHARS = [',', ';', '\t']

    def __init__(
            self,
            content_iter,
            enc_detection_size=None,
            sep_detection_size=None):
        self.content_iter = content_iter
        self._enc_detection_size = enc_detection_size or self.ENC_DETECTION_SIZE
        self._sep_detection_size = sep_detection_size or self.SEP_DETECTION_SIZE

    def __iter__(self):
        encoding, dialect = self._detect_encoding_dialect()
        log.info('detect encoding: %r', encoding)
        log.info('detect eol     : %r', dialect.lineterminator)
        log.info('detect delim   : %r', dialect.delimiter)
        cur_line_iter = ilines(self.content_iter)
        rdr = csv.reader(cur_line_iter, dialect)
        try:
            while True:
                row = rdr.next()
                yield self._decode_row(row, encoding)
        except StopIteration:
            pass

    def _detect_encoding_dialect(self):
        content_header = accumulate_bytes(self.content_iter, self._enc_detection_size)
        encoding = chardet.detect(content_header)
        try:
            dialect = csv.Sniffer().sniff(content_header, delimiters=',;|\t\x1f')
        except csv.Error:
            dialect = csv.excel
        self.content_iter = chain([content_header], self.content_iter)
        return encoding['encoding'], dialect

    def _decode_row(self, row, encoding):
        result = []
        if encoding is None:
            return [auto_unicode(b_cell) for b_cell in row]
        for b_cell in row:
            try:
                u_cell = unicode(b_cell, encoding)
            except UnicodeDecodeError:
                u_cell = auto_unicode(b_cell)
            result.append(u_cell)
        return result


def auto_unicode(bytes):
    '''Guaranteed to return valid unicode string'''
    enc = chardet.detect(bytes)
    if enc['encoding']:
        return bytes.decode(enc['encoding'], 'ignore')
    else:
        return bytes.decode('utf-8', 'ignore')


# From http://code.activestate.com/recipes/286165-ilines-universal-newlines-from-any-data-source/
def ilines(source_iterable):
    '''yield lines as in universal-newlines from a stream of data blocks'''
    tail = ''
    for block in source_iterable:
        if not block:
            continue
        if tail.endswith('\015'):
            yield tail[:-1] + '\012'
            if block.startswith('\012'):
                pos = 1
            else:
                tail = ''
        else:
            pos = 0
        try:
            while True: # While we are finding LF.
                npos = block.index('\012', pos) + 1
                try:
                    rend = npos - 2
                    rpos = block.index('\015', pos, rend)
                    if pos:
                        yield block[pos : rpos] + '\n'
                    else:
                        yield tail + block[:rpos] + '\n'
                    pos = rpos + 1
                    while True: # While CRs 'inside' the LF
                        rpos = block.index('\015', pos, rend)
                        yield block[pos : rpos] + '\n'
                        pos = rpos + 1
                except ValueError:
                    pass
                if '\015' == block[rend]:
                    if pos:
                        yield block[pos : rend] + '\n'
                    else:
                        yield tail + block[:rend] + '\n'
                elif pos:
                    yield block[pos : npos]
                else:
                    yield tail + block[:npos]
                pos = npos
        except ValueError:
            pass
        # No LFs left in block.  Do all but final CR (in case LF)
        try:
            while True:
                rpos = block.index('\015', pos, -1)
                if pos:
                    yield block[pos : rpos] + '\n'
                else:
                    yield tail + block[:rpos] + '\n'
                pos = rpos + 1
        except ValueError:
            pass

        if pos:
            tail = block[pos:]
        else:
            tail += block
    if tail:
        yield tail


def accumulate_bytes(it, size):
    '''Accumulate around `size` bytes from the content iterator'''
    cur_buf = []
    cur_size = 0
    try:
        while cur_size < size:
            chunk = it.next()
            cur_buf.append(chunk)
            cur_size += len(chunk)
    except StopIteration:
        pass
    return ''.join(cur_buf)
