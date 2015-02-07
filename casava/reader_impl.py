import re
import csv
import logging
from itertools import chain

import chardet


log = logging.getLogger(__name__)
re_newline = re.compile('(\r(?=[^\n]))|\r\n')


class reader(object):
    ENC_DETECTION_SIZE = 10 * 1024
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
        encoding = self._detect_encoding()
        log.info('detect encoding: %r', encoding)
        cur_line_iter = line_iter(self.content_iter)
        sep_char, cur_line_iter = self._detect_sep(cur_line_iter)
        log.info('detect delimiter: %r', sep_char)
        rdr = csv.reader(cur_line_iter, delimiter=sep_char)
        try:
            while True:
                row = rdr.next()
                yield self._decode_row(row, encoding)
        except StopIteration:
            pass

    def _detect_encoding(self):
        content_header = accumulate_bytes(self.content_iter, self._enc_detection_size)
        encoding = chardet.detect(content_header)
        self.content_iter = chain([content_header], self.content_iter)
        return encoding['encoding']

    def _detect_sep(self, it):
        lines = accumulate_lines(it, self._sep_detection_size)
        sep_variances = {}
        for sep_char in self.SEP_CHARS:
            rdr = csv.reader(lines, delimiter=sep_char)
            cell_lengths = []
            for row in rdr:
                if len(row) > 1:
                    cell_lengths += [len(cell) for cell in row]
            if cell_lengths:
                sep_variances[sep_char] = variance(cell_lengths)
        if not sep_variances:
            return ',', chain(lines, it)
        for k in sep_variances:
            sep_variances[k] += 0.1
        # Prefer , if it's available and not too bad
        if not sep_variances:   # There were no separations
            best_sep = ','
        else:
            best_sep = min((item for item in sep_variances.items()), key=lambda (sep,var): var)
        if ',' in sep_variances and best_sep[1] / sep_variances[','] > 0.9:
            best_sep_char = ','
        else:
            best_sep_char = best_sep[0]
        new_iter = chain(lines, it)
        return best_sep_char, new_iter

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


def line_iter(content_iter):
    '''Iterate over lines separated by an eol character'''
    cur_buf = ''
    for chunk in content_iter:
        cur_buf += chunk
        cur_buf = re_newline.sub('\n', cur_buf)
        lines = cur_buf.split('\n')
        for line in lines[:-1]:
            yield line + '\n'
        cur_buf = lines[-1]
    if cur_buf:
        for line in cur_buf.split('\n'):
            yield line + '\n'


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

def accumulate_lines(it, count):
    '''Accumulate up to `count` lines from a line iterator'''
    result = []
    try:
        while len(result) < count:
            result.append(it.next())
    except StopIteration:
        pass
    return result


def variance(items):
    mean = 1.0 * sum(items) / len(items)
    return sum((it-mean)**2 for it in items) / len(items)
