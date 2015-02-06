import csv
from itertools import chain

import chardet


class reader(object):
    EOL_DETECTION_SIZE = 10 * 1024
    SEP_DETECTION_SIZE = 1024
    EOL_CHARS = ['\n', '\r\n', '\r']
    SEP_CHARS = [',', ';', '\t']

    def __init__(
            self,
            content_iter,
            eol_detection_size=None,
            sep_detection_size=None):
        self.content_iter = content_iter
        self._eol_detection_size = eol_detection_size or self.EOL_DETECTION_SIZE
        self._sep_detection_size = sep_detection_size or self.SEP_DETECTION_SIZE

    def __iter__(self):
        encoding, eol_char = self._detect_encoding_eol()
        cur_line_iter = line_iter(self.content_iter, eol_char)
        sep_char, cur_line_iter = self._detect_sep(cur_line_iter)
        rdr = csv.reader(cur_line_iter, delimiter=sep_char)
        try:
            while True:
                row = rdr.next()
                yield row
        except StopIteration:
            pass

    def _detect_encoding_eol(self):
        '''(encoding, eol_char) = self.detect_eol()

        Try to detect the end-of-line character in a content iterator.
        This will reassign the content_iter
        '''
        content_header = accumulate_bytes(self.content_iter, self._eol_detection_size)
        encoding = chardet.detect(content_header)
        eol_variances = {}
        for eol in self.EOL_CHARS:
            lines = content_header.split(eol)[:-1]
            if not lines:
                continue
            lengths = [len(line) for line in lines]
            eol_variances[eol] = variance(lengths)
        best_eol = min((item for item in eol_variances.items()), key=lambda (eol,var): var)
        self.content_iter = chain([content_header], self.content_iter)
        return encoding['encoding'], best_eol[0]

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
        if not sep_variances:   # There were no separations
            best_sep = ','
        else:
            best_sep = min((item for item in sep_variances.items()), key=lambda (eol,var): var)
        new_iter = chain(lines, it)
        return best_sep[0], new_iter

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


def line_iter(content_iter, eol):
    '''Iterate over lines separated by an eol character'''
    cur_buf = ''
    for chunk in content_iter:
        cur_buf += chunk
        lines = cur_buf.split(eol)
        for line in lines[:-1]:
            yield line + eol
        cur_buf = lines[-1]
    if cur_buf:
        for line in cur_buf.split(eol):
            yield line + eol


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
