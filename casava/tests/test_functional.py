from cStringIO import StringIO
from nose.tools import assert_equal, assert_less, assert_less_equal, assert_not_equal

from casava import reader_impl


def test_easy():
    fp = StringIO('really,easy,data\nis,really,easy')
    result = list(reader_impl.reader(fp))
    assert_equal(result, [[u'really',u'easy',u'data'], [u'is',u'really',u'easy']])


def test_semi():
    fp = StringIO('really;easy;data\nis;really;easy')
    result = list(reader_impl.reader(fp))
    assert_equal(result, [[u'really',u'easy',u'data'], [u'is',u'really',u'easy']])


def test_tab():
    fp = StringIO('really\teasy\tdata\nis\treally\teasy')
    result = list(reader_impl.reader(fp))
    assert_equal(result, [[u'really',u'easy',u'data'], [u'is',u'really',u'easy']])


def test_cr():
    fp = StringIO('really,easy,data\ris,really,easy')
    result = list(reader_impl.reader(fp))
    assert_equal(result, [[u'really',u'easy',u'data'], [u'is',u'really',u'easy']])


def test_crlf():
    fp = StringIO('really,easy,data\r\nis,really,easy')
    result = list(reader_impl.reader(fp))
    assert_equal(result, [[u'really',u'easy',u'data'], [u'is',u'really',u'easy']])


def test_quoted_newlines():
    fp = StringIO('really,"easy\n",data\r\nis,really,easy')
    result = list(reader_impl.reader(fp))
    assert_equal(result, [[u'really',u'easy\n',u'data'], [u'is',u'really',u'easy']])


def test_weird_unicode():
    zh_data = u'\u5e03\u4f9d\u65cf\u82d7\u65cf'
    lines = [
        '"{}","{}"'.format(zh_data.encode('ascii', 'ignore'), ""),
        '"{}","{}"'.format(zh_data.encode('utf-8'), zh_data.encode('utf-16')),
        ]
    fp = StringIO('\r\n'.join(lines))
    result = list(reader_impl.reader(fp))
    print result

# Test variance
def test_variance_zero():
    zero = [1] * 50
    assert_equal(reader_impl.variance(zero), 0)


def test_variance_one():
    assert_equal(reader_impl.variance([0,2]), 1)


def test_variance_nonnegative():
    assert_less(0, reader_impl.variance([-2, -1]))

# Test line accumulation
def test_acc_lines_small():
    lines = list('12345')
    it_lines = iter(lines)
    prefix = reader_impl.accumulate_lines(it_lines, 2)
    assert_equal(prefix, ['1', '2'])
    remainder = list(it_lines)
    assert_equal(remainder, ['3', '4', '5'])


def test_acc_lines_lg():
    lines = list('12345')
    it_lines = iter(lines)
    prefix = reader_impl.accumulate_lines(it_lines, 10)
    assert_equal(prefix, lines)
    remainder = list(it_lines)
    assert_equal(remainder, [])

# Test byte accumulation
def test_acc_bytes_small():
    lines = 'This is a set of lines of varying length'.split()
    lines = [line + '\n' for line in lines]
    it = iter(lines)
    prefix = reader_impl.accumulate_bytes(it, 20)
    assert_less_equal(20, len(prefix))
    assert_equal(prefix + ''.join(list(it)), ''.join(lines))


def test_acc_bytes_large():
    lines = 'This is a set of lines of varying length'.split()
    lines = [line + '\n' for line in lines]
    it = iter(lines)
    prefix = reader_impl.accumulate_bytes(it, 200)
    assert_less_equal(len(prefix), 200)
    assert_equal([], list(it))
    assert_equal(prefix, ''.join(lines))


# Test line iteration
def test_iter_lines():
    lines = 'This is a set of lines of varying length'.split()
    lines = [line + '\n' for line in lines]
    content = ''.join(lines)
    content_iter = (''.join(ch) for ch in _chunk(content, 4))
    new_lines = list(reader_impl.line_iter(content_iter, '\n'))
    assert_equal(lines, new_lines)


# Test auto unicode
def test_auto_unicode():
    zh_utf16_bytes = '\xff\xfe\x03^\x9dO\xcfe\xd7\x82\xcfe'
    zh_uni = zh_utf16_bytes.decode('utf-16LE')
    assert_equal(reader_impl.auto_unicode(zh_utf16_bytes), zh_uni)
    zh_utf16_bytes_bad = '\xff\xfex03^\x9dOxcfe\xd7\x82\xcfe'
    assert_not_equal(reader_impl.auto_unicode(zh_utf16_bytes_bad), zh_uni)





def _chunk(iterator, chunksize):
    chunk = []
    iterator = iter(iterator)
    while True:
        try:
            chunk.append(iterator.next())
        except StopIteration:
            if chunk:
                yield chunk
            break
        if len(chunk) >= chunksize:
            yield chunk
            chunk = []


