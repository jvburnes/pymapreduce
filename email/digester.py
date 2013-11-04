#!/usr/local/bin/python

import sys, re
import email.parser, email.header
import tarfile
import logging

# my micro parser.  minimal header parser

# Header is attribute:value pair, a unix-from wrapper line or a continuation line
header_format = re.compile(
    r"""^                                             # beginning of line
        (?P<field>                                    # 'field' group
            ([\041-\071\073-\176]+:(?P<blanks>\ *))|  # a header attribute name OR
            (From\ )|                                 # a unix-from wrapper OR
            ([\t ])                                   # a continuation line start
        )                                             # end 'field' group
        (?P<content>.*?)((\r\Z)|(\n\Z)|(\r\n\Z))      # 'content' group
    """, re.VERBOSE|re.DOTALL)

class mDict(dict):
    defects = []
    def __getitem__(self,key):
        return self.get(key, None)

class MicroParser:
    def __init__(self):       # Create a new message and start by parsing headers.
        self.msg = mDict()    # normal python parser returns None on non-existant key
        self.lines = []

    def parse(self,fp,headersonly=True):
        self.fp = fp
        self.headersonly = headersonly

        # Pre-parse the header lines.  Needed because of continuation lines

        while True:
            line = self.fp.readline()
            m = header_format.match(line)
            if not m: break
            # adjust for floating whitespace with 'blanks'
            g = m.group('blanks')
            if g: 
                w = len(g)
            else:
                w = 0 
            self.lines.append((m.group('field'),w,m.group('content')))

        #  Analysis
        curfield = None
        for (field, wspc, content) in self.lines:
            # Is it a continuation?
            if field in ' \t':
                # so append it
                if curfield:
                    # hack for non us-ascii continuations, allows proper decoding later.
                    self.msg[curfield] += (field + content)
                # else it was bogus, ignore it
            # Is it a unix-from? (if so, we're assuming first line?)
            elif field == 'From ':
                # unix-from wrapper
                curfield = 'unixfrom'
                self.msg[curfield] = content
            else:
            # it must be a "normal" header field
                curfield = field[:-1-wspc].lower()
                self.msg[curfield] = content
            
        return self.msg

# convenience function decodes non US-ASCII headers into unicode

def decodeHeader(encodedtxt, default_charset="ascii"):
    decoded_parts = email.header.decode_header(encodedtxt)
    unicode_parts = [unicode(dtext, hdr_charset or default_charset)
                       for dtext, hdr_charset in decoded_parts]
    return u"".join(unicode_parts)

def unfoldHeader(foldedTxt):
    return foldedTxt.replace('\n','')

# This class constructs an iterable parser that digests MSG input streams
#
# (This class is usable as an actor in a LINDA-like producer, consumer scnario.
#  To increase efficiency in map-reducers, send more than one message file to the
#  digester and create a loop in the yield iterator to generate more than one tuple.
#  If the digester is running on a separate processor you may want to parse all of
#  the input files in the constructor and push the tuples into a queue for later
#  yield or parse them all at once and yield a list of tuples.)
# 
class DigestStream:
    def __init__(self, inStream, parser=email.parser.Parser(),unfold=True,decode=False):
        # the message file handle input stream
        self.inStream = inStream

        # The parser defaults to the standard python RFC-2822 parser.
        # If substituting your own parser, plz comply with the
        # interface of email.parser.Parser().parse()
        #  -- returns a "msg" dictionary with header attributes
        #  -- retrieval of non-existent attribute results in None
        #  -- returns a defects list in msg.defects
        #  -- accepts a headersonly boolean skip body parsing/semantics
        #  -- and accepts a readable file handle.  
        #  -- if supplying your own, use "parser = YourParser()" format
        #  -- the instance of your parse should have a parse(fp) method
        #     which accepts the fp input stream handle
        self.Parser = parser

        # RFC 2822 says that header fields should be treated in their
        # "unfolded" form for further processing.  The Python parser leaves
        # that up to us.   We do it by default.  If you don't want that 
        # behavior, set unfold to False and we'll leave it alone.
        self.unfold = unfold

        # decode is off by default, but if enabled will decode non US-ASCII
        # header fields
        self.decode = decode

 
    def __iter__(self):
        return self.msg_iterator()

    def msg_iterator(self):
        # each time we're called, emit another msg digest tuple
	msg = self.Parser.parse(self.inStream,headersonly=True)

        # warn about any defects
        if msg.defects: 
            logging.warning('Found defect(s) in message: %s / %s' % (msg['Message-ID'], msg.defects))

        # construct initial digested field list, use 'Sender:' if present, 'From:' if not
        # use 'Sender:' value if available, else use 'From:' value
        field_list = [msg['date'], msg['sender'] or msg['from'], msg['subject']]

        # optionally decode and unfold non us-ascii headers
        if self.decode:
            field_list = [decodeHeader(field) for field in field_list]

        # by default unfold folded fields (always do this *after* non ascii decoding)
        if self.unfold:
            field_list = [unfoldHeader(field) for field in field_list]

        yield tuple(field_list)

# this object opens a tar.gz file and generates the file handle of each contained file on successive iterations
class MsgStreams:
    def __init__(self, targz):
        self.targz = targz
        self.tar = tarfile.open(self.targz,'r:gz')
            
    def __iter__(self):
        return self.fp_iterator()

    # return a file-like handle to the contained file
    def fp_iterator(self):
        for info in self.tar:
            # make sure its a regular file
            if info.isreg(): 
                # defeat the hackers
                if '..' in info.name or info.name[0]=='/':
                    logging.warning('Alert: tarfile "%s" member "%s" directed to exit normal namespace. Skipping.' % (self.targz, info.name))
                    break
                logging.info('streaming archive file %s' % (info.name))
                yield self.tar.extractfile(info)

# the test code which runs a map/reduce on the test MSG archive

def testParser():
    # setup the logging system
    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

    logging.info('Starting MSG digester test 01...')

    # kick-off one digester stream per msg file.  decode non us-ascii fields 
    m = map(lambda msgFile:DigestStream(msgFile,decode=True), MsgStreams('100Emails.tar.gz'))
    #m = map(lambda msgFile:DigestStream(msgFile,parser=MicroParser(),decode=True), MsgStreams('100Emails.tar.gz'))
    
    # now reduce those digests into a single stream
    reduction = reduce(lambda curStream, nextStream: curStream + list(nextStream), m, [])

    # now format the reduced digest streams
    for digest in reduction:
	print 'Date: %s\nSender: %s\nSubject: %s\n' % digest

    logging.info('MSG digester test 01 completed')

if __name__ == '__main__':
    testParser()

