import os
import itertools
from collections import defaultdict, deque, OrderedDict
import cPickle as pickle
from bs4 import BeautifulSoup
import hashlib

stopwords_lists_filenames = ('FoxStoplist.txt', 'SmartStoplist.txt')
vocabulary_filenames = ('2of4brif.txt',)
vocabulary_directory = '.'

class Corpus(object):

    """
    A class that does little else except get the names of the files in the BNC
    corpus.

    """

    @classmethod
    def get_corpus_filenames(cls, corpus_xmlfiles_rootdir):

        corpus = cls(corpus_xmlfiles_rootdir)
        return corpus.corpus_filenames


    def __init__(self, corpus_xmlfiles_rootdir):

        self.corpus_xmlfiles_rootdir = corpus_xmlfiles_rootdir


    @property
    def corpus_filenames(self):

        """
        Get the list of all BNC xml corpus files.

        """

        corpus_xmlfiles = []

        for root, dirs, filenames in os.walk(self.corpus_xmlfiles_rootdir):
            for filename in filenames:
                basename, extension = os.path.splitext(filename)
                if extension == '.xml':
                    corpus_xmlfiles.append(os.path.join(root, filename))


        return corpus_xmlfiles

    def _get_written_or_spoken_corpus_filenames(self, signature):


        return [filename for filename in self.corpus_filenames
                if signature in open(filename).read()]


    def get_written_corpus_filenames(self):

        """
        Return list of xml files that correspond to the written portion of the
        BNC.

        """

        return self._get_written_or_spoken_corpus_filenames('<wtext')


    def get_spoken_corpus_filenames(self):

        """
        Return list of xml files that correspond to the spoken portion of the
        BNC.

        """

        return self._get_written_or_spoken_corpus_filenames('<stext')


def get_words(xmlelement):

    """
    Get all words, lower-cased, from the word tags in the BNC xmlelement.

    """

    return [word_tag.text.strip().lower()
            for word_tag in xmlelement.find_all('w')]


def get_corpus_file_soup(corpus_filename):

    """
    For a given corpus xml filename, return its BeautifulSoup soup.

    """

    return BeautifulSoup(open(corpus_filename), 'xml')


def dump(data, filename, protocol=2):

    """
    For pickle writing large lists to avoid memory errors.
    From http://stackoverflow.com/a/20725705/1009979

    """

    with open(filename, "wb") as f:
        pickle.dump(len(data), f, protocol=protocol)
        for value in data:
            pickle.dump(value, f, protocol=protocol)


def load(filename):

    """
    For pickle loading large pickled lists.
    From http://stackoverflow.com/a/20725705/1009979

    """

    data = []
    with open(filename, "rb") as f:
        N = pickle.load(f)
        for _ in xrange(N):
            data.append(pickle.load(f))

    return data


def get_all_paragraphs(xmlfilename):

    """
    Return all paragraphs, indicating xml filename and div1 count and paragraph
    count in the div1.

    """

    soup = get_corpus_file_soup(xmlfilename)

    results = []
    for i, div in enumerate(soup.find_all('div', {'level': '1'})):

        all_paragraphs_in_div1 = div.find_all('p')

        for j, paragraph in enumerate(all_paragraphs_in_div1):

            words = get_words(paragraph)

            paragraph_details = dict(corpus_filename = xmlfilename,
                                     div1_index = i,
                                     paragraph_index = j,
                                     paragraph_count = len(all_paragraphs_in_div1),
                                     words = words,
                                     word_count = len(words))

            results.append(paragraph_details)

    return results


def init_ipyparallel():

    '''
    Initialize the ipyparallel client. This assumes the the cluster has been
    started with "ipcluster start -n N", where N is the number of engine you
    want.
    '''

    try:
        from ipyparallel import Client
    except ImportError as e:
        print(e)
        print('Could get ipyparallel client. Have done "ipcluster start -n N"')
        raise

    clients = Client()
    clients.block = True

    return clients.load_balanced_view()


def get_all_paragraphs_parallel(view, xmlfilenames):

    """
    Use IPyparallel to get all paragraphs, using get_all_paragraphs.
    Use it like this, for example,

    ipcluster start -n 4

        from ipyparallel import Client

        clients = Client()
        clients.block = True
        view = clients.load_balanced_view()

        paragraphs = get_all_paragraphs_parallel(view, corpus_filenames)

        paragraphs = sorted(paragraphs,
                            key=lambda args: args['corpus_filename'])

        dump(paragraphs, filename='paragraphs.pkl')

    """

    _all_paragraphs = view.map(get_all_paragraphs,
                               xmlfilenames)

    return list(itertools.chain(*_all_paragraphs))


def get_div1_documents(paragraphs):

    '''
    Bundle up all paragraphs from the same BNC div1 document as a list of word lists.
    Return a list of list of word lists (each div1 document being a list of words lists).
    '''

    div1_documents = defaultdict(list)
    for paragraph in paragraphs:
        key = (paragraph['corpus_filename'], paragraph['div1_index'])
        div1_documents[key].append(paragraph['words'])

    return div1_documents.values()


class MiniDocumentCorpus(object):

    '''
    I'm doing x = y[:] (where y is a list) all over the place because I was bit so
    bad by doing x = y before, when I wanted x to be a copy of y.
    '''

    @classmethod
    def make(cls,
             list_of_paragraphs,
             minimum_paragraph_length=250,
             maximum_paragraph_length=500,
             sep='|'):

        mini_doc_factory = cls(list_of_paragraphs,
                               minimum_paragraph_length,
                               maximum_paragraph_length)


        mini_documents = mini_doc_factory.make_mini_documents()

        mini_doc_to_string\
            = lambda mini_doc: sep.join([w.replace(sep,'_') for w in mini_doc]).encode('utf-8')

        return map(mini_doc_to_string, mini_documents)

    def __init__(self,
                 list_of_paragraphs,
                 minimum_paragraph_length,
                 maximum_paragraph_length):

        self.list_of_paragraphs = deque(list_of_paragraphs[:])
        self.minimum_paragraph_length = minimum_paragraph_length
        self.maximum_paragraph_length = maximum_paragraph_length

    def _pop_until(self, break_condition):

        while True:

            paragraph = self.list_of_paragraphs.popleft()

            if break_condition(paragraph):
                break

        return paragraph[:]

    def make_mini_doc(self):

        mini_doc = []

        try:

            mini_doc = self._pop_until(lambda paragraph: 0 < len(paragraph) < self.maximum_paragraph_length)

            assert 0 < len(mini_doc) < self.maximum_paragraph_length, (len(mini_doc),
                                                                       len(self.list_of_paragraphs))

            while True:

                paragraph = self.list_of_paragraphs.popleft()

                if len(mini_doc) + len(paragraph) < self.maximum_paragraph_length:

                    mini_doc.extend(paragraph[:])

                else:

                    break

            # Push the last popped paragraph back
            self.list_of_paragraphs.appendleft(paragraph)

        except IndexError:
            # We've come to the end of the line
            pass

        return mini_doc

    def make_mini_documents(self):

        mini_documents = []

        while len(self.list_of_paragraphs):
            mini_documents.append(self.make_mini_doc())

        # Should be empty
        assert len(self.list_of_paragraphs) == 0

        # Should never have oversized paragraphs
        assert all(map(lambda mini_doc: len(mini_doc) < self.maximum_paragraph_length,
                       mini_documents))

        # Take out the small ones now
        mini_documents = filter(lambda mini_doc: len(mini_doc) > self.minimum_paragraph_length,
               mini_documents)

        # Double check
        assert all(map(lambda mini_doc: self.minimum_paragraph_length < len(mini_doc) < self.maximum_paragraph_length,
                       mini_documents))

        return mini_documents

# TODO (Fri 16 Jun 2017 21:06:32 BST): This had a bad bug that lead to texts
# being repeated.
#def make_mini_documents(div1_document, mini_document_length=(250, 500), sep='|'):
#
#    '''
#
#    Given a `div1_document` which is a list of word lists, where each word list
#    is the set of words in a paragraph in a div1 document from the BNC, return a set
#    of smaller or "mini" documents.
#
#    Each mini document is either a single paragraph or a concatenation of consecutive
#    paragraphs such that the total word count in each mini document is in the range
#    `mini_document_length`.
#
#    The mini documents are returned strings, where the words in the each mini document are
#    delimited by `sep`.
#
#    Return value: list of strings
#    '''
#
#    min_length, max_length = mini_document_length
#
#    document_words = []
#
#    if div1_document:
#        words = []
#        for paragraph in div1_document:
#
#            if len(words) + len(paragraph) < max_length:
#
#                words.extend(paragraph)
#
#            else:
#
#                document_words.append(words)
#                words = paragraph[:]
#
#        document_words.append(words)
#
#        return [sep.join([w.replace(sep,'_') for w in doc_words]).encode('utf-8')
#                for doc_words in document_words if min_length <= len(doc_words) <= max_length]



def paragraphs_to_mini_documents(paragraphs, mini_document_length=(250, 500), sep='|'):

    '''
    Given all the paragraphs in the BNC, create mini documents by joining the
    paragraphs into their div1 documents and then chopping them up using
    make_mini_documents.

    Return a unique list of mini documents.
    '''

    min_length, max_length = mini_document_length
    mini_documents = []
    _mini_documents = map(lambda div1: MiniDocumentCorpus.make(div1,
                                                               min_length,
                                                               max_length,
                                                               sep),
                          get_div1_documents(paragraphs))

    mini_documents = list(itertools.chain(*_mini_documents))

    # Make sure the mini_documents are unique strings.
    unique_mini_documents = OrderedDict()
    for mini_document in mini_documents:
        unique_mini_documents[checksum(mini_document)] = mini_document

    return unique_mini_documents.values()


def checksum(argument, algorithm='sha256'):

    '''
    Return the checksum hash for a given string input.
    '''

    h = hashlib.new(algorithm)

    h.update(argument)

    return h.hexdigest()


def _read_wordlist(filename):

    """
    Read in file contents, return all newline delimited strings
    unless the line starts with "#".

    """

    filepath = os.path.join(vocabulary_directory, filename)

    file_contents = open(filepath).read().strip().split('\n')
    return [word for word in file_contents if word[0] != '#']


def _get_wordlists_from_filenames(words_list_filenames):

    """
    Read in all words lists. Create their set union.
    Return as new list.

    """

    words_sets = map(lambda arg: set(_read_wordlist(arg)),
                     words_list_filenames)

    return list(set.union(*words_sets))


def get_stopwords_list():

    """
    Read in all stop words lists. Create their set union.
    Return as new list.

    """

    return _get_wordlists_from_filenames(stopwords_lists_filenames)


def get_brief_vocabulary():

    """
    Read in all stop words lists. Create their set union.
    Return as new list.

    """

    return _get_wordlists_from_filenames(vocabulary_filenames)


def get_corpus_vocabulary(mini_documents, minimum_count=5, sep='|'):

    """

    The vocabulary is defined as the intersection of the set of lower cased
    words in the mini document BNC and the words in the vocab file minus the
    stopwords.

    Return the vocabularly with its frequencies.

    """


    stopwords = dict.fromkeys(get_stopwords_list())
    acceptable_word_list = get_brief_vocabulary()

    word_counter = dict.fromkeys(acceptable_word_list, 0)

    for mini_document in mini_documents:
        for word in mini_document.split(sep):
            try:
                word_counter[word] += 1
            except KeyError:
                pass


    # Clear out the stop words and low frequency words
    for word in word_counter.keys():
        if word in stopwords or word_counter[word] < minimum_count:
            del word_counter[word]

    return word_counter
