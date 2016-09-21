import re
import os
import json
import subprocess

from .cache import simple_caching, hash_obj

base_cmd = ['java', '-classpath', 'metamap-api.jar',
            'gov.nih.nlm.nls.metamap.MetaMapApiTest']

# java -classpath bin/metamap-api.jar gov.nih.nlm.nls.metamap.MetaMapApiTest

base_config = {
    '-I': None,
    '-O': None,
    '-G': None,
    '--negex': None,
    '-J': (
        'sosy,bpoc,clnd,diap,dsyn,fndg,hlca,inpo,inpr,medd,'
        'mobd,neop,patf,phsu,topp'
    )
}
# T029,T023,T200,T060,T047,T033,T058,T037,T170,
# T074,T048,T191,T046,T121,T184,T061


class MetaMap(object):
    """Wrapper for MetaMap Java APIs"""

    def __init__(self, basepath='', default_config=base_config,
                 options=None, cachedir='cache'):

        if not os.path.exists(cachedir):
            os.makedirs(cachedir)
        self.cachedir = cachedir

        self.base_cmd = [
            'java', '-classpath',
            os.path.join(basepath, 'metamap-api.jar'),
            'gov.nih.nlm.nls.metamap.MetaMapApiTest'
        ]

        if type(default_config) != dict:
            with open(default_config) as f:
                config = json.load(f)
        else:
            config = default_config
        if options:
            config.update(options)

        self.opts = []

        map(lambda t: self.opts.extend(filter(lambda t: len(t) > 0, t)),
            [(k, str(v) if v else '') for k, v in config.items()])

        self.format_string = re.compile(r'(,\s|\[)(([A-Za-z]+\s?)+)')

    def __hash__(self):
        return hash_obj([self.opts, self.base_cmd])

    def tag(self, txt, invalidate=False):
        cc = hash_obj([self.opts, txt])
        return self.__tag(txt, cache_comment=cc, invalidate=invalidate)

    @simple_caching()
    def __tag(self, txt):

        # metamap only works with ascii characters.
        cmd = self.base_cmd + self.opts + [txt.encode('ascii', 'ignore')]

        p = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        out, err = p.communicate()

        decoded_out = out.decode('ascii')

        out = decoded_out.replace('Acronyms and Abbreviations:', '').strip()

        if err:
            raise OSError(err.decode('utf-8')) if err else OSError(out)

        parsed_mm = json.loads(out)
        return parsed_mm


def extract_candidates(mm_response, minscore=0, sources=None):
    '''Extracts candidates from MetaMap response if they meet minimum
    score and appear in the provided sources. If no sources are provided,
    any source is then considered valid. '''
    if sources is not None:
        sources = set(sources)

    for utterance in mm_response['utterances']:
        for phrase in utterance['phrases']:
            for candidate in phrase['candidates']:

                # skip a candidate if it does not meet minimum score
                if candidate['score'] * -1 < minscore:
                    continue

                invalid_concept = False

                concepts = []
                for concept in candidate['concepts']:

                    # removes concepts not mapping the sources
                    # if sources are provided
                    if sources is not None:
                        invalid_source = all(
                            source not in sources
                            for source in concept['sources'])
                        if invalid_source:
                            invalid_concept = True
                            break

                    # turns score to positive
                    concept['score'] = -1 * concept['score']

                    concepts.append(concept)

                if invalid_concept:
                    continue

                yield concepts


def extract_concepts(mm_response, minscore=0, sources=None):
    '''Extracts concepts from MetaMap response if they meet minimum
    score and appear in the provided sources. If no sources are provided,
    any source is then considered valid. '''
    if sources is not None:
        sources = set(sources)

    for candidate in extract_candidates(mm_response):
        for concept in candidate:
            if concept['score'] < minscore:
                continue
            if sources is not None:
                invalid_source = all(
                    source not in sources for source in concept['sources'])
                if invalid_source:
                    continue

            yield concept
