'''
wrapper around logging
'''

import logging

def get_logger(name=None):
    """ Return a logging object. """

    logging.TRACE = 5
    logging.addLevelName(logging.TRACE, 'TRACE')

    if name is None or name in ['root', '.']:
        log = logging.getLogger()
    else:
        log = logging.getLogger(name)

    setattr(log, 'trace', lambda *args: log.log(logging.TRACE, *args))
    # dzhang
    ch = logging.StreamHandler()
#    ch.setLevel(logging.INFO)
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    # add formatter to ch
    ch.setFormatter(formatter)
    log.setLevel(logging.INFO)
    log.addHandler(ch)
#    log.setLevel(logging.DEBUG)
#    log.error("test error %s" %name)
#    log.info("test info %s" % name)
#    log.setLevel(logging.TRACE)

    return log
