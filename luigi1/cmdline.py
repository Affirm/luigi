import os
import optparse
import logging
import sys

import luigi1.server
import luigi1.process
import luigi1.configuration
import luigi1.interface


def luigi_run(argv=sys.argv[1:]):
    luigi1.interface.run(argv, use_dynamic_argparse=True)


def luigid(argv=sys.argv[1:]):
    parser = optparse.OptionParser()
    parser.add_option(u'--background', help=u'Run in background mode', action='store_true')
    parser.add_option(u'--pidfile', help=u'Write pidfile')
    parser.add_option(u'--logdir', help=u'log directory')
    parser.add_option(u'--state-path', help=u'Pickled state file')
    parser.add_option(u'--address', help=u'Listening interface')
    parser.add_option(u'--port', default=8082, help=u'Listening port')

    opts, args = parser.parse_args(argv)

    if opts.state_path:
        config = luigi1.configuration.get_config()
        config.set('scheduler', 'state-path', opts.state_path)

    if opts.background:
        # daemonize sets up logging to spooled log files
        logging.getLogger().setLevel(logging.INFO)
        luigi1.process.daemonize(luigi1.server.run, api_port=opts.port,
                                 address=opts.address, pidfile=opts.pidfile,
                                 logdir=opts.logdir)
    else:
        if opts.logdir:
            logging.basicConfig(level=logging.INFO, format=luigi1.process.get_log_format(),
                                filename=os.path.join(opts.logdir, "luigi-server.log"))
        else:
            logging.basicConfig(level=logging.INFO, format=luigi1.process.get_log_format())
        luigi1.server.run(api_port=opts.port, address=opts.address)
