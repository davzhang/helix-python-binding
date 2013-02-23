'''
 Main entry point for HelixMonitor Webapp
 '''
from optparse import OptionParser
from helix_monitor import app, setupHelixSpectator
import sys

def constructCommandLineOptions(parser):
    parser = OptionParser(usage="usage: %prog [options]")
    parser.add_option('--host', default='localhost', help='Host address to bind to. [default: %(default)s]')
    parser.add_option ('--port', default=8000, type=int, help='Port to listen on. [default: %(default)s]')
    parser.add_option("--zkSvr", action="store", dest="zkServer", default="eat1-app207.stg:12913", help="zookeeper address [default: %default]")
    parser.add_option("--cluster", action="store", dest="cluster", default="ESPRESSO_DEV_FT_4", help = "Cluster name [default: %default]")
#    parser.add_option("--zkSvr", action="store", dest="zkServer", default="localhost:2181", help="zookeeper address [default: %default]")
#    parser.add_option("--cluster", action="store", dest="cluster", default="storage-integration-cluster", help = "Cluster name [default: %default]")

    return parser



def main(args):
    parser = constructCommandLineOptions(args)
    (options, args) = parser.parse_args()
    setupHelixSpectator(options.zkServer, options.cluster)
    app.run(host=options.host, port=options.port, debug=True)


if __name__ == "__main__":
    main(sys.argv[1:])


