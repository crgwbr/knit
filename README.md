# Knit

Knit is a simple HTTP Proxy (think varnish) with a self replicating mesh network of caches. This enables multiple, geolocated cache-proxies to share cached resources. It can use anything from process memory to memcache as the actual cache backend; actual cache data replication is handled entirely within knit.

## Usage

Show command line options:

    python -m knit --help

Start proxy with development HTTP server:

    python -m knit --devel

Run with Gunicorn (use environment variables instead of command line options):

    export KNIT_DEVEL=False
    export KNIT_SETTINGS='/path/to/custom/settings.yml'
    export KNIT_DISCOVER='10.10.0.42:42000'
    gunicorn knit.wsgi:application

