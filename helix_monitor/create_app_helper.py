from flask import Flask, jsonify, render_template_string, request, url_for

# http://flask.pocoo.org/snippets/35/
class ReverseProxied(object):
    """
      Wrap the application in this middleware and configure the front-end server to
      add these headers, to let you quietly bind this to a URL other than / and to an
      HTTP scheme that is different than what is used locally.

      In nginx::

        location /myprefix {
          proxy_pass http://192.168.0.1:5001;
          proxy_set_header Host $host;
          proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
          proxy_set_header X-Scheme $scheme;
          proxy_set_header X-Script-Name /myprefix;
        }

      :param app: the WSGI application
    """

    def __init__(self, app):
        self.app = app

    def __call__(self, environ, start_response):
        script_name = environ.get('HTTP_X_SCRIPT_NAME', '')

        if script_name:
            environ['SCRIPT_NAME'] = script_name
            path_info = environ['PATH_INFO']

            if path_info.startswith(script_name):
                environ['PATH_INFO'] = path_info[len(script_name):]

        scheme = environ.get('HTTP_X_SCHEME', '')

        if scheme:
            environ['wsgi.url_scheme'] = scheme

        return self.app(environ, start_response)

def url_for_other_page(page):
    """
      Generate a URL for another page with arguments. Useful for pagination.

      See: http://flask.pocoo.org/snippets/44/
    """

    args = request.view_args.copy()
    args['page'] = page
    return url_for(request.endpoint, **args)

def create_app(name):
    """
      Create a new Flask application.

      :param name: app name.

      See also:
        * http://flask.pocoo.org/docs/api/
        * http://flask.pocoo.org/docs/patterns/packages/
        * http://flask.pocoo.org/snippets/20/
    """

    app = Flask(name)

    # Wrap with the proxy helper.
    app.wsgi_app = ReverseProxied(app.wsgi_app)

    app.jinja_env.add_extension('jinja2.ext.do')
    app.jinja_env.add_extension('jinja2.ext.loopcontrols')
    app.jinja_env.globals['url_for_other_page'] = url_for_other_page

    return app
