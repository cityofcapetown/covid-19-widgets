import collections
import itertools
import json
import os
import tempfile

import jinja2
import requests

GRIDJS_LINK = ("gridjs.js", "https://unpkg.com/gridjs/dist/gridjs.production.min.js")
GRIDJS_CSS_LINK = ("mermain.css", "https://unpkg.com/gridjs/dist/theme/mermaid.min.css")


class TableWidget:
    _template = jinja2.Template("""
    <!DOCTYPE html>
    <html lang="en">

    <head>
        <meta charset="utf-8">
        <title>{{ this.title }}</title>
        <link href="mermain.css" rel="stylesheet"/>
    </head>

    <body style="margin:0px;overflow-x: hidden;">
        <div id="wrapper"></div>
        <script src="gridjs.js"></script>
        <script>
            {% for func in this.js_funcs %}
            {{ func }}
            {% endfor %}

            var jsonFetchPromise = fetch("{{ this.data_link }}", {{ this.fetch_config|tojson }} )
            var jsonParsePromise = jsonFetchPromise.then(res => res.json())
            jsonParsePromise.then((out) => {
                    // Parsing any custom sorts that have been specified
                    out['columns'].forEach(function (col, index) {
                        if (col.hasOwnProperty('sort')) {
                            if (col['sort'].hasOwnProperty('compare')) {
                                col["sort"]["compare"] = window[col["sort"]["compare"]];
                            }
                        }
                        if (col.hasOwnProperty('formatter')) {
                            col["formatter"] = window[col["formatter"]];
                        }
                    });
                    new gridjs.Grid(out).render(document.getElementById("wrapper"));
            });
        </script>
    </body>
    </html>
    """)

    def __init__(self, data, name, title=None, fetch_config=None, js_funcs=None):
        self.data = data
        self.name = name
        self.fetch_config = fetch_config if fetch_config else {'credentials': 'same-origin'}

        self.data_link = f"{self.name}.json"
        self.title = title if title else self.name
        self.js_funcs = js_funcs if js_funcs else []

    def generate_data_json(self):
        assert 'data' in self.data, "Data key missing in data dict"
        assert isinstance(self.data['data'], collections.Container), "data key is not some sort of container"

        return self.data_link, json.dumps(self.data)

    def generate_html(self):
        return f"{self.name}.html", self._template.render(this=self)

    def get_dep_files(self, proxy_username=None, proxy_password=None, proxy_domain=None):
        http_session = requests.Session()

        if proxy_username and proxy_password and proxy_domain:
            proxy_string = f'http://{proxy_username}:{proxy_password}@{proxy_domain}/'
            http_session.proxies = {
                "http": proxy_string,
                "https": proxy_string
            }

        for filename, file_link in (GRIDJS_LINK, GRIDJS_CSS_LINK):
            resp = http_session.get(file_link)
            yield filename, resp.text

    def output_file_generator(self, proxy_username=None, proxy_password=None, proxy_domain=None):
        output_file_chain = itertools.chain(
            [self.generate_data_json()],
            [self.generate_html()],
            self.get_dep_files(proxy_username, proxy_password, proxy_domain)
        )

        with tempfile.TemporaryDirectory() as tempdir:
            for filename, file_contents in output_file_chain:
                local_path = os.path.join(tempdir, filename)

                with open(local_path, "w") as file:
                    file.write(file_contents)

                yield filename, local_path
