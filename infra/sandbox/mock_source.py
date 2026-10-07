"""Isolated deployment fixture: one project row and empty other resources."""

import json
from http.server import BaseHTTPRequestHandler, HTTPServer


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        if isinstance(body, list):
            filters = body[0]["query"][0]["filters"]
            project = any("es-bidp-project-p" in f.get("fieldValues", []) for f in filters)
            items = [{"id": "deployment-fixture"}] if project else []
            response = {"page": {"content": items, "totalElements": len(items),
                                  "totalPages": len(items), "number": 0, "size": 50}}
        else:
            response = {"id": "deployment-fixture", "projectDTO": {"version": "01"}}
        payload = json.dumps(response).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *_args):
        pass


if __name__ == "__main__":
    HTTPServer(("0.0.0.0", 8077), Handler).serve_forever()
