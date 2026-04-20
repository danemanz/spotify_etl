import os
import requests
import base64
import urllib.parse
import webbrowser
from http.server import HTTPServer, BaseHTTPRequestHandler
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID     = os.getenv("SPOTIFY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
REDIRECT_URI  = os.getenv("SPOTIFY_REDIRECT_URI")
SCOPE = "user-read-recently-played user-top-read user-library-read playlist-read-private playlist-read-collaborative"
# Step 1 — open the Spotify login page in your browser
auth_url = "https://accounts.spotify.com/authorize?" + urllib.parse.urlencode({
    "client_id":     CLIENT_ID,
    "response_type": "code",
    "redirect_uri":  REDIRECT_URI,
    "scope":         SCOPE,
    "show_dialog":   "true",  # add this line
})
print("Opening Spotify login in your browser...")
webbrowser.open(auth_url)

# Step 2 — spin up a tiny local server to catch the redirect
auth_code = None

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        global auth_code
        params = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
        auth_code = params.get("code", [None])[0]
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"<h2>Auth complete! You can close this tab.</h2>")

    def log_message(self, format, *args):
        pass  # suppress server logs

print("Waiting for Spotify to redirect back...")
server = HTTPServer(("localhost", 8888), Handler)
server.handle_request()  # handles exactly one request then stops

# Step 3 — exchange the code for tokens
creds = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
response = requests.post("https://accounts.spotify.com/api/token",
    headers={"Authorization": f"Basic {creds}"},
    data={
        "grant_type":   "authorization_code",
        "code":         auth_code,
        "redirect_uri": REDIRECT_URI,
    }
)
tokens = response.json()

refresh_token = tokens.get("refresh_token")
if refresh_token:
    # Step 4 — save the refresh token into your .env file
    with open(".env", "a") as f:
        f.write(f"\nSPOTIFY_REFRESH_TOKEN={refresh_token}\n")
    print("\nSuccess! Your refresh token has been saved to .env")
    print("You never need to run this script again.")
else:
    print("\nSomething went wrong:")
    print(tokens)