# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Provides the flask main routines along with core app configuration.

Provides flask's application main along with global configuration.
Provides the core user routes for login/logout.

Configuration:

    the following environment variables are read by this module:

        * `FRONTEND_ONLY`: optional, if specified, only deploy the routes
            required for user-facing functionality.
        * `BACKEND_ONLY`: optional, if specified, only deploy backend routes
            used for data ingestion.
        * `DEBUG`: if set, configure debug logging globally

Routes:

    / (index) - if logged in, show user status, if not, redirect to splash
    /splash - show application splash page and login button

    the rest of the routes are provided by other modules.
"""
import os
import logging
from flask import Flask, session, redirect, render_template, request, url_for
from werkzeug.middleware.proxy_fix import ProxyFix

from .devices.fitbit.fitbit_auth import bp as fitbit_auth_bp, firestorage as fitbit_storage, fitbit_session
from .devices.dexcom.dexcom_auth import bp as dexcom_auth_bp, firestorage as dexcom_storage, dexcom_session

from .frontend import bp as frontend_bp
from .devices.fitbit.fitbit_ingest import bp as fitbit_ingest_bp
from .devices.dexcom.dexcom_ingest import bp as dexcom_ingest_bp

#
# configuration
#
app = Flask(__name__)
app.secret_key = "b9718561170654e9cc5d2594ecd36a1264760b890a415546cd0ef9cc716e4c15"  # pylint: disable=line-too-long

# fix for running behind a proxy, such as with cloudrun
app.wsgi_app = ProxyFix(app.wsgi_app)

#
# setup blueprints and routes
#
app.register_blueprint(fitbit_auth_bp, url_prefix="/fitbit")
app.register_blueprint(dexcom_auth_bp, url_prefix="/dexcom")

if not os.environ.get("FRONTEND_ONLY"):
    app.register_blueprint(fitbit_ingest_bp)
    app.register_blueprint(dexcom_ingest_bp)
if not os.environ.get("BACKEND_ONLY"):
    app.register_blueprint(frontend_bp)

#
# configure logging
#
if os.environ.get("DEBUG"):
    logging.basicConfig(level=logging.DEBUG)

#
# main routes
#


@app.route("/")
def index():
    """Show user's fitbit linking status, or spash page"""

    if os.environ.get("BACKEND_ONLY"):
        return "no frontend configured"

    user = session.get("user")

    if not user:
        return render_template("login.html")

    fitbit_storage.user = user["email"]
    if fitbit_session.token:
        del fitbit_session.token

    return render_template(
        "home.html",
        user=user,
        app_name=request.host_url,
        is_fitbit_registered=fitbit_session.authorized,
        is_dexcom_registered=dexcom_session.authorized
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0")
