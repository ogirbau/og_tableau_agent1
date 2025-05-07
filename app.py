from flask import Flask, request, jsonify
import tableauserverclient as TSC
import logging
import ssl
from gevent.pool import Pool
import os
import time

os.environ['HTTP_PROXY'] = ''
os.environ['HTTPS_PROXY'] = ''
os.environ['NO_PROXY'] = '*'

ssl._create_default_https_context = ssl._create_unverified_context

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

app = Flask(__name__)

server_url = 'https://prod-useast-b.online.tableau.com'
token_name = "PowerAutomateAgent"
personal_access_token = "IK9CGGROTHCkI0DY2sslXQ==:FztEDBxDDAQY5gm8VL2J0LUTBRDfkQGq"  # Replace with your actual Token Secret
site = 'axosfinancialproduction'

try:
    tableau_auth = TSC.PersonalAccessTokenAuth(token_name, personal_access_token, site)
    server = TSC.Server(server_url, use_server_version=True)
    server.add_http_options({'timeout': 10})
    logger.info("Tableau Server connection initialized successfully.")
except Exception as e:
    logger.error(f"Failed to initialize Tableau Server connection: {str(e)}")
    raise

def populate_views_async(workbook):
    max_retries = 3
    for attempt in range(max_retries):
        try:
            server.workbooks.populate_views(workbook)
            return workbook
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed for workbook {workbook.id}: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff: 1s, 2s, 4s
            else:
                raise

def search_workbooks(query):
    try:
        with server.auth.sign_in(tableau_auth):
            logger.info("Successfully signed in to Tableau Server.")
            all_workbooks = list(TSC.Pager(server.workbooks))
            logger.info(f"Found {len(all_workbooks)} total workbooks.")
            results = []
            pool = Pool(5)  # Reduce to 5 greenlets
            populated_workbooks = pool.map(populate_views_async, all_workbooks)
            for workbook in populated_workbooks:
                logger.debug(f"Checking workbook: {workbook.name}")
                view_names = [view.name for view in workbook.views]
                logger.debug(f"Views in workbook {workbook.name}: {view_names}")
                if query.lower() in workbook.name.lower():
                    results.append({
                        "name": workbook.name,
                        "id": workbook.id,
                        "project_name": workbook.project_name,
                        "webpage_url": workbook.webpage_url,
                        "views": view_names
                    })
                else:
                    matching_views = [view for view in workbook.views if query.lower() in view.name.lower()]
                    if matching_views:
                        results.append({
                            "name": workbook.name,
                            "id": workbook.id,
                            "project_name": workbook.project_name,
                            "webpage_url": workbook.webpage_url,
                            "views": [view.name for view in matching_views]
                        })
            return results
    except Exception as e:
        logger.error(f"Error in search_workbooks: {str(e)}")
        raise

@app.route('/')
def home():
    return jsonify({"status": "API is running"}), 200

@app.route('/search', methods=['GET'])
def search():
    try:
        query = request.args.get('query', default="", type=str)
        if not query:
            logger.warning("No query parameter provided.")
            return jsonify({"error": "No query parameter provided"}), 400
        logger.info(f"Received search query: {query}")
        results = search_workbooks(query)
        logger.info(f"Returning {len(results)} results for query: {query}")
        return jsonify(results)
    except Exception as e:
        logger.error(f"Error in /search endpoint: {str(e)}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
