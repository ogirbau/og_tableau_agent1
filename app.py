from flask import Flask, request, jsonify
import tableauserverclient as TSC
import logging
import ssl

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
    server.add_http_options({'timeout': 10})  # Set a 10-second timeout for requests
    logger.info("Tableau Server connection initialized successfully.")
except Exception as e:
    logger.error(f"Failed to initialize Tableau Server connection: {str(e)}")
    raise

def search_workbooks(query):
    try:
        with server.auth.sign_in(tableau_auth):
            logger.info("Successfully signed in to Tableau Server.")
            all_workbooks = list(TSC.Pager(server.workbooks))
            logger.info(f"Found {len(all_workbooks)} total workbooks.")
            results = []
            for workbook in all_workbooks:
                # Check if the query matches the workbook name first
                if query.lower() in workbook.name.lower():
                    server.workbooks.populate_views(workbook)
                    results.append({
                        "name": workbook.name,
                        "id": workbook.id,
                        "project_name": workbook.project_name,
                        "webpage_url": workbook.webpage_url,
                        "views": [view.name for view in workbook.views]
                    })
                else:
                    # If the query doesn't match the workbook name, check the views
                    server.workbooks.populate_views(workbook)
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
