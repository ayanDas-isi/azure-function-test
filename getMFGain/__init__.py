import logging
import mimetypes
import azure.functions as func
from . import makeGraph
import traceback

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    try:
        fpath=makeGraph.generate_html()
        with open(fpath, 'rb') as f:
                mimetype = mimetypes.guess_type(fpath)
                return func.HttpResponse(f.read(), mimetype=mimetype[0])
    except:
        
        return func.HttpResponse(
                 str(traceback.format_exc()),
                 status_code=400
            )
