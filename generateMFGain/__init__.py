import logging

import azure.functions as func
from . import mutualFund
import traceback

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    try:
        mutualFund.evaluate()
    except:
        return func.HttpResponse(str(traceback.format_exc()))
    return func.HttpResponse("Hello,  This HTTP triggered function executed successfully.")
