import logging

import azure.functions as func
from . import obj_sto_prod

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        flist=obj_sto_prod.get_full_bucket_contents('ups-invoices-logging-bucket-prod-it','2021-09-01')
        #return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully. huuuuu..")
        return func.HttpResponse(f"Hello, {flist[0][0]}. This HTTP triggered function executed successfully. huuuuu..")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )
