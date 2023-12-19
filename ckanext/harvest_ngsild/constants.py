from rdflib import Namespace
from rdflib.namespace import DCAT, DCTERMS
SDM = Namespace("https://smartdatamodels.org/")
SDMDCAT = Namespace("https://smartdatamodels.org/dataModel.DCAT-AP/")
NGSILD = Namespace("https://uri.etsi.org/ngsi-ld/")

DEFAULT_NGSILD_CONTEXT = "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.7.jsonld"
SUBSCRIPTION_ID_PATTERN = "urn:ngsi-ld:Subscription:CKAN:"