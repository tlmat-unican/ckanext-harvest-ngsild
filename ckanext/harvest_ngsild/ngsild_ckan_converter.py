import re

from ngsildclient import Client, Entity

from .constants import DEFAULT_NGSILD_CONTEXT, SDM, SDMDCAT, DCTERMS, NGSILD

from typing import Dict, List, Tuple

import logging

log = logging.getLogger(__name__)


class NgsildCkanConverter:

    broker: Client

    ctx = DEFAULT_NGSILD_CONTEXT

    def __init__(self, broker: Client, ctx = DEFAULT_NGSILD_CONTEXT):
        self.broker = broker
        self.ctx = ctx


    def _get_ngsild_entity(self, id: str) -> Entity:
        return self.broker.get(id, ctx=self.ctx)


    # TODO: Modify to use yield for each package
    def make_ckan_organization(self, catalog_id: str) -> Tuple[dict, List[dict]]:
        try:
            catalog = self._get_ngsild_entity(catalog_id)
        except:
            return ({},[])

        # Update organization
        org_dict = self.organization_from_catalog(catalog)
            
        catalog = catalog.to_ngsi_dict()
        if str(SDMDCAT["dataset"]) not in catalog:
            return org_dict, []
        
        datasets = (
            catalog[str(SDMDCAT["dataset"])].value
            if isinstance(catalog[str(SDMDCAT["dataset"])].value, list)
            else [catalog[str(SDMDCAT["dataset"])].value]
        )
    
        packages = []        
        for dataset_id in datasets:
            try:
                p, _ = self.make_ckan_package(dataset_id)
                packages.append(p)
            except Exception as e:
                log.error("Error retrieving package %s from broker", e)
                continue

        return org_dict, packages
    

    def get_catalog_from_dataset(self, dataset_id: str) -> dict:
        # Get all catalogs from broker
        # TODO: Modify ngsiclient library to include attrs in query for filtering
        catalogs = self.broker.query(type=str(SDMDCAT["Catalogue"]))

        # Iterate to find the catalog that contains the dataset
        for catalog in catalogs:
            catalog = catalog.to_ngsi_dict()
            if str(SDMDCAT["dataset"]) not in catalog:
                continue
            
            if dataset_id in catalog[str(SDMDCAT["dataset"])].value:
                return catalog

        return {}


    def make_ckan_package(self, dataset_id: str) -> Tuple[dict, List[dict]]:
        dataset = self._get_ngsild_entity(dataset_id)

        package = self.package_from_dataset(dataset)

        # Using the current injector, the dataset is always created with distributions
        dataset = dataset.to_ngsi_dict()
        if str(SDMDCAT["distribution"]) not in dataset:
            return package, []

        distributions = (
            dataset[str(SDMDCAT["distribution"])].value
            if isinstance(dataset[str(SDMDCAT["distribution"])].value, list)
            else [dataset[str(SDMDCAT["distribution"])].value]
        )

        for distribution_id in distributions:
            try:
                resource = self.make_ckan_resource(distribution_id)
                package["resources"].append(resource)
            except Exception as e:
                # Skip distribution
                log.error("Error retrieving distribution %s from broker", e)

        return package, package["resources"]


    def make_ckan_resource(self, distribution_id: str) -> dict:
        distribution = self._get_ngsild_entity(distribution_id)

        resource = self.resource_from_distribution(distribution)

        return resource


    @staticmethod
    def ckan_format_value(key, value):
        # Convert an array into a long string by separating the items by commas.
        # Exceptions: "notes" and "descriptions" fields, these will separate the items with two line breaks (markdown) 
        #             to make them easy to read in the CKAN web interface.
        # TODO: remove the "if key not in ["notes", "description"]" condition --> is not really a necessary format for creating the ckan object.
        #       ISSUE: cannot be done after this conversion because the array will no longer exist (long string separated with commas) 
        if isinstance(value, list):
            value = (
                ",".join(value)
                if key not in ["notes", "description"]
                else "\n\n".join(value)
            )
        return value


    @staticmethod
    def ngsild_to_ckan(ngsild: Entity, mapping: dict) -> dict:
        out_dict = {}

        d = ngsild.to_ngsi_dict()
        # Moved below because none of the data models have a "name" attribute
        # Adapt data format
        # if "name" in d:
        #     d["name"].value = NgsildCkanConverter.to_ckan_valid_name(d["name"])

        # Do the actual mapping if data
        keys_list = set()  # as a set
        for key, value in mapping.items():
            keys = key.split(":")
            if len(keys) > 1:
                dd = out_dict.setdefault(keys[0], {})
                key = keys[1]
                keys_list.add(keys[0])
            else:
                dd = out_dict
                key = keys[0]

            if isinstance(value, list):
                for v in value:
                    if v in d:
                        # dd[key] = d[v].value   
                        dd[key] = NgsildCkanConverter.ckan_format_value(key, d[v].value)
                        break
            else:
                if value in d:
                    dd[key] = NgsildCkanConverter.ckan_format_value(key, d[value].value)
                    # dd[key] = d[value].value
        
        # Adapt data format
        if "name" in out_dict:
            out_dict["name"] = NgsildCkanConverter.to_ckan_valid_name(out_dict["name"])
        
        for key in keys_list:
            out_dict[key] = [
                {"key": k, "value": v} for k, v in out_dict[key].items()
            ]
        return out_dict


    @staticmethod
    def organization_from_catalog(catalog: Entity) -> dict:
        org_dict = {}

        # ngsi-ld-core-context-v1.7.jsonld is stored in the context broker --> if not, uncomment DCTERMS["title"], DCTERMS["description"],
        organization_to_catalog_mapping = {
            # "name": "name",
            "name": "title", # DCTERMS["title"],
            "title": "title", # DCTERMS["title"],
            "description": "description", # DCTERMS["description"],
            # "image_url" :
            # "state" :
            # "approval_status" :
            "extras:url": str(SDMDCAT["homepage"]), #"homepage",
        }

        org_dict["id"] = catalog.id

        org_dict |= NgsildCkanConverter.ngsild_to_ckan(catalog, organization_to_catalog_mapping)

        org_dict["state"] = "active"

        return org_dict
    

    @staticmethod
    def package_has_resources(package: dict) -> bool:
        if "resources" not in package or len(package["resources"]) == 0:
            return False
        return True


    @staticmethod
    def package_from_dataset(dataset: Entity) -> dict:
        pkg_dict = {}
        # Use NgsiDict as it provides same name to access values/objects
        d = dataset.to_ngsi_dict()

        # ngsi-ld-core-context-v1.7.jsonld is stored in the context broker --> if not, uncomment DCTERMS["title"], DCTERMS["description"]
        package_to_dataset_mapping = {
            # "name": "name",
            "name": "title", # DCTERMS["title"],
            "title": "title", # DCTERMS["title"],
            "author": str(SDMDCAT["creator"]), # "creator",
            "maintainer": str(SDM["dataProvider"]), # "dataProvider",
            "license_id": str(SDMDCAT["license"]), # "license",
            "notes": ["description", "datasetDescription"], # [DCTERMS["description"], "datasetDescription"],
            "url": str(SDMDCAT["landingPage"]), # "landingPage",
            "version": str(SDMDCAT["versionInfo"]), # "version",
            "metadata_created": str(SDM["dateCreated"]), # "dateCreated",
            "metadata_modified": str(SDM["dateModified"]), # "dateModified",
            "extras:issued": ["releaseDate", str(SDM["dateCreated"])], # ["releaseDate", "dateCreated"],
            "extras:modified": ["updateDate", str(SDM["dateModified"])], # ["updateDate", "dateModified"],
            "extras:theme": str(SDMDCAT["theme"]), # "theme",
            "extras:language": str(SDMDCAT["language"]), # "language",
            "extras:version_notes": str(SDMDCAT["versionNotes"]), # "versionNotes",
            "extras:has_version": str(SDMDCAT["hasVersion"]), # "hasVersion",
            "extras:temporal_start": str(SDMDCAT["temporal"]), # "temporal",
            # "extras:temporal_end": "temporal",
            "extras:temporal_resolution": str(SDMDCAT["temporalResolution"]), # "temporalResolution",
            "extras:documentation": "documentation",
            "extras:contact_name": str(SDM["contactPoint"]), # "contactPoint",
            "extras:access_rights": str(SDMDCAT["accessRights"]), # "accessRights",
            "extras:spatial": str(SDMDCAT["spatial"]), # "spatial",
        }
        
        pkg_dict["id"] = dataset.id

        pkg_dict |= NgsildCkanConverter.ngsild_to_ckan(dataset, package_to_dataset_mapping)

        pkg_dict["name"] = pkg_dict["name"].replace(":", "_")
        pkg_dict["private"] = False
        pkg_dict["state"] = "active"
        # TO BE CHANGED
        pkg_dict["owner_org"] = d.get(str(SDMDCAT["publisher"])).value

        if d.get(str(SDMDCAT["keyword"])):
            # d["keyword"] can be a string (1 keyword) or an array (2+ keywords)
            pkg_dict["tags"] = (
                [
                    {
                        "name": x,
                        # Currently using free tags (don't belong to a vocabulary)
                    }
                    for x in d[str(SDMDCAT["keyword"])].value
                ]
                if isinstance(d[str(SDMDCAT["keyword"])].value, list)
                else [{"name": d[str(SDMDCAT["keyword"])].value}]
                )

        pkg_dict["resources"] = []

        return pkg_dict


    @staticmethod
    def resource_from_distribution(distribution: Entity) -> dict:
        rsc_dict = {}

        # I think it is not required to put the package_id if the resource is included in the package creation

        # ngsi-ld-core-context-v1.7.jsonld is stored in the context broker --> if not, uncomment DCTERMS["title"], DCTERMS["description"], NGSILD["format"]
        resource_to_distribution_mapping = {
            "package_id": "dataset",
            "url": str(SDMDCAT["accessUrl"]), # "accessUrl",
            "description": "description", # DCTERMS["description"],
            "format":  "format", # NGSILD["format"], 
            "hash": "hash",
            "license": str(SDMDCAT["license"]), # "license",
            "rights": str(SDMDCAT["rights"]), # "rights",
            "name": "title", # DCTERMS["title"],
            "resource_type": [],
            "mimetype": str(SDMDCAT["mediaType"]), # "mediaType",
            "mimetype_inner": [],
            "cache_url": str(SDMDCAT["accessUrl"]), # "accessUrl",  # from dataset
            "access_url": str(SDMDCAT["accessUrl"]), # "accessUrl",
            "download_url": [str(SDMDCAT["downloadURL"]), str(SDMDCAT["accessUrl"])], # ["downloadUrl", "accessUrl"],
            "size": str(SDMDCAT["byteSize"]), # "byteSize",
            "created": ["releaseDate", str(SDM["dateCreated"])], # ["releaseDate", "dateCreated"],
            "last_modified": ["modificationDate", str(SDM["dateModified"])], # ["modificationDate", "dateModified"],
            "cache_last_updated": ["modificationDate", str(SDM["dateModified"])], # ["modificationDate", "dateModified"],
            # "upload":
        }

        rsc_dict["id"] = NgsildCkanConverter.to_ckan_valid_id(distribution.id)
        rsc_dict |= NgsildCkanConverter.ngsild_to_ckan(distribution, resource_to_distribution_mapping)

        return rsc_dict
    

    @staticmethod
    def to_ckan_valid_id(id: str) -> str:
        # As from resource_id_validator(), the valid characters are [^0-9a-zA-Z _-]
        # So, we need to replace the invalid characters with a valid one
        # As from resource_id_validator(), the resource id length should be between 7 and 100
        # len(value) < 7 or len(value) > 100
        pattern = r"[^0-9a-zA-Z _-]"
        return re.sub(pattern, "_", id).lower()


    @staticmethod
    def to_ckan_valid_name(name: str) -> str:
        # To be a valid ckan name, it cannot contain blank spaces, so we replace them with _
        return name.lower().replace(" ", "_")
