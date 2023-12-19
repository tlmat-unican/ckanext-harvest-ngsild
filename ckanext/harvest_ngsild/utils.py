import re

from ngsildclient import Entity, Client, SubscriptionBuilder, MultAttrValue

import logging

log = logging.getLogger(__name__)


def ngsild_to_ckan(ngsild: Entity, mapping: dict) -> dict:
    out_dict = {}

    d = ngsild.to_ngsi_dict()
    # Adapt data format
    if "name" in d:
        d["name"].value = d["name"].value.lower().replace(" ", "-")

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
                    dd[key] = d[v].value
                    break
        else:
            if value in d:
                dd[key] = d[value].value

    for key in keys_list:
        out_dict[key] = [
            {"key": k, "value": v} for k, v in out_dict[key].items()
        ]

    return out_dict


def organization_from_catalog(catalog: Entity) -> dict:
    org_dict = {}

    organization_to_catalog_mapping = {
        "name": "name",
        "title": "title",
        "description": "description",
        # "image_url" :
        # "state" :
        # "approval_status" :
        "extras:url": "homepage",
    }

    org_dict["id"] = catalog.id

    org_dict |= ngsild_to_ckan(catalog, organization_to_catalog_mapping)

    org_dict["state"] = "active"

    return org_dict


def package_from_dataset(dataset: Entity) -> dict:
    pkg_dict = {}
    # Use NgsiDict as it provides same name to access values/objects
    d = dataset.to_ngsi_dict()

    package_to_dataset_mapping = {
        "name": "name",
        "title": "title",
        "author": "creator",
        "maintainer": "provider",
        "license_id": "license",
        "notes": ["description", "datasetDescription"],
        "url": "accessURL",
        "version": "version",
        "metadata_created": "dateCreated",
        "metadata_modified": "dateModified",
        "extras:issued": ["releaseDate", "dateCreated"],
        "extras:modified": ["updateDate", "dateModified"],
        "extras:theme": "theme",
        "extras:language": "language",
        "extras:version_notes": "versionNotes",
        "extras:has_version": "hasVersion",
        "extras:temporal_start": "temporal",
        "extras:temporal_end": "temporal",
        "extras:temporal_resolution": "temporalResolution",
        "extras:documentation": "documentation",
        "extras:contact_name": "contactPoint",
        "extras:access_rights": "accessRights",
        "extras:spatial": "spatial",
    }

    pkg_dict["id"] = dataset.id

    pkg_dict |= ngsild_to_ckan(dataset, package_to_dataset_mapping)

    pkg_dict["private"] = False
    pkg_dict["state"] = "active"
    # TO BE CHANGED
    pkg_dict["owner_org"] = d.get("publisher").value
    if d.get("keyword"):
        pkg_dict["tags"] = [
            {
                "name": x,
                # Currently using free tags (don't belong to a vocabulary)
            }
            for x in d["keyword"].value
        ]

    pkg_dict["resources"] = []

    return pkg_dict


def resource_from_distribution(distribution: Entity) -> dict:
    rsc_dict = {}

    # I think it is not required to put the package_id if the resource is included in the package creation

    resource_to_distribution_mapping = {
        "package_id": "dataset",
        "url": "url",
        "description": "description",
        "format": "format",
        "hash": "hash",
        "name": "name",
        "resource_type": [],
        "mimetype": "mimetype",
        "mimetype_inner": [],
        "cache_url": "accessURL",  # from dataset
        "access_url": "accessURL",
        "download_url": ["downloadURL", "accessURL"],
        "size": "byteSize",
        "created": ["releaseDate", "dateCreated"],
        "last_modified": ["modificationDate", "dateModified"],
        "cache_last_updated": ["modificationDate", "dateModified"],
        # "upload":
    }

    rsc_dict["id"] = to_ckan_valid_id(distribution.id)
    rsc_dict |= ngsild_to_ckan(distribution, resource_to_distribution_mapping)

    return rsc_dict


def to_ckan_valid_id(id: str) -> str:
    # As from resource_id_validator(), the valid characters are [^0-9a-zA-Z _-]
    # So, we need to replace the invalid characters with a valid one
    pattern = r"[^0-9a-zA-Z _-]"
    return re.sub(pattern, "_", id).lower()

def to_ckan_valid_name(name: str) -> str:
    # As from resource_id_validator(), the valid characters are [^0-9a-zA-Z _-]
    # So, we need to replace the invalid characters with a valid one
    return name.lower().replace(" ", "_")
