import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit

from ckan.types import Context

from flask import Blueprint, request, abort, jsonify, Response, make_response

# Import current_user dict (object) which contains the information about the user performing the action (gets user information from APIToken sent with the request)
from ckan.common import current_user

# from ckan.common import json, _, g, request, current_user

import ckan.logic as logic

# from ckan.logic import auth_disallow_anonymous_access
import ckan.authz as authz

from ngsildclient import Client, Entity, SubscriptionBuilder

from .ngsild_ckan_converter import NgsildCkanConverter

from .utils import (
    organization_from_catalog,
    package_from_dataset,
    resource_from_distribution,
    to_ckan_valid_name,
    to_ckan_valid_id
)

from .constants import DEFAULT_NGSILD_CONTEXT, SUBSCRIPTION_ID_PATTERN, SDMDCAT

import logging

log = logging.getLogger(__name__)

BLUEPRINT_NAME = "harvest_ngsild"
BLUEPRINT_NGSILD_NOTIFICATION_ACTION_NAME = "ngsi-ld-notifications"
BLUEPRINT_NGSILD_SUBSCRIBE_ACTION_NAME = "ngsi-ld-subscribe"
BLUEPRINT_NGSILD_UNSUBSCRIBE_ACTION_NAME = "ngsi-ld-unsubscribe"

NOTIFICATIONS_ENDPOINT_CONFIG_OPTION= 'ckanext.harvest_ngsild.notifications_endpoint'

def ngsild_notifications_action():
    """Handle request to NSGI-LD notifications server route"""

    # Check current user is authorized to perform this action
    log.debug("Current user: %s", current_user)

    context = {
        "model": logic.model,
        # "ignore_auth": True,
        # "limits": {'packages': 2},
        # "for_view": True,
        # "__auth_audit" = [],
        "session": logic.model.Session,
        "user": current_user.name,  # Want to set who is doing this
        "auth_user_obj": current_user,
    }

    # Check notification format is the expected
    # - Content-type
    # - Client IP address (only the servers subscribed to)
    # - Body format compliant with NGSI-LD CKAN

    # if request.content_type not in [
    #     "application/json",
    #     "application/json",
    # ]:
    #     abort(400, "Unexpected Content-Type, expecting 'application/json'")

    organization: str = request.headers.get("X-CKAN-Organization")
    if not organization:
        abort(400, "Missing X-CKAN-Organization header")

    # TODO: request complete url (https://hostname:port) so we can extract the hostname, port and secure (parameters for Client object, line 166)
    hostname: str = request.headers.get("X-NGSILD-Broker-Host")
    if not hostname:
        abort(400, "Missing X-NGSILD-Broker-Host")

    port: str = request.headers.get("X-NGSILD-Broker-Port")
    if not port: 
        port = 9091 # Default port
    
    # TODO: non-expiring auth token so it can go appended to the subscription/notifications
    # auth_token: str = request.headers.get("X-NGSILD-Broker-Auth-Token")
    # if not auth_token:
    #     auth_token = None

    body = request.get_json(force=True)
    entities = body.get("data", [])

    # Although we can get the source IP address from request.remote_addr, the
    # domain name could not be the same as the one used to subscribe
    broker = Client(hostname = hostname, port = port, secure = True) #, custom_auth = auth_token)
    converter = NgsildCkanConverter(broker)
    
    # Workaround for patch uninitialized organization (the organization/catalogue entity was not described before, in the ngsi-ld/subscribe request moment)
    org_id = "urn:ngsi-ld:Catalogue:" + organization
    organization_obj, _ = converter.make_ckan_organization(org_id)
    
    if not organization_obj:
        resp = jsonify("")
        resp.status_code = 404
        return resp
    
    logic.action.patch.organization_patch(context, organization_obj)

    organization = to_ckan_valid_name(organization)
    for e in entities:
        entity = Entity(e)
        log.debug("Entity: %s", entity)
        if (
            entity.type != "Dataset"
            and entity.type
            != str(SDMDCAT["Dataset"])
        ):
            log.debug("Ignoring entity of type: %s", entity.type)
            continue
        # TODO: Change functions to support Entity
        package, _ = converter.make_ckan_package(entity.id)
        if converter.package_has_resources(package):
            package["owner_org"] = organization
            
            package.pop("id", None) # 'The input field id was not expected' --> this happends when dataset["resources"] is empty
            # In case of error or not valid permissions, abort with exception
            try:
                package_response = logic.action.create.package_create(context, package)
                log.debug("Package created: %s", package_response)
            except logic.ValidationError: # Resources uris already exists --> we have to recreate the package in order to include the lastest changes or additions
                # Patch entire dataset -- https://docs.ckan.org/en/2.10/api/index.html?highlight=ogic+action+patch+package_patch#ckan.logic.action.patch.package_patch
                                     # -- raises error trying to access the resources
                # package["id"] = package_id
                # package_response = logic.action.patch.package_patch(context, package)
                # log.debug("Package updated: %s", package_response)

                # Delete dataset
                data_dict = {"id": package["name"]}
                context['user'] = "ckan_admin" # Only sysadmin can purge organizations/datasets/distributions
                logic.action.delete.dataset_purge(context, data_dict)
                
                # Recreate dataset
                context["user"] = current_user.name
                package_response = logic.action.create.package_create(context, package)
                log.debug("Package updated: %s", package_response)
            
    resp = jsonify([e["id"] for e in entities])
    resp.status_code = 201

    return resp


# ckan.plugins.toolkit.auth_disallow_anonymous_access

def purge_organization(organization_id: str) -> dict:
    ctx = {"model": logic.model, "user": "ckan_admin"} # purge actions can only be done by ckan_admin

    try:
        ckan_org = logic.action.get.organization_show(
            ctx,
            {
                "id": organization_id,
                "include_datasets": True,
            },
        )

        for pkg in ckan_org["packages"]:
            log.debug("Deleting CKAN package: %s", pkg["id"])
            logic.action.delete.dataset_purge(ctx, {"id": pkg["id"]})

        a = logic.action.delete.organization_purge(
            {"model": logic.model, "user": "ckan_admin"},
            {"id": organization_id},
        )
    except logic.NotFound:
        pass


def initialize_organization(ctx: Context, organization_id: str, broker: Client):
    converter = NgsildCkanConverter(broker)

    # TODO: Check to use yield/generators
    organization, packages = converter.make_ckan_organization(organization_id)

    if organization:
        organization = logic.action.patch.organization_patch(ctx, organization)

    for package in packages:
        # Add to CKAN only if package has resources
        if converter.package_has_resources(package):
            package["owner_org"] = organization_id
            id = package.pop("id") # only sysadmin can set package_id
            # On CKAN boot up, the database can be already populated
            # and packages and organizations might already exist
            # try:
            #     package = logic.action.patch.package_patch(ctx, package)
            # except logic.NotFound:
            #     package = logic.action.create.package_create(ctx, package)
            package = logic.action.create.package_create(ctx, package)
        # update_dict = {"id": package["id"], "organization_id": organization_id}
        # logic.action.update.package_owner_org_update(ctx, update_dict)


def check_resubscription(ctx: Context, organization_id: str, broker: Client, package_titles: list):
    #TODO: this method does not check if an already existing package has undergone some changes (for example: new author, keywords, etc)
    #      so these changes will be lost/missing until a notification arrives from this dataset
    converter = NgsildCkanConverter(broker)
    catalog = converter._get_ngsild_entity(organization_id)
    catalog = catalog.to_ngsi_dict()
    

    datasets = (
        catalog[str(SDMDCAT["dataset"])].value
        if isinstance(catalog[str(SDMDCAT["dataset"])].value, list)
        else [catalog[str(SDMDCAT["dataset"])].value]
    )

    # check if new datasets have been injected into the Context Broker while unsubscribed.
    for d in datasets:
        if ":".join(d.split(":")[-2:]) not in package_titles:
            p, _ = converter.make_ckan_package(d)
            p["owner_org"] = organization_id
            p.pop("id")
            package = logic.action.create.package_create(ctx, p)
    


@logic.auth_disallow_anonymous_access
def ngsild_subscribe_action():
    """Handle request to NSGI-LD subscription server route"""

    # Check current user is authorized to perform this action
    log.debug("Current user: %s", current_user)

    context = {
        "model": logic.model,
        # "ignore_auth": True,
        # "limits": {'packages': 2},
        # "for_view": True,
        # "__auth_audit" = [],
        "session": logic.model.Session,
        "user": current_user.name,  # Want to set who is doing this
        "auth_user_obj": current_user,
    }

    # Retrieve remote server information from request
    # - IP address
    # - Port
    # - Scorpio friendly name: used to create the subscription id and name
    # - Organization where the subscription is stored
    # - CKAN Authorization token
    if request.content_type == "application/json":
        body = request.get_json(force=True)
    elif request.content_type == "application/x-www-form-urlencoded":
        body = request.form
    else:
        abort(
            400,
            "Unexpected Content-Type, expecting 'application/json' or 'application/x-www-form-urlencoded'",
        )

    # log.debug("Body: %s", body)

    # TODO: request complete url (https://hostname:port) so we can extract the hostname, port and secure (parameters for Client object, line 348)
    hostname: str = body.get("hostname", None)
    port: int = body.get("port", None)
    friendly_name: str = body.get("friendlyName", None)
    organization: str = body.get("organization", None)
    token: str = body.get("ckan_token", None)
    # TODO: non-expiring auth token so it can go appended to the subscription/notifications
    # auth_token: str = body.get("auth_token", None)
    # if not auth_token:
    #     auth_token = None

    if not hostname or not port or not friendly_name or not token:
        abort(
            400,
            "Missing parameters. Expected: hostname, port, friendly_name, ckan_token",
        )

    # Create Context Broker client
    broker = Client(hostname = hostname, port = port, secure = True) #, custom_auth = auth_token)

    # Create organization if it does not exist and assign it to the current user
    # If the organization exists, the current user will be added to it as editor
    org_name = to_ckan_valid_name(organization)
    org_id = "urn:ngsi-ld:Catalogue:" + organization
        
    try:
        ## If we purged the organization before extracting the users, the previous users would be lost.
        # purge_organization(org_id) 
        # TODO: Check if the organization is deleted but not purged. Then it will be just activating it
        
        ckan_org = logic.action.get.organization_show(
            context,
            {
                "id": org_name.replace("-", "_"),
                "include_datasets": True,
                "include_users": True,
            },
        )
        log.debug("CKAN organization: %s", ckan_org) # ckan_user can see usernames and capaticy/roles
                                                     # capacity in ["admin", "editor", "member"]

        # user_orgs = logic.action.get.organization_list_for_user(
        #     context, {"id": current_user.name}
        # )
        # log.debug("CKAN user organizations: %s", user_orgs)

        # Check can be done by capturing ValidationError exception
        # But using the list fo users in organization is cleaner
        if not any(u.get("name") == current_user.name for u in ckan_org["users"]):
            # Update organization to add current user as editor
            data_dict = {
                "id": org_id,
                "username": current_user.name,
                "role": "editor",
            }

            # Get admin user of this organization to perform the member creation
            for u in ckan_org['users']:
                context['user'] = (
                    u.get("name")
                    if u.get("capacity") == "admin"
                    else "ckan_admin"
                )
            
            member = logic.action.create.organization_member_create(
                context, data_dict
            )
        
        # Resubscription --> organization already exists and a package has been injected into the Broker while unsubscribed
        # package_titles = [p.get("title") for p in ckan_org['packages']] # organization_show returns only the first 10 datasets
        package_list = logic.action.get.current_package_list_with_resources(context, {"limit": 100})
        package_titles = [p['title'] for p in package_list if p['organization']['name'] == org_name]
        
        check_resubscription(context, org_id, broker, package_titles)
    
    except logic.NotFound as e:
        data_dict = {
            "name": org_name,
            "id": org_id,
            "title": org_name,
            "state": "active",
            "users": [{"name": current_user.name, "capacity": "admin"}],
        }
        
        org = logic.action.create.organization_create(context, data_dict)
        log.debug("CKAN organization %s created", org)
        
        initialize_organization(context, data_dict["id"], broker)


    # TODO: 2+ organizations for the same Context Broker 
        # subscription_id_pattern="urn:ngsi-ld:Subscription:CKAN:<catalogue>:<username>" 
        # idPattern for select_entities urn:ngsi-ld:Dataset:<catalogue>:.*
    # Check if already exists a subscription to a certain context broker
    # Two subscriptions to the same context broker but created from different users will notify the same entities
    if broker.subscriptions.list(pattern = SUBSCRIPTION_ID_PATTERN, ctx=DEFAULT_NGSILD_CONTEXT):
        resp = make_response("A subscription already exists for this Context Broker", 409)
    else:
        # Suscription entities
        # Dataset
        # Distribution
        subscr = (
            SubscriptionBuilder(
                uri = plugins.toolkit.config.get(
                NOTIFICATIONS_ENDPOINT_CONFIG_OPTION, None
            ),
                receiver_headers={
                    "Authorization": token,
                    "X-CKAN-Organization": organization,
                    "X-NGSILD-Broker-Host": hostname,
                    "X-NGSILD-Broker-Port": port,
                    # TODO: non-expiring auth token so it can go appended to the subscription/notifications
                    # "X-NGSILD-Broker-Auth-Token": auth_token     
                },
            )
            .id(
                SUBSCRIPTION_ID_PATTERN + to_ckan_valid_name(organization) + ":" + to_ckan_valid_name(friendly_name)
            )
            .name("CKAN subscription for " + friendly_name + " and organization " + organization)
            .description("Notify me on new datasets")
            # TODO: add idPattern for select_entities?
            # .select_entities("Catalogue")
            .select_entities(
                str(SDMDCAT["Dataset"])
            )
            # .select_entities("Distribution")
            # .context(DEFAULT_NGSILD_CONTEXT)
            .build()
        )

        log.debug(subscr.to_dict())

        
        # TODO: Check if subscription already exists and check for exception/status code
        id = broker.subscriptions.create(subscr)

        resp = make_response("", 204)
        resp.headers["Location"] = (
            "http://"
            + hostname
            + ":"
            + str(port)
            + "/ngsi-ld/subscriptions/"
            + subscr.id
        )

    return resp


@logic.auth_disallow_anonymous_access
def ngsild_unsubscribe_action():
    """Handle request to NSGI-LD unsubscription server route"""

    # Check current user is authorized to perform this action
    log.debug("Current user: %s", current_user)

    context = {
        "model": logic.model,
        # "ignore_auth": True,
        # "limits": {'packages': 2},
        # "for_view": True,
        # "__auth_audit" = [],
        "session": logic.model.Session,
        "user": current_user.name,  # Want to set who is doing this
        "auth_user_obj": current_user,
    }

    # Retrieve remote server information from request
    # - IP address
    # - Port
    # - Scorpio friendly name: used to create the subscription id and name
    # - Organization where the subscription is stored
    # - Authorization token
    if request.content_type == "application/json":
        body = request.get_json(force=True)
    elif request.content_type == "application/x-www-form-urlencoded":
        body = request.form
    else:
        abort(
            400,
            "Unexpected Content-Type, expecting 'application/json' or 'application/x-www-form-urlencoded'",
        )

    # log.debug("Body: %s", body)
    # TODO: request complete url (https://hostname:port) so we can extract the hostname, port and secure (parameters for Client object, line 527)
    hostname: str = body.get("hostname", None)
    port: int = body.get("port", None)
    friendly_name: str = body.get("friendlyName", None)
    organization: str = body.get("organization", None)
    token: str = body.get("ckan_token", None)
    # TODO: non-expiring auth token so it can go appended to the subscription/notifications
    # auth_token: str = body.get("auth_token", None)
    # if not auth_token:
    #     auth_token = None

    if not hostname or not port or not friendly_name or not token:
        abort(
            400,
            "Missing parameters. Expected: hostname, port, friendly_name, token",
        )

    broker = Client(hostname = hostname, port = port, secure = True) #, custom_auth = auth_token)
    status_code = broker.subscriptions.delete(
        SUBSCRIPTION_ID_PATTERN + to_ckan_valid_name(organization) + ":" + to_ckan_valid_name(friendly_name)
    )
    
    resp = make_response("Successfully unsubscribed", 204)
    return resp


class HarvestNgsildPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IBlueprint)

    # IConfigurer

    def update_config(self, config_):
        toolkit.add_template_directory(config_, "templates")
        toolkit.add_public_directory(config_, "public")
        toolkit.add_resource("fanstatic", "harvest_ngsild")

    # IBlueprint

    # Use IBlueprint instead of the former IController
    # https://ckan.org/blog/migrating-ckan-28-to-ckan-29
    # https://github.com/ckan/ckan/wiki/Migration-from-Pylons-to-Flask
    # https://medium.com/@pooya.oladazimi/how-to-develop-a-plugin-for-ckan-part-one-45e7ca1f2270
    def get_blueprint(self):
        """Controller to be used for OAI-PMH using Blueprint."""

        blueprint = Blueprint(BLUEPRINT_NAME, self.__module__)

        # TODO: Consider creating specific notifications url for each NGSI-LD server
        blueprint.add_url_rule(
            "/ngsi-ld/notifications",
            BLUEPRINT_NGSILD_NOTIFICATION_ACTION_NAME,
            ngsild_notifications_action,
            methods=["POST"],
        )

        blueprint.add_url_rule(
            "/ngsi-ld/subscribe",
            BLUEPRINT_NGSILD_SUBSCRIBE_ACTION_NAME,
            ngsild_subscribe_action,
            methods=["POST"],
        )

        blueprint.add_url_rule(
            "/ngsi-ld/unsubscribe",
            BLUEPRINT_NGSILD_UNSUBSCRIBE_ACTION_NAME,
            ngsild_unsubscribe_action,
            methods=["POST"],
        )

        return blueprint
