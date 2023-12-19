# ckanext-harvest-ngsild
This extension provides plugins that allow the connection between the NGSI-LD world and the CKAN world. Its main objective is to transform the NGSI-LD data obtained through an accessible endpoint into CKAN format and, afterwords, import it into this data management system.

## Endpoints
The aim of this extension is to transform NSGI-LD entities into CKAN format. Particularly, entities of types Catalogue, Dataset and Distribution, all of them belonging to the DCAT-AP subject from the Smart Data Models initiative. These entities are mapped into the CKAN world in Organization, Dataset and Resources, respectively.

The expected data cycle starts at the subscription of this extension to a Context Broker with NGSI-LD support, in order to receive notifications every time a new Dataset entity is created or updated. Then, the extension will transform this information to CKAN format and import it to this management instance. 

To this end, this extension enables three new endpoints to CKAN_HOST. 
- `/nsgi-ld/subscribe`: a request to this endpoint will trigger the creation of the subscription into the Context Broker. There is a mandatory POST body:
    ```json
    {
        "hostname": <hostname or IP of the Context Broker>,
        "port": <port of the Context Broker>,
        "friendlyName": <CKAN username>,
        "ckan_token": <CKAN API Token>,
        "organization": <organization name>
    }
    ```
    By means of these parameters, the import of data can be achieved (thanks to `ckan_token`) and can be tracked (thanks to the `friendlyName`). 
- `/nsgi-ld/unsubscribe`: analogous to the previous endpoint, the POST body is also required and a request to this endpoint is responsible for unsuscribing from the indicated Context Broker, stopping the reception of notifications.
- `/nsgi-ld/notifications`: this last endpoint corresponds to the URL resource that receives the notifications from the Context Broker. This parameters is set in the subscription as the callback. As already mentioned, when a notification arrives, it triggers the transformation to CKAN format and the creation of datasets/resources. 


## Requirements
- This extension has been developed using CKAN 2.10.1 version.
- On the other hand, as this extensions bridges the NGSI-LD and CKAN world, it needs a NGSI-LD Context Broker deployment. 


## Installation - Docker-compose
### Production environment
To install `ckanext-harvest-ngsild`:
1. Add the extension to the Dockerfile and add these lines at the end (folder path: `ckan-docker/ckan/`):
    ```bash
    RUN pip3 install -e git+https://github.com/tlmat-unican/ckanext-harvest-ngsild.git@main#egg=ckanext-harvest-ngsild && \
    pip3 install -r ${APP_DIR}/src/ckanext-harvest-ngsild/requirements.txt
    ```

2. Add parameters to `.env` file (folder path: `ckan-docker/`):
    ```bash
    CKAN__PLUGINS = "ennvars <plugins> harvest_ngsild"
    CKANEXT__HARVEST_NGSILD__NOTIFICATIONS_ENDPOINT = "<ckan_host>/ngsi-ld/notifications"
    ```
    **Notes**: 
    - `<plugins>` is a placeholder for the rest of your plugins.
    - change `<ckan_host>` to your HOST_NAME variable or CKAN_SITE_URL value.

3. Run your docker-compose file (folder path: `ckan-docker/`):
    ```bash
    docker-compose -f <docker-compose file> build --no-cache 
    docker-compose -f <docker-compose file> up
    ```
    With the `--no-cache` parameter, you are specifying to do not use cache when building the image. This parameter is optional.

### Development environment
To install `ckanext-harvest-ngsild`:
1. Clone the GitHub repository (folder path: `ckan-docker/src/`):
    ```bash
    git clone https://github.com/tlmat-unican/ckanext-harvest-ngsild.git
    ```
    **Note**: if `src/` folder do not exist, create it.

2. Add parameters to `.env` file (folder path: `ckan-docker/`):
    ```bash
    CKAN__PLUGINS = "envvars <plugins> harvest_ngsild"
    CKANEXT__HARVEST_NGSILD__NOTIFICATIONS_ENDPOINT = "<ckan_host>/ngsi-ld/notifications"
    ```
    **Notes**: 
    - `<plugins>` is a placeholder for the rest of your plugins.
    - change `<ckan_host>` to your HOST_NAME variable or CKAN_SITE_URL value.

3. Run your docker-compose file (folder path: `ckan-docker/`):
    ```bash
    docker-compose -f <docker-compose-dev file> up --build
    ```


## Authors
The ckanext-harvest-ngsild extension has been written by:
- [Laura Martín](https://github.com/lauramartingonzalezzz)
- [Jorge Lanza](https://github.com/jlanza)
- [Víctor González](https://github.com/vgonzalez7)
- [Juan Ramón Santana](https://github.com/juanrasantana)
- [Pablo Sotres](https://github.com/psotres)
- [Luis Sánchez](https://github.com/sanchezgl)


## Acknowledgement
This work was supported by the European Commission CEF Programme by means of the project SALTED "Situation-Aware Linked heTerogeneous Enriched Data" under the Action Number 2020-EU-IA-0274.


## License
This material is licensed under the GNU Lesser General Public License v3.0 whose full text may be found at the *LICENSE* file.

It mainly makes use of the following libraries and frameworks (dependencies of dependencies have been omitted):

| Library / Framework |   License    |
|---------------------|--------------|
| Flask          | BSD          |
| ngsildclient             | Apache 2.0          |
| rdflib                 | BSD-3-Clause          |
| setuptools          |  MIT          |

## hacer fork