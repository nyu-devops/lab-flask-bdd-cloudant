######################################################################
# Copyright 2016, 2023 John Rofrano. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
######################################################################
# cSpell:words Cloudant
"""
Pet Model that uses Cloudant

You must initialize this class before use by calling init_db().
This class looks for an environment variable called CLOUDANT_BINDING
to get it's database credentials from. If it cannot find one, it
tries to connect to Cloudant on the localhost. If that fails it looks
for a server name 'cloudant' to connect to.

To use with Docker couchdb database use:
    docker run -d --name couchdb -p 5984:5984 -e COUCHDB_USER=admin -e COUCHDB_PASSWORD=pass couchdb

Docker Note:
    CouchDB uses /opt/couchdb/data to store its data, and is exposed as a volume
    e.g., to use current folder add: -v $(pwd):/opt/couchdb/data
    You can also use Docker volumes like this: -v couchdb_data:/opt/couchdb/data
"""

import os
import json
import logging
from enum import Enum
from datetime import date
from retry import retry
from ibm_cloud_sdk_core import ApiException
from ibmcloudant.cloudant_v1 import CloudantV1, Document
from ibm_cloud_sdk_core.authenticators import IAMAuthenticator, BasicAuthenticator

# Get configuration from environment (12-factor)
ADMIN_PARTY = os.environ.get("ADMIN_PARTY", "False").lower() == "true"
CLOUDANT_AUTH_TYPE = os.environ.get("CLOUDANT_AUTH_TYPE", "COUCHDB_SESSION")
CLOUDANT_USERNAME = os.environ.get("CLOUDANT_USERNAME", "admin")
CLOUDANT_PASSWORD = os.environ.get("CLOUDANT_PASSWORD", "pass")
CLOUDANT_HOST = os.environ.get("CLOUDANT_HOST", "localhost")
CLOUDANT_PORT = int(os.environ.get("CLOUDANT_PORT", "5984"))
CLOUDANT_URL = os.environ.get("CLOUDANT_URL", "http://admin:pass@localhost:5984")
CLOUDANT_APIKEY = os.environ.get("CLOUDANT_APIKEY", None)

# global variables for retry (must be int)
RETRY_COUNT = int(os.environ.get("RETRY_COUNT", 10))
RETRY_DELAY = int(os.environ.get("RETRY_DELAY", 1))
RETRY_BACKOFF = int(os.environ.get("RETRY_BACKOFF", 2))


class DatabaseConnectionError(Exception):
    """Custom Exception when database connection fails"""


class DataValidationError(Exception):
    """Custom Exception with data validation fails"""


class Gender(Enum):
    """Enumeration of valid Pet Genders"""

    MALE = 0
    FEMALE = 1
    UNKNOWN = 3


################################################################################
# Pet class is a subclass of a Cloudant Document
################################################################################
class Pet(Document):
    """
    Class that represents a Pet

    This version uses a NoSQL database for persistence
    """

    logger = logging.getLogger(__name__)
    client: CloudantV1 = None
    database: str = None

    def __init__(
        self,
        name: str = None,
        category: str = None,
        available: bool = True,
        gender: Gender = Gender.UNKNOWN,
        birthday: date = date.today(),
    ):
        """Constructor"""
        super().__init__()
        self.id = None  # pylint: disable=invalid-name
        self.rev = None
        self.name = name
        self.category = category
        self.available = available
        self.gender = gender
        self.birthday = birthday

    def __repr__(self):
        return f"<Pet {self.name} id=[{self.id}]>"

    @retry(
        DatabaseConnectionError,
        delay=RETRY_DELAY,
        backoff=RETRY_BACKOFF,
        tries=RETRY_COUNT,
        logger=logger,
    )
    def create(self) -> str:
        """
        Creates a new Pet in the database
        """
        if self.name is None:  # name is the only required field
            raise DataValidationError("name attribute is not set")

        # Save the document in the database with "post_document" function
        try:
            result = self.client.post_document(
                db=Pet.database, document=self.serialize()
            ).get_result()
            if result["ok"]:
                Pet.logger.info("Created a Pet with ID: %s", self.id)
            self.id = result["id"]
            self.rev = result["rev"]

        except ApiException as api_error:
            msg = f"Create failed Code: {api_error.code} Error: {api_error}"
            Pet.logger.warning(msg, exc_info=1)
            raise DatabaseConnectionError(msg) from api_error

        return self.id

    @retry(
        DatabaseConnectionError,
        delay=RETRY_DELAY,
        backoff=RETRY_BACKOFF,
        tries=RETRY_COUNT,
        logger=logger,
    )
    def update(self) -> None:
        """Updates a Pet in the database"""
        try:
            # Update the document in the database
            result = self.client.post_document(
                db=Pet.database, document=self.serialize()
            ).get_result()
            if result["ok"]:
                Pet.logger.info("Updated the Pet with ID: %s", self.id)
            # Keeping track of the latest revision number of the document
            # object is necessary for further UPDATE/DELETE operations:
            self.rev = result["rev"]

        except ApiException as api_error:
            msg = f"Update failed Code: {api_error.code} Error: {api_error}"
            Pet.logger.warning(msg, exc_info=1)
            raise DatabaseConnectionError(msg) from api_error

    @retry(
        DatabaseConnectionError,
        delay=RETRY_DELAY,
        backoff=RETRY_BACKOFF,
        tries=RETRY_COUNT,
        logger=logger,
    )
    def delete(self):
        """Deletes a Pet from the database"""
        try:
            result = self.client.delete_document(
                db=Pet.database,
                doc_id=self.id,  # `doc_id` is required for DELETE
                rev=self.rev,  # `rev` is required for DELETE
            ).get_result()

            if result["ok"]:
                Pet.logger.info("Deleted the Pet with ID: %s", self.id)

        except ApiException as api_error:
            msg = f"Update failed Code: {api_error.code} Error: {api_error}"
            Pet.logger.warning(msg, exc_info=1)
            raise DatabaseConnectionError(msg) from api_error

    def serialize(self) -> dict:
        """serializes a Pet into a dictionary"""
        pet = {
            "name": self.name,
            "category": self.category,
            "available": self.available,
            "gender": self.gender.name,  # convert enum to string
            "birthday": self.birthday.isoformat(),
        }
        if self.id:
            pet["_id"] = self.id
        if self.rev:
            pet["_rev"] = self.rev
        return pet

    def deserialize(self, data: dict) -> None:
        """deserializes a Pet my marshalling the data.

        :param data: a Python dictionary representing a Pet.
        """
        Pet.logger.info("deserialize(%s)", data)
        try:
            self.name = data["name"]
            self.category = data["category"]
            if isinstance(data["available"], bool):
                self.available = data["available"]
            else:
                raise DataValidationError(
                    "Invalid type for boolean [available]: "
                    + str(type(data["available"]))
                )
            self.gender = getattr(Gender, data["gender"])  # create enum from string
            self.birthday = date.fromisoformat(data["birthday"])
        except KeyError as error:
            raise DataValidationError(
                "Invalid pet: missing " + error.args[0]
            ) from error
        except TypeError as error:
            raise DataValidationError(
                "Invalid pet: body of request contained bad or no data"
            ) from error

        # if there is no id and the data has one, assign it
        if not self.id and "_id" in data:
            self.id = data["_id"]
        if "_rev" in data:
            self.rev = data["_rev"]

        return self

    ######################################################################
    #  S T A T I C   D A T A B A S E   M E T H O D S
    ######################################################################

    # @classmethod
    # @retry(HTTPError, delay=RETRY_DELAY, backoff=RETRY_BACKOFF, tries=RETRY_COUNT, logger=logger)
    # def create_query_index(cls, field_name: str, order: str = "asc"):
    #     """Creates a new query index for searching"""
    #     cls.database.create_query_index(index_name=field_name, fields=[{field_name: order}])

    @classmethod
    @retry(
        ApiException,
        delay=RETRY_DELAY,
        backoff=RETRY_BACKOFF,
        tries=RETRY_COUNT,
        logger=logger,
    )
    def remove_all(cls):
        """Removes all documents from the database (use for testing)"""
        for document in cls.all():
            document.delete()

    @classmethod
    @retry(
        ApiException,
        delay=RETRY_DELAY,
        backoff=RETRY_BACKOFF,
        tries=RETRY_COUNT,
        logger=logger,
    )
    def all(cls):
        """Query that returns all Pets"""
        results = []
        try:
            query = cls.client.post_all_docs(db=cls.database, include_docs=True)
            for row in query.result["rows"]:
                pet = Pet().deserialize(row["doc"])
                results.append(pet)

        except ApiException as api_error:
            msg = f"Query All failed Code: {api_error.code} Error: {api_error}"
            Pet.logger.warning(msg, exc_info=1)
            raise DatabaseConnectionError(msg) from api_error

        return results

    ######################################################################
    #  F I N D E R   M E T H O D S
    ######################################################################

    @classmethod
    @retry(
        ApiException,
        delay=RETRY_DELAY,
        backoff=RETRY_BACKOFF,
        tries=RETRY_COUNT,
        logger=logger,
    )
    def find_by(cls, **kwargs):
        """Find records using selector"""
        results = []
        try:
            query = cls.client.post_find(db=cls.database, selector=kwargs).get_result()
            for doc in query["docs"]:
                pet = Pet().deserialize(doc)
                pet.id = doc["_id"]
                results.append(pet)

        except ApiException as api_error:
            msg = f"Find By failed Code: {api_error.code} Error: {api_error}"
            Pet.logger.warning(msg, exc_info=1)
            raise DatabaseConnectionError(msg) from api_error

        return results

    @classmethod
    @retry(
        ApiException,
        delay=RETRY_DELAY,
        backoff=RETRY_BACKOFF,
        tries=RETRY_COUNT,
        logger=logger,
    )
    def find(cls, pet_id: str):
        """Query that finds Pets by their id"""
        Pet.logger.info('Finding document with id: "%s" ...', pet_id)
        pet = Pet()
        try:
            document = cls.client.get_document(
                db=cls.database, doc_id=pet_id
            ).get_result()
            pet.deserialize(document)
            pet.id = document["_id"]

        except ApiException as ae_exp:
            if ae_exp.code == 404:
                print(f'Cannot find document with id "{pet_id}"')
            return None
        return pet
        # try:
        #     document = cls.database[pet_id]
        #     # Cloudant doesn't delete documents. :( It leaves the _id with no data
        #     # so we must validate that _id that came back has a valid _rev
        #     # if this next line throws a KeyError the document was deleted
        #     _ = document['_rev']
        #     return Pet().deserialize(document)
        # except KeyError:
        #     return None

    @classmethod
    @retry(
        ApiException,
        delay=RETRY_DELAY,
        backoff=RETRY_BACKOFF,
        tries=RETRY_COUNT,
        logger=logger,
    )
    def find_by_name(cls, name: str):
        """Query that finds Pets by their name"""
        return cls.find_by(name=name)

    @classmethod
    @retry(
        ApiException,
        delay=RETRY_DELAY,
        backoff=RETRY_BACKOFF,
        tries=RETRY_COUNT,
        logger=logger,
    )
    def find_by_category(cls, category: str):
        """Query that finds Pets by their category"""
        return cls.find_by(category=category)

    @classmethod
    @retry(
        ApiException,
        delay=RETRY_DELAY,
        backoff=RETRY_BACKOFF,
        tries=RETRY_COUNT,
        logger=logger,
    )
    def find_by_availability(cls, available: bool = True):
        """Query that finds Pets by their availability"""
        return cls.find_by(available=available)

    @classmethod
    @retry(
        ApiException,
        delay=RETRY_DELAY,
        backoff=RETRY_BACKOFF,
        tries=RETRY_COUNT,
        logger=logger,
    )
    def find_by_gender(cls, gender: str = Gender.UNKNOWN.name):
        """Query that finds Pets by their gender as a string"""
        return cls.find_by(gender=gender)

    ############################################################
    #  C L O U D A N T   D A T A B A S E   C O N N E C T I O N
    ############################################################
    @classmethod
    def database_exists(cls, db_name: str) -> bool:
        """Tests whether a database exists by requesting its headers"""
        exists: bool = False
        try:
            response = cls.client.head_database(db=db_name)
            code = response.get_status_code()
            if code == 200:
                exists = True
        except ApiException:
            exists = False
        return exists

    @classmethod
    def create_database(cls, db_name: str):
        """Initialized the Cloudant database"""
        # Try to create database if it doesn't exist
        logging.info("Connecting to %s...", db_name)
        try:
            if not cls.database_exists(db_name):
                result = cls.client.put_database(db=db_name).get_result()
                if result["ok"]:
                    logging.info('Database "%s" was created.', db_name)
        except ApiException as ae_exp:
            logging.error('Cannot create "%s" database: %s', db_name, ae_exp)

    @staticmethod
    def init_db(dbname: str = "pets"):
        """
        Initialized Cloudant database connection

        IBM Cloudant credentials example:
        {
            "apikey": "{api_key}",
            "host": "{host_name}",
            "password": "{password}",
            "port": 5984,
            "url": "{url_to_cloudant_host}",
            "username": "{username}"
        }
        """
        credentials = {}

        # Check if there is a Kubernetes binding
        if "BINDING_CLOUDANT" in os.environ:
            Pet.logger.info("Found Kubernetes BINDING_CLOUDANT bindings")
            credentials = json.loads(os.environ["BINDING_CLOUDANT"])

        # If Credentials not found in BINDING_CLOUDANT
        # get it from the CLOUDANT_xxx environment variables
        if not credentials:
            Pet.logger.info("BINDING_CLOUDANT undefined using environment variables.")
            credentials = {
                "apikey": CLOUDANT_APIKEY,
                "username": CLOUDANT_USERNAME,
                "password": CLOUDANT_PASSWORD,
                "host": CLOUDANT_HOST,
                "port": CLOUDANT_PORT,
                "url": CLOUDANT_URL,
            }

        if any(
            k not in credentials
            for k in ("host", "username", "password", "port", "url")
        ):
            raise DatabaseConnectionError(
                "Error - Failed to retrieve options. "
                "Check that app is bound to a Cloudant service."
            )

        Pet.logger.info("Cloudant Endpoint: %s", credentials["url"])
        try:
            if CLOUDANT_AUTH_TYPE == "IAM":
                authenticator = IAMAuthenticator(CLOUDANT_APIKEY)
            else:
                authenticator = BasicAuthenticator(CLOUDANT_USERNAME, CLOUDANT_PASSWORD)
            Pet.client = CloudantV1(authenticator=authenticator)
            Pet.client.set_service_url(CLOUDANT_URL)

        except ApiException:
            raise DatabaseConnectionError("Cloudant service could not be reached")

        Pet.database = dbname
        # Create database if it doesn't exist
        Pet.create_database(dbname)
        # check for success
        if not Pet.database_exists(dbname):
            raise DatabaseConnectionError(f"Database [{dbname}] could not be obtained")
