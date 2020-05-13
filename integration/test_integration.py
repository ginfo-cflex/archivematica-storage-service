# -*- coding: utf-8 -*-
"""Integration testing.

This module tests Archivematica Storage Service in isolation, it does not
require Archivematica pipelines deployed.

Currently, the tests in this module are executed via Docker Compose. The long
term goal is to have pytest orchestrate the Compose services instead.

What we're testing:
* AIPs created with older versions of Archivematica
* AIPs that are compressed and/or uncompressed
* Replication and fixity

First goal:
* Reproduce https://github.com/archivematica/Issues/issues/1149

"""
import json
import shutil
import os
import scandir

import pytest

from metsrw.plugins import premisrw
from locations.models import Location

try:
    from pathlib import Path
except ImportError:
    from pathlib2 import Path


FIXTURES_DIR = Path(__file__).parent / "fixtures"


class Client(object):
    """Slim API client."""

    def __init__(self, admin_client):
        self.admin_client = admin_client

    def add_space(self, data):
        return self.admin_client.post("/api/v2/space/", json.dumps(data), content_type="application/json")

    def add_pipeline(self, data):
        return self.admin_client.post("/api/v2/pipeline/", json.dumps(data), content_type="application/json")

    def get_pipelines(self, data):
        return self.admin_client.get("/api/v2/pipeline/", data)

    def add_location(self, data):
        return self.admin_client.post("/api/v2/location/", json.dumps(data), content_type="application/json")

    def set_location(self, location_id, data):
        return self.admin_client.post("/api/v2/location/{}/".format(location_id), json.dumps(data), content_type="application/json")

    def get_locations(self, data):
        return self.admin_client.get("/api/v2/location/", data)

    def add_file(self, file_id, data):
        return self.admin_client.put("/api/v2/file/{}/".format(file_id), json.dumps(data), content_type="application/json")

    def get_pointer_file(self, file_id):
        return self.admin_client.get("/api/v2/file/{}/pointer_file/".format(file_id))


@pytest.fixture()
def client(admin_client, scope="session"):
    return Client(admin_client)


@pytest.fixture()
def startup(scope="function"):
    """Create default space and its locations.

    Storage Service provisions a default space and a number of locations when
    the application starts. Its purpose is questionable but this module is just
    trying to reproduce it.

        * space (staging_path=/var/archivematica/storage_service, path=/)
        * location (purpose=TRANSFER_SOURCE, path=home)
        * location (purpose=AIP_STORAGE, path=/var/archivematica/sharedDirectory/www/AIPsStore)
        * location (purpose=DIP_STORAGE, path=/var/archivematica/sharedDirectory/www/DIPsStore)
        * location (purpose=BACKLOG, path=/var/archivematica/sharedDirectory/www/AIPsStore/transferBacklog)
        * location (purpose=STORAGE_SERVICE_INTERNAL, path=/var/archivematica/storage_service)
        * location (purpose=AIP_RECOVERY, path=/var/archivematica/storage_service/recover)

    From the list above, CURRENTLY_PROCESSING is missing but that's later added
    when a pipeline is registered.
    """
    from storage_service.urls import startup
    startup()  # TODO: get rid of this!


@pytest.fixture(scope="function")
def pipeline(client, tmpdir):
    """Register a pipeline."""
    resp = client.add_pipeline({
        "uuid": "00000b87-1655-4b7e-bbf8-344b317da334",
        "description": "Beefy pipeline",
        "create_default_locations": True,
        "shared_path": "/var/archivematica/sharedDirectory",
        "remote_name": "http://127.0.0.1:65534",
        "api_username": "test",
        "api_key": "test",
    })
    assert resp.status_code == 201
    return json.loads(resp.content)


@pytest.fixture(scope="function")
def pipeline_with_s3_replication(pipeline, client):
    """Register a pipeline with replication in S3."""
    resp = client.add_space({
        "access_protocol": "S3",
        "path": "",
        "staging_path": "/var/archivematica/sharedDirectory/tmp/s3_staging_path",
        "endpoint_url": "http://minio:9000",
        "access_key_id": "minio",
        "secret_access_key": "minio123",
        "region": "planet-earth",
        "bucket": "AIPStorage"
    })
    assert resp.status_code == 201
    space = json.loads(resp.content)

    resp = client.add_location({
        "relative_path": "aips",
        "staging_path": "",
        "purpose": "RP",
        "space": space["resource_uri"],
        "pipeline": [pipeline["resource_uri"]],
    })
    assert resp.status_code == 201
    rp_location = json.loads(resp.content)

    """ TODO: POST or PATCH not available for updates.
    resp = client.get_locations({"pipeline_uuid": pipeline["uuid"], "purpose": "AS"})
    as_location = json.loads(resp.content)["objects"][0]
    as_location["replicators"] = [rp_location["resource_uri"]]
    resp = client.set_location(as_location["uuid"], as_location)
    assert resp.status_code == 204
    """

    # Set up replicator (not using API since POST/PATCH not working).
    resp = client.get_locations({"pipeline_uuid": pipeline["uuid"], "purpose": "AS"})
    as_location = json.loads(resp.content)["objects"][0]
    rp_location = Location.objects.get(uuid=rp_location["uuid"])
    as_location = Location.objects.get(uuid=as_location["uuid"])
    as_location.replicators.add(rp_location)

    # Confirm that the replicator has been associated.
    assert Location.objects.get(uuid=as_location.uuid).replicators.all().count() == 1

    return pipeline


@pytest.fixture(scope="function")
def compression_event():
    return tuple([
        "event",
        premisrw.PREMIS_META,
        (
            "event_identifier",
            ("event_identifier_type", "UUID"),
            ("event_identifier_value", "4711f4eb-8903-4e58-85da-4827e6530d0b"),
        ),
        ("event_type", "compression"),
        ("event_date_time", "2017-08-15T00:30:55"),
        ("event_detail", (
            "program=7z; "
            "version=p7zip Version 9.20 "
            "(locale=en_US.UTF-8,Utf16=on,HugeFiles=on,2 CPUs); "
            "algorithm=bzip2"
        )),
        (
            "event_outcome_information",
            (
                "event_outcome_detail",
                ("event_outcome_detail_note", 'Standard Output="..."; Standard Error=""'),
            ),
        ),
        (
            "linking_agent_identifier",
            ("linking_agent_identifier_type", "foobar"),
            ("linking_agent_identifier_value", "foobar"),
        )
    ])


@pytest.fixture(scope="function")
def agent():
    return tuple([
        "agent",
        premisrw.PREMIS_3_0_META,
        (
            "agent_identifier",
            ("agent_identifier_type", "foobar"),
            ("agent_identifier_value", "foobar"),
        ),
        ("agent_name", "foobar"),
        ("agent_type", "foobar")
    ])


def get_size(path):
    if isinstance(path, Path):
        path = str(path)
    if os.path.isfile(path):
        return os.path.getsize(path)
    size = 0
    for dirpath, _, filenames in scandir.walk(path):
        for filename in filenames:
            file_path = os.path.join(dirpath, filename)
            size += os.path.getsize(file_path)
    return size


@pytest.mark.django_db(transaction=True)
def test_store_compressed_aip(startup, pipeline, client, compression_event, agent):
    resp = client.get_pipelines({})
    pipeline = json.loads(resp.content)["objects"][0]

    resp = client.get_locations({"pipeline_uuid": pipeline["uuid"], "purpose": "CP"})
    cp_location = json.loads(resp.content)["objects"][0]
    cp_location_path = Path(cp_location["path"])
    assert str(cp_location_path) == "/var/archivematica/sharedDirectory"

    resp = client.get_locations({"pipeline_uuid": pipeline["uuid"], "purpose": "AS"})
    as_location = json.loads(resp.content)["objects"][0]
    as_location_path = Path(as_location["path"])
    assert str(as_location_path) == "/var/archivematica/sharedDirectory/www/AIPsStore"

    filename = "20200513054116-5658e603-277b-4292-9b58-20bf261c8f88.7z"
    shutil.copy(str(FIXTURES_DIR / filename), str(cp_location_path))
    path = cp_location_path / filename
    assert path.stat().st_size == 12191

    # Submit AIP.
    resp = client.add_file(
        "5658e603-277b-4292-9b58-20bf261c8f88",
        {
            "uuid": "5658e603-277b-4292-9b58-20bf261c8f88",
            "origin_location": cp_location["resource_uri"],
            "origin_path": path.name,
            "current_location": as_location["resource_uri"],
            "current_path": path.name,
            "package_type": "AIP",
            "aip_subtype": "Archival Information Package",
            "size": path.stat().st_size,
            "origin_pipeline": pipeline["resource_uri"],
            "events": [compression_event],
            "agents": [agent],
        },
    )
    assert resp.status_code == 201

    aip = json.loads(resp.content)
    aip_path = as_location_path / "5658/e603/277b/4292/9b58/20bf/261c/8f88/20200513054116-5658e603-277b-4292-9b58-20bf261c8f88.7z"
    assert aip["uuid"] == "5658e603-277b-4292-9b58-20bf261c8f88"
    assert aip["current_full_path"] == str(aip_path)
    assert aip_path.stat().st_size == 12191

    resp = client.get_pointer_file(aip["uuid"])
    assert resp.status_code == 200


@pytest.mark.django_db(transaction=True)
def test_store_uncompressed_aip(startup, pipeline, client, compression_event, agent):
    resp = client.get_pipelines({})
    content = json.loads(resp.content)
    assert len(content["objects"]) == 1

    resp = client.get_locations({"pipeline_uuid": pipeline["uuid"], "purpose": "CP"})
    cp_location = json.loads(resp.content)["objects"][0]
    cp_location_path = Path(cp_location["path"])

    resp = client.get_locations({"pipeline_uuid": pipeline["uuid"], "purpose": "AS"})
    as_location = json.loads(resp.content)["objects"][0]
    as_location_path = Path(as_location["path"])

    dirname = "20200513060703-828c44bb-e631-4137-8638-bda4434218dc"
    path = cp_location_path / dirname
    shutil.copytree(str(FIXTURES_DIR / dirname), str(path))
    assert path.is_dir()

    # Submit AIP.
    resp = client.add_file(
        "828c44bb-e631-4137-8638-bda4434218dc",
        {
            "uuid": "828c44bb-e631-4137-8638-bda4434218dc",
            "origin_location": cp_location["resource_uri"],
            "origin_path": path.name + os.sep,
            "current_location": as_location["resource_uri"],
            "current_path": path.name + os.sep,
            "package_type": "AIP",
            "aip_subtype": "Archival Information Package",
            "size": get_size(path),
            "origin_pipeline": pipeline["resource_uri"],
            "related_package_uuid": None,
            "events": [compression_event],
            "agents": [agent],
        },
    )
    assert resp.status_code == 201

    aip = json.loads(resp.content)
    aip_path = as_location_path / "828c/44bb/e631/4137/8638/bda4/4342/18dc" / dirname
    assert aip["uuid"] == "828c44bb-e631-4137-8638-bda4434218dc"
    assert aip["current_full_path"] == str(aip_path)

    resp = client.get_pointer_file(aip["uuid"])
    assert resp.status_code == 404


@pytest.mark.django_db(transaction=True)
def test_store_compressed_aip_with_s3_replication(startup, pipeline_with_s3_replication, client, compression_event, agent):
    pipeline = pipeline_with_s3_replication

    resp = client.get_locations({"pipeline_uuid": pipeline["uuid"], "purpose": "CP"})
    cp_location = json.loads(resp.content)["objects"][0]
    cp_location_path = Path(cp_location["path"])

    resp = client.get_locations({"pipeline_uuid": pipeline["uuid"], "purpose": "AS"})
    as_location = json.loads(resp.content)["objects"][0]
    as_location_path = Path(as_location["path"])

    filename = "20200513054116-5658e603-277b-4292-9b58-20bf261c8f88.7z"
    shutil.copy(str(FIXTURES_DIR / filename), str(cp_location_path))
    path = cp_location_path / filename
    assert path.stat().st_size == 12191

    # Submit AIP.
    resp = client.add_file(
        "5658e603-277b-4292-9b58-20bf261c8f88",
        {
            "uuid": "5658e603-277b-4292-9b58-20bf261c8f88",
            "origin_location": cp_location["resource_uri"],
            "origin_path": path.name,
            "current_location": as_location["resource_uri"],
            "current_path": path.name,
            "package_type": "AIP",
            "aip_subtype": "Archival Information Package",
            "size": path.stat().st_size,
            "origin_pipeline": pipeline["resource_uri"],
            "events": [compression_event],
            "agents": [agent],
        },
    )
    assert resp.status_code == 201

    aip = json.loads(resp.content)
    aip_path = as_location_path / "5658/e603/277b/4292/9b58/20bf/261c/8f88/20200513054116-5658e603-277b-4292-9b58-20bf261c8f88.7z"
    assert aip["uuid"] == "5658e603-277b-4292-9b58-20bf261c8f88"
    assert aip["current_full_path"] == str(aip_path)
    assert aip_path.stat().st_size == 12191

    resp = client.get_pointer_file(aip["uuid"])
    assert resp.status_code == 200
