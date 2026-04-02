import os
import json
import base64
import unittest
from unittest.mock import MagicMock, patch

# Ensure test environment variables are set before importing/instantiating Config
os.environ["GCP_PROJECT_ID"] = "test-project"
os.environ["GCP_LOCATION"] = "global"
os.environ["GCS_BUCKET"] = "test-bucket"
os.environ["DATA_STORE_ID"] = "test-ds"
os.environ["SECRET_ACL_MAPPING"] = "ge-acl-mapping"

import connector

class TestConnectorSecurity(unittest.TestCase):

    @patch("connector.secretmanager.SecretManagerServiceClient")
    def test_fetch_secret(self, mock_sm_client):
        """Test that the connector successfully fetches and decodes secrets."""
        # Setup mock response
        mock_client_instance = MagicMock()
        mock_response = MagicMock()
        mock_acl_payload = {
            "readers": [{"group": "engineering@example.com"}],
            "viewers": [{"user": "admin@example.com"}]
        }
        mock_response.payload.data = json.dumps(mock_acl_payload).encode("UTF-8")
        mock_client_instance.access_secret_version.return_value = mock_response
        mock_sm_client.return_value = mock_client_instance

        # Execute
        result = connector.fetch_secret("test-project", "ge-acl-mapping")
        
        # Verify
        self.assertIsNotNone(result)
        parsed_result = json.loads(result)
        self.assertEqual(parsed_result["readers"][0]["group"], "engineering@example.com")
        mock_client_instance.access_secret_version.assert_called_once_with(
            request={"name": "projects/test-project/secrets/ge-acl-mapping/versions/latest"}
        )

    def test_build_document_with_acl(self):
        """Test that ACL mappings are correctly injected into the Discovery Engine document."""
        # Mock Data
        mock_record = {
            "id": 101,
            "title": "Confidential Security Architecture",
            "body": "This document contains sensitive details.",
            "userId": 1,
            "updated_at": "2026-04-02T10:00:00Z"
        }
        mock_users = {
            1: {"username": "sec-admin", "company": {"name": "Onix"}}
        }
        mock_acl_mapping = {
            "readers": [{"group": "security-team@example.com"}]
        }

        # Execute
        doc = connector.build_discovery_engine_doc(mock_record, mock_users, acl_mapping=mock_acl_mapping)

        # Verify Document Structure matches Google Specs
        self.assertEqual(doc["id"], "doc-101")
        self.assertEqual(doc["structData"]["author"], "sec-admin")
        self.assertEqual(doc["structData"]["company"], "Onix")
        
        # Verify Content Base64 Encoding
        decoded_content = base64.b64decode(doc["content"]["rawBytes"]).decode("utf-8")
        self.assertIn("Confidential Security Architecture", decoded_content)
        
        # Verify ACL Injection
        self.assertIn("aclInfo", doc)
        self.assertEqual(doc["aclInfo"]["readers"][0]["principals"][0]["groupId"], "security-team@example.com")


if __name__ == "__main__":
    unittest.main(verbosity=2)
